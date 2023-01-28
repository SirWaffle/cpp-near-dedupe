#pragma once

#include "HashTable.h"
#include "LSHBandHashMap.h"
#include "ChunkList.h"

template<typename UINT_HASH_TYPE>
struct CompareItem;

template<typename UINT_HASH_TYPE, int MAX_HASH_LEN, int BLOCK_SIZE>
class ICompareStrat
{
public:
    virtual uint64_t GetNumEntries() = 0;
    virtual uint64_t GetEstimatedLSHMemoryUsageBytes() = 0;
    virtual size_t GetUniqueItemsCount() = 0;
    virtual uint64_t GetMemUsageBytes() = 0;

    virtual void Init() = 0;
    virtual void CheckAgainstHashes(CompareItem< UINT_HASH_TYPE>* citem) = 0;
    virtual bool HasPotentialMatches() = 0;
    virtual bool CheckFormatches(CompareItem< UINT_HASH_TYPE>* citem, double earlyOut, double dupeThreash) = 0;
    virtual void AddToLSH(CompareItem< UINT_HASH_TYPE>* citem) = 0;
};


template<typename UINT_HASH_TYPE, int MAX_HASH_LEN, typename UINT_BAND_HASH_TYPE, int BLOCK_SIZE>
class CompareStratBase : public ICompareStrat<UINT_HASH_TYPE, MAX_HASH_LEN, BLOCK_SIZE>
{
public:
    typedef LSHBandHashMap<UINT_HASH_TYPE, UINT_BAND_HASH_TYPE, MAX_HASH_LEN> LSHHashMap;

    HashBlockAllocator<UINT_HASH_TYPE, MAX_HASH_LEN, BLOCK_SIZE> hashblocks;

    const int singleThreadSize = 4096;

    LSHHashMap bandHashMap;
    BS::thread_pool* threadPool; 

    std::vector<UINT_BAND_HASH_TYPE> bandHashes;


    CompareStratBase(uint32_t bands, uint64_t buckets, LSHHashMap::LSH_TYPE_ENUM lshType, BS::thread_pool* _threadPool, uint64_t maxDocuments)
        : hashblocks(HashBlockAllocator<UINT_HASH_TYPE, MAX_HASH_LEN, BLOCK_SIZE>((maxDocuments / BLOCK_SIZE) + BLOCK_SIZE)),
        bandHashMap(LSHHashMap(bands, buckets, lshType)),
        threadPool(_threadPool)
    {

    }

    uint64_t GetMemUsageBytes() final
    {
        return hashblocks.MemoryUsage();
    }

    size_t GetUniqueItemsCount() final
    {
        return hashblocks.NumEntries();
    }

    uint64_t GetNumEntries() final
    {
        return bandHashMap.GetNumEntries();
    }

    uint64_t GetEstimatedLSHMemoryUsageBytes() final
    {
        return bandHashMap.GetEstimatedMemoryUsageBytes();
    }

    //add to lsh
    void AddToLSH(CompareItem< UINT_HASH_TYPE>* citem) override
    {
        auto entry = hashblocks.AddItem(citem->myHashData->hashes.get(), citem->myHashData->hashLen);
        bandHashMap.AddToMap(bandHashes.begin(), entry);
    }
};




//split this out to try different methods
template<typename UINT_HASH_TYPE, int MAX_HASH_LEN, typename UINT_BAND_HASH_TYPE, int BLOCK_SIZE>
class SetReduceCompareStrat: public CompareStratBase<UINT_HASH_TYPE, MAX_HASH_LEN, UINT_BAND_HASH_TYPE, BLOCK_SIZE>
{
    std::list< HashBlockEntry<UINT_HASH_TYPE, MAX_HASH_LEN>* > potentialmatchCandidates;
    size_t totalPotentialCandidates;

public:
    SetReduceCompareStrat(BS::thread_pool* _threadPool, uint32_t bands, uint64_t buckets, CompareStratBase<UINT_HASH_TYPE, MAX_HASH_LEN, UINT_BAND_HASH_TYPE, BLOCK_SIZE>::LSHHashMap::LSH_TYPE_ENUM lshType, uint64_t maxDocuments)
        :CompareStratBase<UINT_HASH_TYPE, MAX_HASH_LEN, UINT_BAND_HASH_TYPE, BLOCK_SIZE>(bands, buckets, lshType, _threadPool, maxDocuments)
    {

    }


    void Init() override
    {
        this->bandHashes.resize(this->bandHashMap.GetBands());
    }

    //get a list of potential candidates
    void CheckAgainstHashes(CompareItem< UINT_HASH_TYPE>* citem) override
    {
        this->potentialmatchCandidates.clear();
        this->bandHashMap.Hash(citem->myHashData->hashes.get(), citem->myHashData->hashLen, this->bandHashes.begin());
        totalPotentialCandidates = this->bandHashMap.GetCollidedSet(this->bandHashes.begin(), this->potentialmatchCandidates);
    }

    //potential candidate count
    bool HasPotentialMatches() override
    {
        return this->potentialmatchCandidates.size() > this->bandHashMap.GetBands() / 4;
    }

    //iterator for worker thread start
    bool CheckFormatches(CompareItem< UINT_HASH_TYPE>* citem, double earlyOut, double dupeThreash) override
    {
        //do iteration, wait on futures, etc.
        //spread the work of comparing across threads..  

        BS::multi_future<bool> internalCompareThreadFutures;

        std::stop_source workerThreadStopper;

        //check if we should jsut do the whole thing on one thread...
        bool matched = false;
        if (totalPotentialCandidates < this->singleThreadSize) //faster not to wait on threads and locks and all that
        {
            matched = CompareThreadWorkerFunc<UINT_HASH_TYPE, MAX_HASH_LEN, BLOCK_SIZE>(
                workerThreadStopper, this->potentialmatchCandidates.begin(), this->potentialmatchCandidates.end(), 1, earlyOut, dupeThreash, citem);
        }
        else
        {

            auto list = &(this->potentialmatchCandidates);

            //should chunkify these vectors as well, since some can be much longer than others
            uint32_t numSlices = (uint32_t)list->size() / this->singleThreadSize;
            if (numSlices == 0)
                numSlices = 1;
            auto sliceIt = list->begin();
            auto endIt = list->end();

            for (size_t slice = 0; slice < numSlices && sliceIt != list->end() && !workerThreadStopper.stop_requested(); slice++, sliceIt++)
            {
                internalCompareThreadFutures.push_back(
                    this->threadPool->submit([this, workerThreadStopper, sliceIt, endIt, numSlices, &earlyOut, &dupeThreash, citem]() {

                        return CompareThreadWorkerFunc<UINT_HASH_TYPE, MAX_HASH_LEN, BLOCK_SIZE>(
                            workerThreadStopper, sliceIt, endIt, numSlices, earlyOut, dupeThreash, citem);
                        }
                    )
                );
            }


            //wait for worker threads
            if (internalCompareThreadFutures.size() > 0)
                internalCompareThreadFutures.wait();

        }

        //run through each and look at result, if its a dupe, pass it to dupes, if its not, we need to compare all the
        bool isDupe = matched;
        for (size_t i = 0; i < internalCompareThreadFutures.size() && isDupe == false; ++i)
        {
            isDupe = internalCompareThreadFutures[i].get();
        }

        return isDupe;
    }

};




//split this out to try different methods
template<typename UINT_HASH_TYPE, int MAX_HASH_LEN, typename UINT_BAND_HASH_TYPE, int BLOCK_SIZE>
class AllBucketsCompareStrat : public CompareStratBase<UINT_HASH_TYPE, MAX_HASH_LEN, UINT_BAND_HASH_TYPE, BLOCK_SIZE>
{

    std::vector<  std::deque< HashBlockEntry<UINT_HASH_TYPE, MAX_HASH_LEN>* >* > potentialmatchCandidates;
    size_t totalPotentialCandidates;


public:
    AllBucketsCompareStrat(BS::thread_pool* _threadPool, uint32_t bands, uint64_t buckets, CompareStratBase<UINT_HASH_TYPE, MAX_HASH_LEN, UINT_BAND_HASH_TYPE, BLOCK_SIZE>::LSHHashMap::LSH_TYPE_ENUM lshType, uint64_t maxDocuments)
        :CompareStratBase<UINT_HASH_TYPE, MAX_HASH_LEN, UINT_BAND_HASH_TYPE, BLOCK_SIZE>(bands, buckets, lshType, _threadPool, maxDocuments)
    {

    }


    void Init() override
    {
        this->bandHashes.resize(this->bandHashMap.GetBands());
        potentialmatchCandidates.reserve(MAX_HASH_LEN);
    }

    //get a list of potential candidates
    void CheckAgainstHashes(CompareItem< UINT_HASH_TYPE>* citem) override
    {
        potentialmatchCandidates.resize(0);

        this->bandHashMap.Hash(citem->myHashData->hashes.get(), citem->myHashData->hashLen, this->bandHashes.begin());
        totalPotentialCandidates = this->bandHashMap.GetCollided(this->bandHashes.begin(), this->potentialmatchCandidates);
    }

    //potential candidate count
    bool HasPotentialMatches() override
    {
        return this->potentialmatchCandidates.size() > this->bandHashMap.GetBands() / 4;
    }

    //iterator for worker thread start
    bool CheckFormatches(CompareItem< UINT_HASH_TYPE>* citem, double earlyOut, double dupeThreash) override
    {
        //do iteration, wait on futures, etc.
        //spread the work of comparing across threads..  

        BS::multi_future<bool> internalCompareThreadFutures;

        std::stop_source workerThreadStopper;

        //check if we should jsut do the whole thing on one thread...
        bool matched = false;
        if (totalPotentialCandidates < this->singleThreadSize) //faster not to wait on threads and locks and all that
        {
            matched = CompareThreadWorkerFunc<UINT_HASH_TYPE, MAX_HASH_LEN, BLOCK_SIZE>(
                workerThreadStopper, potentialmatchCandidates.begin(), potentialmatchCandidates.end(), earlyOut, dupeThreash, citem);
        }
        else
        {
            //split the list by offseting start iterators, and incrementing them by different amounts
            for (auto pcIt = potentialmatchCandidates.begin(); pcIt != potentialmatchCandidates.end() && !workerThreadStopper.stop_requested(); ++pcIt)
            {
                auto list = *pcIt;

                //should chunkify these vectors as well, since some can be much longer than others
                uint32_t numSlices = (uint32_t)list->size() / this->singleThreadSize;
                if (numSlices == 0)
                    numSlices = 1;
                auto sliceIt = list->begin();
                auto endIt = list->end();

                for (size_t slice = 0; slice < numSlices && sliceIt != list->end() && !workerThreadStopper.stop_requested(); slice++, sliceIt++)
                {
                    internalCompareThreadFutures.push_back(
                        this->threadPool->submit([this, workerThreadStopper, sliceIt, endIt, numSlices, &earlyOut, &dupeThreash, citem]() {

                                return CompareThreadWorkerFunc<UINT_HASH_TYPE, MAX_HASH_LEN, BLOCK_SIZE>(
                                    workerThreadStopper, sliceIt, endIt, numSlices, earlyOut, dupeThreash, citem);
                            }
                        )
                    );
                }

            }

            //wait for worker threads
            if (internalCompareThreadFutures.size() > 0)
                internalCompareThreadFutures.wait();

        }

        //run through each and look at result, if its a dupe, pass it to dupes, if its not, we need to compare all the
        bool isDupe = matched;
        for (size_t i = 0; i < internalCompareThreadFutures.size() && isDupe == false; ++i)
        {
            isDupe = internalCompareThreadFutures[i].get();
        }

        return isDupe;
    }

};