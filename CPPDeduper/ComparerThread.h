#pragma once

#include "HasherThread.h"
#include "Hashing.h"
#include "LockableQueue.h"
#include "Jaccard.h"
#include <list>
#include <thread>

#include "ThreadPool.h"
#include "HashTable.h"
#include "LSHBandHashMap.h"

#include <immintrin.h>
#include <nmmintrin.h>
#include <emmintrin.h>

#include "ChunkList.h"

#define LOAD_TEST false

#define ALREADY_PROCESSED_CHECK true
#define USE_INTRIN true

//max items to compare across this thread ( to prevent slowdowns from locking for small numbers of items )
const int singleThreadSize = 256;

struct CompareThreadDupeItem
{
    CompareThreadDupeItem(uint32_t _docId, int64_t _rowNumber)
        :docId(_docId),
        rowNumber(_rowNumber)
    {}

    uint32_t docId;
    int64_t rowNumber;
};


template<typename UINT_HASH_TYPE>
struct CompareItem
{
    CompareItem(HasherThreadOutputData< UINT_HASH_TYPE>* _myHashData)
        :myHashData(_myHashData)
    {}

    ~CompareItem()
    {
        delete myHashData;
    }

    HasherThreadOutputData< UINT_HASH_TYPE>* myHashData;
};


//flat container
template<typename UINT_HASH_TYPE, uint32_t MAX_HASH_LEN, uint32_t BLOCK_SIZE>
bool WorkThreadFunc(
    std::stop_source workerThreadStopper,
    auto startIt,
    auto endIt,
    uint32_t iterIncrementAmount,
    double earlyOut, double dupeThreash, CompareItem<UINT_HASH_TYPE>* citem)
{
    //compare incoming against all others, update the its max value.
    //this will prioritize removing later documents that match, not the first one
#if USE_INTRIN
    __m128i itemflags = _mm_set_epi64x(citem->myHashData->arrowData->docId, citem->myHashData->arrowData->rowNumber);
#endif
    auto dbgIt = startIt;
    for (; startIt != endIt && !workerThreadStopper.stop_requested();)
    {
        auto hashes = *startIt;

#if USE_INTRIN 
        //used to not check the same document for the same item across threads, since docs can be in multiple buckets
        __m128i flags = _mm_loadu_si128((__m128i*) (&hashes->flags[0]));
        __m128i cmp = _mm_cmpeq_epi64(itemflags, flags);

        if (_mm_extract_epi64(cmp, 0) != 0 && _mm_extract_epi64(cmp, 1) != 0)
        {
            //increment the start iterator by provided amount
            for (uint32_t i = 0; i < iterIncrementAmount && startIt != endIt; ++i, ++startIt)
            {
            }
            continue;
        }
        else
        {
            _mm_storeu_si128((__m128i*) (&hashes->flags[0]), itemflags);
        }
#else
        if (hashes.flags[0] == citem->myHashData->arrowData->docId
            && hashes.flags[1] == citem->myHashData->arrowData->rowNumber)
        {
            continue;
        }

        hashes.flags[0] = citem->myHashData->arrowData->docId;
        hashes.flags[1] = citem->myHashData->arrowData->rowNumber;
#endif


        double match = JaccardTurbo2(citem->myHashData->hashes.get(), (int)citem->myHashData->hashLen,
            &hashes->hashes[0], (int)hashes->hashLen, earlyOut);

        if (match > dupeThreash)
        {
            //we are done
            workerThreadStopper.request_stop();
            return true;
        }

        //increment the start iterator by provided amount
        for (uint32_t i = 0; i < iterIncrementAmount && startIt != endIt; ++i, ++startIt)
        {
        }
    }

    return false;
}

//container of containers
template<typename UINT_HASH_TYPE, uint32_t MAX_HASH_LEN, uint32_t BLOCK_SIZE>
bool WorkerThreadFunc2(std::stop_source workerThreadStopper,
    auto startIt,
    auto endIt,
    double earlyOut, double dupeThreash, CompareItem<UINT_HASH_TYPE>* citem)
{
    bool matched = false;
    for (; startIt != endIt && matched == false && !workerThreadStopper.stop_requested(); ++startIt)
    {
        auto listStartIt = (*startIt)->begin();
        auto listEndIt = (*startIt)->end();
        matched = WorkThreadFunc<UINT_HASH_TYPE, MAX_HASH_LEN, BLOCK_SIZE>(
            workerThreadStopper, listStartIt, listEndIt, 1, earlyOut, dupeThreash, citem);
    }

    return matched;
}



//split this out to try different methods
template<typename LSHMap>
class CompareStrat
{
    LSHMap* lshMap;

    std::list< HashBlockEntry<UINT_HASH_TYPE, MAX_HASH_LEN>* > potentialmatchCandidates;
    //std::vector< typename LSHHashMap::BucketHashPointerList* > potentialmatchCandidates;
    //potentialmatchCandidates.reserve(MAX_HASH_LEN);

    //get a list of potential candidates
    void CheckAgainstHashes(citem)
    {
        //potentialmatchCandidates.resize(0);
        potentialmatchCandidates.clear();


        CompareItem< UINT_HASH_TYPE>* citem = new CompareItem< UINT_HASH_TYPE>(std::move(workItem));

        bandHashMap.Hash(citem->myHashData->hashes.get(), citem->myHashData->hashLen, bandHashes.begin());
        // size_t totalPotentialCandidates = bandHashMap.GetCollided(bandHashes.begin(), potentialmatchCandidates);


        size_t totalPotentialCandidates = bandHashMap.GetCollidedSet(bandHashes.begin(), potentialmatchCandidates);
    }

    //potential candidate count
    bool HasPotentialMatches()
    {
        return potentialmatchCandidates.size() < bandHashMap.GetBands() / 4;
    }

    //iterator for worker thread start
    bool CheckFormatches()
    {
        //do iteration, wait on futures, etc.
    }

    //add to lsh
    void AddToLSH()
    {
    }

};




class IComparerThread
{
public:
    virtual ~IComparerThread() {}

    virtual void IncreaseMaxWorkerThreads(int amt) = 0;
    virtual uint32_t GetWorkerThreadCount() = 0;
    virtual void WaitForFinish() = 0;
    virtual size_t GetUniqueItemsCount() = 0;
    virtual uint64_t GetNumLSHEntries() = 0;
    virtual uint64_t GetEstimatedLSHMemoryUsageMB() = 0;
    virtual uint64_t GetMemUsageMB() = 0;
    virtual LockableQueue< CompareThreadDupeItem* >* GetOutputQueuePtr() = 0;
    virtual size_t GetRemainingWork() = 0;
    virtual uint64_t GetComparedItems() = 0;
};

template<typename UINT_HASH_TYPE, uint32_t MAX_HASH_LEN, uint32_t BLOCK_SIZE, typename UINT_BAND_HASH_TYPE >
class ComparerThread: public IComparerThread
{
protected:
    bool m_throwOutDupes;
    std::stop_source m_stop;
    std::atomic<uint32_t> maxThreadWorkers;
    BS::thread_pool* threadPool;
    uint32_t workChunkSize;
    uint64_t comparedItems;

    std::queue<HasherThreadOutputData<UINT_HASH_TYPE>* > workQueue;

    LockableQueue< CompareThreadDupeItem* > duplicateItems;

    HashBlockAllocator<UINT_HASH_TYPE, MAX_HASH_LEN, BLOCK_SIZE> hashblocks;

    typedef LSHBandHashMap<UINT_HASH_TYPE, UINT_BAND_HASH_TYPE, MAX_HASH_LEN> LSHHashMap;
    LSHHashMap bandHashMap;

public:
    ComparerThread(bool throwOutDupes, uint32_t _workChunkSize, BS::thread_pool* _threadPool, uint64_t maxDocuments, uint32_t bands, uint64_t buckets, LSHHashMap::LSH_TYPE_ENUM lshType, uint32_t maxThreadWorkers = 0)
        :m_throwOutDupes(throwOutDupes),
        maxThreadWorkers(maxThreadWorkers),
        threadPool(_threadPool),
        workChunkSize(_workChunkSize),
        comparedItems(0),
        hashblocks(HashBlockAllocator<UINT_HASH_TYPE, MAX_HASH_LEN, BLOCK_SIZE>( (maxDocuments / BLOCK_SIZE) + BLOCK_SIZE)),
        bandHashMap( LSHHashMap(bands, buckets, lshType))
    {
    }

    ~ComparerThread() final
    {
    }

    void IncreaseMaxWorkerThreads(int amt) final
    {
        maxThreadWorkers.fetch_add(amt);
    }

    uint32_t GetWorkerThreadCount() final
    {
        return maxThreadWorkers.load();
    }

    void WaitForFinish() final
    {
        m_stop.request_stop();
    }

    size_t GetUniqueItemsCount() final
    {
        return hashblocks.NumEntries();
    }

    uint64_t GetNumLSHEntries() final
    {
        return bandHashMap.GetNumEntries();
    }

    uint64_t GetEstimatedLSHMemoryUsageMB() final
    {
        return bandHashMap.GetEstimatedMemoryUsageBytes() / (1024ULL * 1024ULL);
    }

    uint64_t GetMemUsageMB() final
    {
        return hashblocks.MemoryUsage() / (1024ULL * 1024ULL);
    }

    LockableQueue< CompareThreadDupeItem* >* GetOutputQueuePtr() final
    {
        return &duplicateItems;
    }

    size_t GetRemainingWork() final
    {
        return workQueue.size();
    }

    uint64_t GetComparedItems() final
    {
        return comparedItems;
    }

    void EnterProcFunc(LockableQueue< HasherThreadOutputData<UINT_HASH_TYPE>* >* hashedDataQueue, double earlyOut, double dupeThreash)
    {
        //this guy needs to compare each incoming hashed data against all prexisting data, gonna be slow.        
        HasherThreadOutputData< UINT_HASH_TYPE>* workItem;

        std::vector<UINT_BAND_HASH_TYPE> bandHashes;
        bandHashes.resize(bandHashMap.GetBands());

        std::list< HashBlockEntry<UINT_HASH_TYPE, MAX_HASH_LEN>* > potentialmatchCandidates;
        //std::vector< typename LSHHashMap::BucketHashPointerList* > potentialmatchCandidates;
        //potentialmatchCandidates.reserve(MAX_HASH_LEN);

        while (!m_stop.stop_requested() || hashedDataQueue->Length() > 0)
        {
            if (hashedDataQueue->try_pop_range(&workQueue, workChunkSize, 1000ms) == 0)
            {
                std::this_thread::sleep_for(25ms);
                continue;
            }

            while (workQueue.size() > 0)
            {
                ++comparedItems;

                workItem = workQueue.front();
                workQueue.pop();

                //potentialmatchCandidates.resize(0);
                potentialmatchCandidates.clear();


                CompareItem< UINT_HASH_TYPE>* citem = new CompareItem< UINT_HASH_TYPE>(std::move(workItem));
                
                bandHashMap.Hash(citem->myHashData->hashes.get(), citem->myHashData->hashLen, bandHashes.begin());
               // size_t totalPotentialCandidates = bandHashMap.GetCollided(bandHashes.begin(), potentialmatchCandidates);

                
                size_t totalPotentialCandidates = bandHashMap.GetCollidedSet(bandHashes.begin(), potentialmatchCandidates);
                //early out since no checks
                //TODO: tune this...
                //TODO: this is fast when there are 256 buckets, because we *will* hit exact matches
                //if (hashblocks.IsEmpty() || potentialmatchCandidates.size() < 200) [[unlikely]]
                
                if (hashblocks.IsEmpty() || potentialmatchCandidates.size() < bandHashMap.GetBands() / 4) [[unlikely]]
                
                {
                    auto entry = hashblocks.AddItem(citem->myHashData->hashes.get(), citem->myHashData->hashLen);

                    bandHashMap.AddToMap(bandHashes.begin(), entry);

                    delete citem;
                    citem = nullptr;
                    continue;
                }

                //spread the work of comparing across threads..  
                
                BS::multi_future<bool> internalCompareThreadFutures;

                std::stop_source workerThreadStopper;

                //check if we should jsut do the whole thing on one thread...
                bool matched = false;
                if (totalPotentialCandidates < singleThreadSize) //faster not to wait on threads and locks and all that
                {
                    matched = WorkThreadFunc<UINT_HASH_TYPE, MAX_HASH_LEN, BLOCK_SIZE>(
                        workerThreadStopper, potentialmatchCandidates.begin(), potentialmatchCandidates.end(), 1, earlyOut, dupeThreash, citem);

                    //matched = WorkerThreadFunc2<UINT_HASH_TYPE, MAX_HASH_LEN, BLOCK_SIZE>(
                    //    workerThreadStopper, potentialmatchCandidates.begin(), potentialmatchCandidates.end(), earlyOut, dupeThreash, citem);
                }
                else
                {
                    //split the list by offseting start iterators, and incrementing them by different amounts
                    //for (auto pcIt = potentialmatchCandidates.begin(); pcIt != potentialmatchCandidates.end() && !workerThreadStopper.stop_requested(); ++pcIt)
                    {
                        //auto list = *pcIt;
                        auto list = &potentialmatchCandidates;

                        //should chunkify these vectors as well, since some can be much longer than others
                        uint32_t numSlices = (uint32_t)list->size() / singleThreadSize;
                        if (numSlices == 0)
                            numSlices = 1;
                        auto sliceIt = list->begin();
                        auto endIt = list->end();

                        for (size_t slice = 0; slice < numSlices && sliceIt != list->end() && !workerThreadStopper.stop_requested(); slice++, sliceIt++)
                        {
                            internalCompareThreadFutures.push_back(
                                threadPool->submit([this, workerThreadStopper, sliceIt, endIt, numSlices , &earlyOut, &dupeThreash, citem]() {

                                    return WorkThreadFunc<UINT_HASH_TYPE, MAX_HASH_LEN, BLOCK_SIZE>(
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

                if (isDupe)
                {
                    //for processing in the removal of dupes
                    CompareThreadDupeItem* dupeItem = new CompareThreadDupeItem(citem->myHashData->arrowData->docId, citem->myHashData->arrowData->rowNumber);
                    duplicateItems.push(std::move(dupeItem));
                }
                else
                {
                    //for testing, add a lot more
                    
#if LOAD_TEST
                    static bool add = true;
                    if (add == true)
                    {
                        add = false;
                        for (int i = 0; i < 5000000; ++i)
                        {
                            auto entry = hashblocks.AddItem(citem->myHashData->hashes.get(), citem->myHashData->hashLen);
                            bandHashMap.AddToMap(bandHashes.begin(), entry);
                        }
                    }
#endif
                    auto entry = hashblocks.AddItem(citem->myHashData->hashes.get(), citem->myHashData->hashLen);
                    bandHashMap.AddToMap(bandHashes.begin(), entry);

                }

                delete citem;
                citem = nullptr;

            }//do work

        }//thread running


    }//thread func
};