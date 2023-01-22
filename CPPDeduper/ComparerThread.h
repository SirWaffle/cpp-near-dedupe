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


#define ALREADY_PROCESSED_CHECK true
#define USE_INTRIN true
#define UINT_BAND_HASH_TYPE uint32_t

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



template<typename UINT_HASH_TYPE, uint32_t MAX_HASH_LEN, uint32_t BLOCK_SIZE>
bool WorkThreadFunc(
    std::stop_source workerThreadStopper,
    std::vector< HashBlockEntry<UINT_HASH_TYPE, MAX_HASH_LEN>* >* hashesVec,
    double earlyOut, double dupeThreash, CompareItem<UINT_HASH_TYPE>* citem)
{
    //compare incoming against all others, update the its max value.
    //this will prioritize removing later documents that match, not the first one
#if USE_INTRIN
    __m128i itemflags = _mm_set_epi64x(citem->myHashData->arrowData->docId, citem->myHashData->arrowData->rowNumber);
#endif

    for (size_t ind = 0; ind < hashesVec->size() && !workerThreadStopper.stop_requested(); ++ind)
    {
        auto& hashes = *(*hashesVec)[ind];
#if ALREADY_PROCESSED_CHECK
#if USE_INTRIN 
        __m128i flags = _mm_loadu_si128((__m128i*) (&hashes.flags[0]));
        __m128i cmp = _mm_cmpeq_epi64(itemflags, flags);

        if (_mm_extract_epi64(cmp, 0) != 0 && _mm_extract_epi64(cmp, 1) != 0)
        {
            continue;
        }
        else
        {
            _mm_storeu_si128((__m128i*) (&hashes.flags[0]), itemflags);
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
#endif

       double match = JaccardTurbo2(citem->myHashData->hashes.get(), (int)citem->myHashData->hashLen,
                &hashes.hashes[0], (int)hashes.hashLen, earlyOut);

        if (match >= dupeThreash)
        {
            //we are done
            workerThreadStopper.request_stop();
            return true;
        }
    }

    return false;
}



template<typename UINT_HASH_TYPE, uint32_t MAX_HASH_LEN, uint32_t BLOCK_SIZE>
bool WorkThreadFunc(
    std::stop_source workerThreadStopper,
    HashBlockEntry<UINT_HASH_TYPE, MAX_HASH_LEN>* hashesVec, size_t numhashes,
    double earlyOut, double dupeThreash, CompareItem<UINT_HASH_TYPE>* citem)
{
    //compare incoming against all others, update the its max value.
    //this will prioritize removing later documents that match, not the first one
#if USE_INTRIN
    __m128i itemflags = _mm_set_epi64x(citem->myHashData->arrowData->docId, citem->myHashData->arrowData->rowNumber);
#endif

    for (size_t ind = 0; ind < numhashes && !workerThreadStopper.stop_requested(); ++ind, ++hashesVec)
    {
        auto& hashes = *hashesVec;

#if USE_INTRIN 
        __m128i flags = _mm_loadu_si128((__m128i*) (&hashes.flags[0]));
        __m128i cmp = _mm_cmpeq_epi64(itemflags, flags);

        if (_mm_extract_epi64(cmp, 0) != 0 && _mm_extract_epi64(cmp, 1) != 0)
        {
            continue;
        }
        else
        {
            _mm_storeu_si128((__m128i*) (&hashes.flags[0]), itemflags);
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
            &hashes.hashes[0], (int)hashes.hashLen, earlyOut);

        if (match >= dupeThreash)
        {
            //we are done
            workerThreadStopper.request_stop();
            return true;
        }
    }

    return false;
}


template<typename UINT_HASH_TYPE, uint32_t MAX_HASH_LEN, uint32_t BLOCK_SIZE>
class ComparerThread
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
    std::vector<LSHHashMap> bandHashMap;

public:
    ComparerThread(bool throwOutDupes, uint32_t _workChunkSize, BS::thread_pool* _threadPool, uint64_t maxDocuments, uint32_t buckets, uint32_t maxThreadWorkers = 0)
        :m_throwOutDupes(throwOutDupes),
        maxThreadWorkers(maxThreadWorkers),
        threadPool(_threadPool),
        workChunkSize(_workChunkSize),
        comparedItems(0),
        hashblocks(HashBlockAllocator<UINT_HASH_TYPE, MAX_HASH_LEN, BLOCK_SIZE>( (maxDocuments / BLOCK_SIZE) + BLOCK_SIZE))
    {
        bandHashMap.resize(buckets, LSHHashMap(buckets));
    }

    ~ComparerThread()
    {
    }

    void IncreaseMaxWorkerThreads(int amt)
    {
        maxThreadWorkers.fetch_add(amt);
    }

    uint32_t GetWorkerThreadCount()
    {
        return maxThreadWorkers.load();
    }

    void WaitForFinish()
    {
        m_stop.request_stop();
    }

    size_t GetUniqueItemsCount()
    {
        return hashblocks.NumEntries();
    }

    uint64_t GetMemUsageMB()
    {
        return hashblocks.MemoryUsage() / (1024ULL * 1024ULL);
    }

    LockableQueue< CompareThreadDupeItem* >* GetOutputQueuePtr()
    {
        return &duplicateItems;
    }

    size_t GetRemainingWork()
    {
        return workQueue.size();
    }

    uint64_t GetComparedItems()
    {
        return comparedItems;
    }

    void EnterProcFunc(LockableQueue< HasherThreadOutputData<UINT_HASH_TYPE>* >* hashedDataQueue, double earlyOut, double dupeThreash)
    {
        //this guy needs to compare each incoming hashed data against all prexisting data, gonna be slow.        
        HasherThreadOutputData< UINT_HASH_TYPE>* workItem;
        UINT_BAND_HASH_TYPE* bandHashes = new UINT_BAND_HASH_TYPE[bandHashMap[0].GetBuckets()];
        std::vector< std::vector< HashBlockEntry<UINT_HASH_TYPE, MAX_HASH_LEN>* >* > potentialmatchCandidates;
        potentialmatchCandidates.reserve(MAX_HASH_LEN);

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

                potentialmatchCandidates.resize(0);


                CompareItem< UINT_HASH_TYPE>* citem = new CompareItem< UINT_HASH_TYPE>(std::move(workItem));
                
                //only need to hash from one, they all use the same
                bandHashMap[0].Hash(citem->myHashData->hashes.get(), citem->myHashData->hashLen, bandHashes);
                size_t totalPotentialCandidates = 0;
                for(uint32_t b = 0; b < bandHashMap[0].GetBuckets(); b++)
                { 
                    auto matched = bandHashMap[b].GetCollided(bandHashes[b]);
                    if (matched != nullptr)
                    {
                        totalPotentialCandidates += matched->size();
                        potentialmatchCandidates.push_back(matched);
                    }
                }

                //early out since no checks
                if (hashblocks.IsEmpty() || potentialmatchCandidates.size() == 0) [[unlikely]]
                {
                    auto entry = hashblocks.AddItem(citem->myHashData->hashes.get(), citem->myHashData->hashLen);

                    for (uint32_t b = 0;  b < bandHashMap[0].GetBuckets(); b++)
                    {
                        bandHashMap[b].AddToMap(bandHashes[b], entry);
                    }
                    delete citem;
                    citem = nullptr;
                    continue;
                }

                //spread the work of comparing across threads..  
                
                BS::multi_future<bool> internalCompareThreadFutures;

                std::stop_source workerThreadStopper;

                //check if we should jsut do the whole thing on one thread...
                bool matched = false;
                if (totalPotentialCandidates < 4096) //faster not to wait on threads and locks and all that
                {
                    for (size_t pc = 0; pc < potentialmatchCandidates.size() && matched == false; ++pc)
                    {
                        auto list = potentialmatchCandidates[pc];
                        auto ptr = (*list)[0];
                        size_t len = list->size();

                        matched = WorkThreadFunc<UINT_HASH_TYPE, MAX_HASH_LEN, BLOCK_SIZE>(
                            workerThreadStopper, ptr, len, earlyOut, dupeThreash, citem);
                    }
                }
                else
                {
                    for (size_t pc = 0; pc < potentialmatchCandidates.size(); ++pc)
                    {
                        auto list = potentialmatchCandidates[pc];

                        if (list->size() < 4096 * 2) //no need to slice
                        {
                            auto ptr = (*list)[0];
                            size_t len = list->size();

                            internalCompareThreadFutures.push_back(
                                threadPool->submit([this, workerThreadStopper, ptr, len, &earlyOut, &dupeThreash, citem]() {

                                    return WorkThreadFunc<UINT_HASH_TYPE, MAX_HASH_LEN, BLOCK_SIZE>(
                                        workerThreadStopper, ptr, len, earlyOut, dupeThreash, citem);
                                    }
                                )
                            );
                        }
                        else //slice into more managable peices
                        {
                            //should chunkify these vectors as well, since some can be much longer than others
                            size_t sliceLen = 4096 * 2;
                            for (size_t slice = 0; slice < list->size(); slice += sliceLen)
                            {
                                size_t len = sliceLen;
                                if (slice + len >= list->size())
                                    len = list->size() - slice;

                                auto ptr = (*list)[slice];

                                internalCompareThreadFutures.push_back(
                                    threadPool->submit([this, workerThreadStopper, ptr, len, &earlyOut, &dupeThreash, citem]() {

                                        return WorkThreadFunc<UINT_HASH_TYPE, MAX_HASH_LEN, BLOCK_SIZE>(
                                            workerThreadStopper, ptr, len, earlyOut, dupeThreash, citem);
                                        }
                                    )
                                );
                            }
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
                    
//#pragma message("testing code, bad!")
                    //for (int i = 0; i < 10000; ++i)
                    
                    {
                        auto entry = hashblocks.AddItem(citem->myHashData->hashes.get(), citem->myHashData->hashLen);
                        for (uint32_t b = 0;  b < bandHashMap[0].GetBuckets(); b++)
                        {
                            bandHashMap[b].AddToMap(bandHashes[b], entry);
                        }
                    }
                }

                delete citem;
                citem = nullptr;

            }//do work

        }//thread running

        delete bandHashes;
        bandHashes = nullptr;

    }//thread func
};