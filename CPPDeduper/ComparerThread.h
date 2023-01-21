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

#include <intrin.h>
#include <immintrin.h>
#include <nmmintrin.h>
#include <emmintrin.h>


#define USE_INTRIN true
#define BUCKETS 64
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
    __m128i itemflags;
    itemflags.m128i_i64[0] = citem->myHashData->arrowData->docId;
    itemflags.m128i_i64[1] = citem->myHashData->arrowData->rowNumber;
#endif

    for (size_t ind = 0; ind < hashesVec->size() && !workerThreadStopper.stop_requested(); ++ind)
    {
        auto& hashes = *(*hashesVec)[ind];

#if USE_INTRIN 
        __m128i flags = _mm_loadu_si128((__m128i*) (&hashes.flags[0]));
        __m128i cmp = _mm_cmpeq_epi64(itemflags, flags);
        if (cmp.m128i_u16[0] != 0 && cmp.m128i_u64[1] != 0)
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
    LSHHashMap bandHashMap;

public:
    ComparerThread(bool throwOutDupes, uint32_t _workChunkSize, BS::thread_pool* _threadPool, uint64_t maxDocuments, uint32_t maxThreadWorkers = 0)
        :m_throwOutDupes(throwOutDupes),
        maxThreadWorkers(maxThreadWorkers),
        threadPool(_threadPool),
        workChunkSize(_workChunkSize),
        hashblocks(HashBlockAllocator<UINT_HASH_TYPE, MAX_HASH_LEN, BLOCK_SIZE>( (maxDocuments / BLOCK_SIZE) + BLOCK_SIZE)),
        bandHashMap(LSHHashMap( BUCKETS, MAX_HASH_LEN / BUCKETS ) )
    {
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

        std::vector< LSHHashMap::BucketHashPointerListPtr> potentialmatchCandidates;
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

                uint32_t threadsToUse = maxThreadWorkers.load();

                CompareItem< UINT_HASH_TYPE>* citem = new CompareItem< UINT_HASH_TYPE>(std::move(workItem));

                UINT_BAND_HASH_TYPE bandHashes[BUCKETS];
                bandHashMap.Hash(citem->myHashData->hashes.get(), citem->myHashData->hashLen, bandHashes);

                for(uint32_t b = 0; b < bandHashMap.GetBandSize(); b++)
                { 
                    auto matched = bandHashMap.GetCollided(bandHashes[b]);
                    if(matched != nullptr)
                        potentialmatchCandidates.push_back( matched  );
                }

                //early out since no checks
                if (hashblocks.IsEmpty() || potentialmatchCandidates.size() == 0) [[unlikely]]
                {
                    auto entry = hashblocks.AddItem(citem->myHashData->hashes.get(), citem->myHashData->hashLen);
                    for (uint32_t b = 0;  b < bandHashMap.GetBandSize(); b++)
                    {
                        bandHashMap.AddToMap(bandHashes[b], entry);
                    }
                    delete citem;
                    citem = nullptr;
                    continue;
                }

                //spread the work of comparing across threads..  
                
                BS::multi_future<bool> internalCompareThreadFutures;

                std::stop_source workerThreadStopper;

                for(int pc = 0; pc < potentialmatchCandidates.size(); ++pc)
                {
                    auto list = potentialmatchCandidates[pc];

                    internalCompareThreadFutures.push_back(
                        threadPool->submit([this, workerThreadStopper, list, &earlyOut, &dupeThreash, citem]() {

                            return WorkThreadFunc<UINT_HASH_TYPE, MAX_HASH_LEN, BLOCK_SIZE>(
                                workerThreadStopper, list, earlyOut, dupeThreash, citem);
                            }
                        )
                    );                  
                }




                //wait for worker threads
                internalCompareThreadFutures.wait();



                //run through each and look at result, if its a dupe, pass it to dupes, if its not, we need to compare all the
                bool isDupe = false;
                for (size_t i = 0; i < internalCompareThreadFutures.size(); ++i)
                {
                    bool result = internalCompareThreadFutures[i].get();
                    if (result == true)
                    {
                        isDupe = true;
                        break;
                    }
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
                    /*
#pragma message("testing code, bad!")
                    for (int i = 0; i < 1000; ++i)
                    */
                    {
                        auto entry = hashblocks.AddItem(citem->myHashData->hashes.get(), citem->myHashData->hashLen);
                        for (uint32_t b = 0;  b < bandHashMap.GetBandSize(); b++)
                        {
                            bandHashMap.AddToMap(bandHashes[b], entry);
                        }
                    }
                }

                delete citem;
                citem = nullptr;

            }//do work

        }//thread running

    }//thread func
};