#pragma once

#include "HasherThread.h"
#include "Hashing.h"
#include "LockableQueue.h"
#include "Jaccard.h"
#include <list>
#include <thread>

#include "HashTable.h"
#include "ThreadPool.h"

namespace BruteForce
{
    /*
    struct CompareThreadDupeItem
    {
        CompareThreadDupeItem(uint32_t _docId, int64_t _rowNumber)
            :docId(_docId),
            rowNumber(_rowNumber)
        {}

        uint32_t docId;
        int64_t rowNumber;
    };
    */

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

    //using vector container
    /*
    template<typename UINT_HASH_TYPE, uint32_t MAX_HASH_LEN, uint32_t BLOCK_SIZE>
    bool WorkThreadFunc(
        std::stop_source workerThreadStopper,
        HashBlockAllocator<UINT_HASH_TYPE, MAX_HASH_LEN, BLOCK_SIZE>& hashblocks, uint32_t inclusiveStartInd, uint32_t exclusiveEndInd,
        double earlyOut, double dupeThreash, CompareItem<UINT_HASH_TYPE>* citem)
    {
        //compare incoming against all others, update the its max value.
        //this will prioritize removing later documents that match, not the first one
        for (uint32_t blockInd = inclusiveStartInd; blockInd < exclusiveEndInd; blockInd++)
        {
            Block<UINT_HASH_TYPE, MAX_HASH_LEN, BLOCK_SIZE>* block = hashblocks.GetBlockPtr(blockInd);

            for (uint32_t hashInd = 0; hashInd < block->size && !workerThreadStopper.stop_requested(); ++hashInd)
            {
                double match = JaccardTurbo(citem->myHashData->hashes.get(), citem->myHashData->hashLen,
                    &(block->entries[hashInd].hashes[0]), (int)(block->entries[hashInd].hashLen),
                    earlyOut);

                if (match >= dupeThreash)
                {
                    //we are done
                    workerThreadStopper.request_stop();
                    return true;
                }
            }
        }
        return false;
    }*/

    //list container refactor
    template<typename UINT_HASH_TYPE, uint32_t MAX_HASH_LEN, uint32_t BLOCK_SIZE>
    bool BruteForceWorkThreadFunc(
        std::stop_source workerThreadStopper,
        typename HashBlockAllocator<UINT_HASH_TYPE, MAX_HASH_LEN, BLOCK_SIZE>::iterator start,
        typename HashBlockAllocator<UINT_HASH_TYPE, MAX_HASH_LEN, BLOCK_SIZE>::iterator exclusiveEnd,
        double earlyOut, double dupeThreash, CompareItem<UINT_HASH_TYPE>* citem)
    {
        //compare incoming against all others, update the its max value.
        //this will prioritize removing later documents that match, not the first one
        for (; start < exclusiveEnd; start++)
        {
            Block<UINT_HASH_TYPE, MAX_HASH_LEN, BLOCK_SIZE>* block = *start;

            for (uint32_t hashInd = 0; hashInd < block->size && !workerThreadStopper.stop_requested(); ++hashInd)
            {
                double match = JaccardTurbo(citem->myHashData->hashes.get(), citem->myHashData->hashLen,
                    &(block->entries[hashInd].hashes[0]), (int)(block->entries[hashInd].hashLen),
                    earlyOut);

                if (match >= dupeThreash)
                {
                    //we are done
                    workerThreadStopper.request_stop();
                    return true;
                }
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

    public:
        ComparerThread(bool throwOutDupes, uint32_t _workChunkSize, BS::thread_pool* _threadPool, uint32_t numBuckets, uint64_t maxDocuments, uint32_t maxThreadWorkers = 0)
            :m_throwOutDupes(throwOutDupes),
            maxThreadWorkers(maxThreadWorkers),
            threadPool(_threadPool),
            workChunkSize(_workChunkSize),
            hashblocks(HashBlockAllocator<UINT_HASH_TYPE, MAX_HASH_LEN, BLOCK_SIZE>((maxDocuments / BLOCK_SIZE) + BLOCK_SIZE))
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

            while (!m_stop.stop_requested() || hashedDataQueue->Length() > 0)
            {
                if (hashedDataQueue->try_pop_range(&workQueue, workChunkSize, 10ms) == 0)
                {
                    std::this_thread::sleep_for(25ms);
                    continue;
                }

                while (workQueue.size() > 0)
                {
                    ++comparedItems;

                    workItem = workQueue.front();
                    workQueue.pop();

                    //early out since no checks
                    if (hashblocks.IsEmpty()) [[unlikely]]
                    {
                        hashblocks.AddItem(workItem->hashes.get(), workItem->hashLen);
                        delete workItem;
                        workItem = nullptr;
                        continue;
                    }

                        //spread the work of comparing across threads..  
                    uint32_t threadsToUse = maxThreadWorkers.load();
                    BS::multi_future<bool> internalCompareThreadFutures;

                    //parallelize across blocks, one item at a time
                    uint32_t blocksPerThread = (uint32_t)hashblocks.NumBlocks() / threadsToUse;
                    int32_t extraBlocks = 0;
                    if (hashblocks.NumBlocks() > threadsToUse && blocksPerThread * threadsToUse < (uint32_t)hashblocks.NumBlocks())
                    {
                        extraBlocks = (uint32_t)hashblocks.NumBlocks() - (blocksPerThread * threadsToUse);
                        blocksPerThread += 1;
                    }

                    if (blocksPerThread == 0)
                        ++blocksPerThread;


                    uint32_t inclusiveStartInd = 0;
                    uint32_t exclusiveEndInd = blocksPerThread;

                    std::stop_source workerThreadStopper;

                    CompareItem< UINT_HASH_TYPE>* citem = new CompareItem< UINT_HASH_TYPE>(std::move(workItem));
                    do
                    {
                        //TODO bad perf here but this is just for testing for now...
                        internalCompareThreadFutures.push_back(
                            threadPool->submit([this, workerThreadStopper, inclusiveStartInd, exclusiveEndInd, &earlyOut, &dupeThreash, citem]() {
                            
                                typename HashBlockAllocator<UINT_HASH_TYPE, MAX_HASH_LEN, BLOCK_SIZE>::iterator start = hashblocks.Begin() + inclusiveStartInd;
                                typename HashBlockAllocator<UINT_HASH_TYPE, MAX_HASH_LEN, BLOCK_SIZE>::iterator end = hashblocks.Begin() + exclusiveEndInd;

                                return BruteForceWorkThreadFunc<UINT_HASH_TYPE, MAX_HASH_LEN, BLOCK_SIZE>(workerThreadStopper,
                                        start, end, earlyOut, dupeThreash, citem);
                                }
                            )
                        );

                        if (extraBlocks > 0)
                        {
                            extraBlocks--;
                            if (extraBlocks == 0)
                            {
                                blocksPerThread -= 1;
                            }
                        }

                        inclusiveStartInd += blocksPerThread;
                        exclusiveEndInd += blocksPerThread;

                        if (hashblocks.NumBlocks() <= exclusiveEndInd)
                            exclusiveEndInd = (uint32_t)hashblocks.NumBlocks();

                    } while (inclusiveStartInd < exclusiveEndInd);

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
                        for (int i = 0; i < 10000; ++i)
                            hashblocks.AddItem(citem->myHashData->hashes.get(), citem->myHashData->hashLen);
                    }

                    delete citem;
                    citem = nullptr;

                }//do work

            }//thread running

        }//thread func
    };
}