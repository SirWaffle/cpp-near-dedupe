#pragma once

#include "HasherThread.h"
#include "Hashing.h"
#include "LockableQueue.h"
#include "Jaccard.h"
#include <list>
#include <thread>

#include "ThreadPool.h"


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
    CompareItem(HasherThreadOutputData< UINT_HASH_TYPE>* _myHashData, double _maxMatchedVal)
        :myHashData(_myHashData)
    {}

    ~CompareItem()
    {
        delete myHashData;
    }

    HasherThreadOutputData< UINT_HASH_TYPE>* myHashData;
};


//memory stuff
template<typename UINT_HASH_TYPE, uint32_t HASH_COUNT>
struct HashBlockEntry
{
    UINT_HASH_TYPE hashes[HASH_COUNT];
    uint64_t hashLen = 0;
};

template<typename UINT_HASH_TYPE, uint32_t HASH_COUNT, uint32_t BLOCK_SIZE>
struct Block
{
    UINT_HASH_TYPE size = 0;
    HashBlockEntry<UINT_HASH_TYPE, HASH_COUNT> entries[BLOCK_SIZE];

};

template<typename UINT_HASH_TYPE, uint32_t MAX_HASH_LEN, uint32_t BLOCK_SIZE>
class HashBlockAllocator
{
    //last entry is where we add stuff...
    std::vector< Block<UINT_HASH_TYPE, MAX_HASH_LEN, BLOCK_SIZE>* > fullBlocks;
    std::vector< Block<UINT_HASH_TYPE, MAX_HASH_LEN / 2, BLOCK_SIZE>* > halfBLocks;

public:
    HashBlockAllocator(uint32_t initialCapacity)
    {
        fullBlocks.capacity(initialCapacity);
        halfBLocks.capacity(initialCapacity);
    }

    void AddCompareItem(CompareItem<UINT_HASH_TYPE>* citem)
    {
        if (citem->myHashData->hashLen > MAX_HASH_LEN / 2)
        {
            Block<UINT_HASH_TYPE, MAX_HASH_LEN, BLOCK_SIZE>* b = fullBlocks[fullBlocks.size() - 1]
            (*b)[b->size].hashes = citem->myHashData->hashes;
            (*b)[b->size].hashLen = citem->myHashData->hashLen;
            b->size++;
            if (b->size == BLOCK_SIZE)
            {
                fullBlocks.push_back(new Block<UINT_HASH_TYPE, MAX_HASH_LEN, BLOCK_SIZE>());
            }
        }
        else
        {
            Block<UINT_HASH_TYPE, MAX_HASH_LEN / 2, BLOCK_SIZE>* b = halfBLocks[fullBlocks.size() - 1]
            (*b)[b->size].hashes = citem->myHashData->hashes;
            (*b)[b->size].hashLen = citem->myHashData->hashLen;
            b->size++;
            if (b->size == BLOCK_SIZE)
            {
                halfBLocks.push_back(new Block<UINT_HASH_TYPE, MAX_HASH_LEN / 2, BLOCK_SIZE>());
            }
        }
    }
};


template<typename UINT_HASH_TYPE>
std::pair< CompareItem<UINT_HASH_TYPE>*, bool> WorkThreadFunc(std::list< CompareItem<UINT_HASH_TYPE>* >& allComparedItems, double earlyOut, double dupeThreash, CompareItem<UINT_HASH_TYPE>* citem)
{
    //compare incoming against all others, update the its max value.
    //this will prioritize removing later documents that match, not the first one
    for (auto it = allComparedItems.begin(); it != allComparedItems.end(); it++)
    {
        double match = JaccardTurbo(citem->myHashData->hashes.get(), citem->myHashData->hashLen,
            (*it)->myHashData->hashes.get(), (*it)->myHashData->hashLen,
            earlyOut);

        if (match >= dupeThreash)
        {
            //we are done
            return std::pair< CompareItem<UINT_HASH_TYPE>*, bool>(citem, true);
        }
    }
    return std::pair< CompareItem<UINT_HASH_TYPE>*, bool>(citem, false);
}


template<typename UINT_HASH_TYPE, uint32_t MAX_HASH_LEN>
class ComparerThread
{
protected:
    bool m_throwOutDupes;
    std::stop_source m_stop;
    std::atomic<uint32_t> maxThreadWorkers;
    BS::thread_pool* threadPool;
    uint32_t workChunkSize;

    std::list< CompareItem<UINT_HASH_TYPE>* > uniqueItems;
    LockableQueue< CompareThreadDupeItem* > duplicateItems;

public:
    ComparerThread(bool throwOutDupes, uint32_t _workChunkSize, BS::thread_pool* _threadPool, uint32_t maxThreadWorkers = 0)
        :m_throwOutDupes(throwOutDupes),
        maxThreadWorkers(maxThreadWorkers),
        threadPool(_threadPool),
        workChunkSize(_workChunkSize)
    {
    }

    ~ComparerThread()
    {
        while (uniqueItems.size() > 0)
        {
            delete uniqueItems.front();
            uniqueItems.pop_front();
        }
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
        return uniqueItems.size();
    }

    LockableQueue< CompareThreadDupeItem* >* GetOutputQueuePtr()
    {
        return &duplicateItems;
    }

    void EnterProcFunc(LockableQueue< HasherThreadOutputData<UINT_HASH_TYPE>* >* hashedDataQueue, double earlyOut, double dupeThreash)
    {
        //this guy needs to compare each incoming hashed data against all prexisting data, gonna be slow.
        std::queue<HasherThreadOutputData<UINT_HASH_TYPE>* > workQueue;
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
                //early out since no checks
                if (uniqueItems.size() == 0) [[unlikely]]
                {
                    workItem = workQueue.front();
                    workQueue.pop();

                    CompareItem< UINT_HASH_TYPE>* citem = new CompareItem< UINT_HASH_TYPE>(std::move(workItem), 0.0);

                    //these dont need the arrow data, since they are nto being removed later
                    citem->myHashData->DeleteArrowData();
                    uniqueItems.push_back(std::move(citem));
                    continue;
                }

                    //spread the work of comparing across threads..  
                uint32_t threadsToUse = maxThreadWorkers.load();
                BS::multi_future<std::pair< CompareItem<UINT_HASH_TYPE>*, bool> > internalCompareThreadFutures;

                for (size_t i = 0; i < threadsToUse && workQueue.size() > 0; ++i)
                {
                    workItem = std::move(workQueue.front());
                    workQueue.pop();

                    CompareItem< UINT_HASH_TYPE>* citem = new CompareItem< UINT_HASH_TYPE>(std::move(workItem), 0.0);

                    internalCompareThreadFutures.push_back(
                        threadPool->submit([this, &earlyOut, &dupeThreash, citem]() {
                            return WorkThreadFunc<UINT_HASH_TYPE>(uniqueItems, earlyOut, dupeThreash, std::move(citem));
                            }
                        )
                    );
                }

                //wait for worker threads
                internalCompareThreadFutures.wait();

                //run through each and look at result, if its a dupe, pass it to dupes, if its not, we need to compare all the
                std::list<CompareItem<UINT_HASH_TYPE>* > potenialKeepers;
                for (size_t i = 0; i < internalCompareThreadFutures.size(); ++i)
                {
                    std::pair< CompareItem<UINT_HASH_TYPE>*, bool> result = internalCompareThreadFutures[i].get();
                    if (result.second == true)
                    {
                        //for processing in the removal of dupes
                        CompareThreadDupeItem* dupeItem = new CompareThreadDupeItem(result.first->myHashData->arrowData->docId, result.first->myHashData->arrowData->rowNumber);
                        duplicateItems.push(std::move(dupeItem));
                        delete result.first;
                    }
                    else
                    {
                        potenialKeepers.push_back(std::move(result.first));
                    }
                }

                //now, compare all potential keepers against each other...
                while (potenialKeepers.size() > 0)
                {
                    CompareItem< UINT_HASH_TYPE>* citem = std::move(potenialKeepers.front());
                    potenialKeepers.pop_front();

                    //compare incoming against all others, update the its max value.
                    //this wilkl prioritize removing later documents that match, not the first one
                    for (auto it = potenialKeepers.begin(); it != potenialKeepers.end(); it++)
                    {
                        double match = JaccardTurbo(citem->myHashData->hashes.get(), citem->myHashData->hashLen,
                            (*it)->myHashData->hashes.get(), (*it)->myHashData->hashLen,
                            earlyOut);

                        if (match >= dupeThreash)
                        {
                            CompareThreadDupeItem* dupeItem = new CompareThreadDupeItem(citem->myHashData->arrowData->docId, citem->myHashData->arrowData->rowNumber);
                            duplicateItems.push(std::move(dupeItem));
                            delete citem;
                            citem = nullptr;
                            break;
                        }
                    }

                    if (citem != nullptr)
                    {
                        //these dont need the arrow data, since they are nto being removed later
                        citem->myHashData->DeleteArrowData();
                        uniqueItems.push_back(std::move(citem));
                    }
                }

            }
        }
    }
};