#pragma once

#include "HasherThread.h"
#include "Hashing.h"
#include "LockableQueue.h"
#include "Jaccard.h"
#include <list>
#include <thread>

#include "ThreadPool.h"


struct ComparerThreadOutputData
{
    ComparerThreadOutputData(HasherThreadOutputData* _myHashData)
        :myHashData(_myHashData)
    {}

    ~ComparerThreadOutputData()
    {
        delete myHashData;
    }

    HasherThreadOutputData* myHashData;
};


//store hashes in contiguous memory 

struct IHashBlockEntry
{
    virtual uint32_t Size() = 0;
    virtual uint32_t* Array() = 0;
};

template<uint32_t N>
struct HashBlockEntry : public IHashBlockEntry
{
    uint32_t[N] hashes = { 0 };
    uint32_t hashLen = 0;

    HashBlockEntry()
    { }

    void Set(uint32_t* _hashes, uint32_t _hashLen)
    {
        hashLen = _hashLen;
        memcpy_s(&hashes, N, _hashes, _hashLen);
    }

    virtual uint32_t Size() final
    {
        return hashLen;
    } 

    virtual uint32_t* Array() final
    {
        return &hashes;
    }
};


//last block is garunteed to never be full, all others are full
template<uint32_t NUM_ENTRIES, uint32_t ENTRY_SIZE>
class HashBlocks
{
public:
    class Block
    {
    public:
        uint32_t tailInd = 0;
        HashBlockEntry<ENTRY_SIZE>[NUM_ENTRIES] entries;

        void AddEntry(HasherThreadOutputData* hasherData)
        {
            entries[tailInd].Set(hasherData->hashes, hasherData->hashLen);
            tailInd++;
        }

        bool IsFull() const
        {
            return tailInd == NUM_ENTRIES;
        }

        uint32_t Size() const
        {
            return tailInd;
        }
    };

    std::vector< Block* > blocks;


    HashBlocks(uint32_t startingCapacity = 100)
    {
        blocks.capacity(startingCapacity);
        AppendBlock();
    }

    ~HashBlocks()
    {
    }

    void AddEntry(HasherThreadOutputData* hasherData)
    {
        blocks[blocks.size() - 1]->AddEntry(hasherData);
        if (blocks[blocks.size() - 1]->IsFull() == true)
            AppendBlock();
    }

    void AppendBlock()
    {
        blocks.push_back(new Block());
    }

    uint32_t GetBlockCount() const
    {
        return blocks.size();
    }

    uint32_t GetBlockSize() const
    { 
        return ENTRY_SIZE;
    }
};


class InternalComparerThread
{
protected:
    double maxMatchVal = 0.0;
    ComparerThreadOutputData* item = nullptr;

public:
    InternalComparerThread()
    {
    }

    double GetMaxMatchVal()
    {
        return maxMatchVal;
    }

    ComparerThreadOutputData* GetComparerItem()
    {
        return item;
    }

    bool EnterProcFunc(hashBlock& allComparedItems, double earlyOut, double dupeThreash, ComparerThreadOutputData* citem)
    {
        for (int i = 0; i < hashBlock.size() && request_stop == false; i++)
        {
            double match = JaccardTurbo(citem->myHashData->hashes.get(), citem->myHashData->hashLen, &hashBlock[i].hashes), hashBlock[i].hashLen, earlyOut);
            if (match > dupeThreash)
                return true;
        }  
        return false;
    }
};

class ComparerThread
{
protected:
    bool m_throwOutDupes;
    std::stop_source m_stop;
    std::atomic<uint32_t> maxThreadWorkers;
    BS::thread_pool* threadPool;
    uint32_t workChunkSize;

public:
    ComparerThread(bool throwOutDupes, uint32_t _workChunkSize, BS::thread_pool* _threadPool, uint32_t maxThreadWorkers = 0)
        :m_throwOutDupes(throwOutDupes),
        maxThreadWorkers(maxThreadWorkers),
        threadPool(_threadPool),
        workChunkSize(_workChunkSize)
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

    void EnterProcFunc(LockableQueue< HasherThreadOutputData* >* hashedDataQueue,
        std::list< ComparerThreadOutputData* >* allComparedItems, 
        LockableQueue< ComparerThreadOutputData* >* duplicateItems, 
        double earlyOut, double dupeThreash);
};

void ComparerThread::EnterProcFunc(LockableQueue< HasherThreadOutputData* >* hashedDataQueue, 
        std::list< ComparerThreadOutputData* >* allComparedItems, LockableQueue< ComparerThreadOutputData* >* duplicateItems,
        double earlyOut, double dupeThreash)
{
    std::vector< InternalComparerThread > internalCompareThread;

    //this guy needs to compare each incoming hashed data against all prexisting data, gonna be slow.
    std::queue<HasherThreadOutputData* > workQueue;
    HasherThreadOutputData* workItem;

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
            if (allComparedItems->size() == 0) [[unlikely]]
            {
                workItem = workQueue.front();
                workQueue.pop();

                ComparerThreadOutputData* citem = new ComparerThreadOutputData(std::move(workItem), 0.0);

                //these dont need the arrow data, since they are nto being removed later
                citem->myHashData->DeleteArrowData();
                allComparedItems->push_back(std::move(citem));
                continue;
            }

            //spread the work of comparing across threads..  
            uint32_t threadsToUse = maxThreadWorkers.load();

            if (internalCompareThread.size() < threadsToUse) [[unlikely]]
                internalCompareThread.resize(threadsToUse);

            BS::multi_future<void> internalCompareThreadFutures;

            for (size_t i = 0; i < threadsToUse && workQueue.size() > 0; ++i)
            {
                workItem = std::move(workQueue.front());
                workQueue.pop();

                ComparerThreadOutputData* citem = new ComparerThreadOutputData(std::move(workItem), 0.0);
           
                internalCompareThreadFutures.push_back(
                    threadPool->submit(&InternalComparerThread::EnterProcFunc, 
                    &internalCompareThread[i], allComparedItems,
                    earlyOut, dupeThreash, std::move(citem))
                );
            }

            //wait for worker threads
            internalCompareThreadFutures.wait();

            //run through each and look at result, if its a dupe, pass it to dupes, if its not, we need to compare all the
            std::list<ComparerThreadOutputData* > potenialKeepers;
            for (size_t i = 0; i < internalCompareThreadFutures.size(); ++i)
            {
                double match = internalCompareThread[i].GetMaxMatchVal();
                ComparerThreadOutputData* citem = internalCompareThread[i].GetComparerItem();

                if (match >= dupeThreash)
                {
                    citem->maxMatchedVal = match;

                    /*
                    std::cout << "found dupe: " << citem->myHashData->batchNum << ", " << citem->myHashData->docId << ", " << citem->myHashData->stringArrayInd << " of "
                        << dupeItem->myHashData->batchNum << ", " << dupeItem->myHashData->docId << ", " << dupeItem->myHashData->stringArrayInd << std::endl;
                    */

                    //for processing in the removal of dupes
                    duplicateItems->push(std::move(citem));                        

                    if (m_throwOutDupes)
                    {
                        //dont add this back to the list of all docs, no need to store dupes
                        citem = nullptr;
                    }
                }

                if (citem != nullptr)
                    potenialKeepers.push_back(std::move(citem));
            }

            //now, compare all potential keepers against each other...
            while(potenialKeepers.size() > 0)
            {
                ComparerThreadOutputData* citem = std::move(potenialKeepers.front());
                potenialKeepers.pop_front();

                //compare incoming against all others, update the its max value.
                //this wilkl prioritize removing later documents that match, not the first one
                for (auto it = potenialKeepers.begin(); it != potenialKeepers.end() && citem->maxMatchedVal < dupeThreash; it++)
                {
                    double match = JaccardTurbo(citem->myHashData->hashes.get(), citem->myHashData->hashLen,
                        (*it)->myHashData->hashes.get(), (*it)->myHashData->hashLen,
                        earlyOut);

                    if (match > citem->maxMatchedVal)
                    {
                        citem->maxMatchedVal = match;

                        if (citem->maxMatchedVal > dupeThreash)
                        {
                            //for processing in the removal of dupes
                            duplicateItems->push(std::move(citem));

                            if (m_throwOutDupes && citem->maxMatchedVal > dupeThreash)
                            {
                                //dont add this back to the list of all docs, no need to store dupes
                                citem = nullptr;
                                break;
                            }
                        }

                    }
                }

                if (citem != nullptr)
                {
                    //these dont need the arrow data, since they are nto being removed later
                    citem->myHashData->DeleteArrowData();
                    allComparedItems->push_back(std::move(citem));
                }
            }

        }
    }
}