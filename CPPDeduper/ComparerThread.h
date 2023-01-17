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
    ComparerThreadOutputData(HasherThreadOutputData* _myHashData, double _maxMatchedVal)
        :myHashData(_myHashData)
        ,maxMatchedVal(_maxMatchedVal)
    {}

    ~ComparerThreadOutputData()
    {
        delete myHashData;
    }

    HasherThreadOutputData* myHashData;
    double maxMatchedVal;
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

    void EnterProcFunc(std::list< ComparerThreadOutputData* >* allComparedItems, double earlyOut, double dupeThreash, ComparerThreadOutputData* citem)
    {
        maxMatchVal = 0.0;
        item = citem;

        //compare incoming against all others, update the its max value.
        //this will prioritize removing later documents that match, not the first one
        for (auto it = allComparedItems->begin(); it != allComparedItems->end() && citem->maxMatchedVal < dupeThreash; it++)
        {
            double match = JaccardTurbo(citem->myHashData->hashes.get(), citem->myHashData->hashLen,
                (*it)->myHashData->hashes.get(), (*it)->myHashData->hashLen,
                earlyOut);
            if (match > maxMatchVal)
            {
                maxMatchVal = match;

                if (citem->maxMatchedVal >= dupeThreash)
                {
                    //we are done
                    return;
                }
            }
        }  
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