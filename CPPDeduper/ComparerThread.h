#pragma once

#include "HasherThread.h"
#include "Hashing.h"
#include "LockableQueue.h"
#include "Jaccard.h"
#include <list>
#include <thread>



struct ComparerThreadOutputData
{
    HasherThreadOutputData* myHashData;
    double maxMatchedVal;
    HasherThreadOutputData* maxMatchedData;
};

//TODO: reuse the internal threads for speedup PERF
class InternalComparerThread
{
protected:
    std::thread* m_thread = nullptr;
    std::stop_source m_stopCompare;
    double maxMatchVal = 0.0;
    ComparerThreadOutputData* item = nullptr;
    ComparerThreadOutputData* dupeItem = nullptr;

public:
    InternalComparerThread()
    {
    }

    void Start(std::list< ComparerThreadOutputData* >* allComparedItems, double earlyOut, double dupeThreash, int numHashes, ComparerThreadOutputData* citem)
    {
        item = citem;
        m_thread = new std::thread(&InternalComparerThread::EnterProcFunc, this, m_stopCompare, allComparedItems, earlyOut, dupeThreash, numHashes, citem);
    }

    void WaitForFinish()
    {
        m_stopCompare.request_stop();
        m_thread->join();
    }

    double GetMaxMatchVal()
    {
        return maxMatchVal;
    }

    ComparerThreadOutputData* GetComparerItem()
    {
        return item;
    }

    ComparerThreadOutputData* GetDupeItem()
    {
        return dupeItem;
    }

protected:
    void EnterProcFunc(std::stop_source stop, std::list< ComparerThreadOutputData* >* allComparedItems, double earlyOut, double dupeThreash, int numHashes, ComparerThreadOutputData* citem)
    {
        //compare incoming against all others, update the its max value.
//this wilkl prioritize removing later documents that match, not the first one
        for (auto it = allComparedItems->begin(); it != allComparedItems->end() && citem->maxMatchedVal < dupeThreash; it++)
        {
#pragma message("figure out intrinsics on gcc")
#ifndef __GNUC__
            double match = JaccardTurbo(citem->myHashData->hashes.get(), numHashes,
                (*it)->myHashData->hashes.get(), numHashes,
                earlyOut);
#else
            double match = JaccardFast(citem->myHashData->hashes.get(), numHashes,
                (*it)->myHashData->hashes.get(), numHashes,
                earlyOut);
#endif
            if (match > maxMatchVal)
            {
                maxMatchVal = match;
                dupeItem = (*it);

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
    std::thread* m_thread = nullptr;
    std::stop_source m_stopCompare;
    uint32_t numberOfInternalThreads;

public:
    ComparerThread(bool throwOutDupes, uint32_t interalThreads)
        :m_throwOutDupes(throwOutDupes),
        numberOfInternalThreads(interalThreads)
    {
    }

    void Start(LockableQueue< HasherThreadOutputData* >* hashedDataQueue, std::list< ComparerThreadOutputData* >* allComparedItems, LockableQueue< ComparerThreadOutputData* >* duplicateItems, int chunkSize, double earlyOut, double dupeThreash, int numHashes)
    {
        m_thread = new std::thread(&ComparerThread::EnterProcFunc, this, m_stopCompare, hashedDataQueue, allComparedItems, duplicateItems, chunkSize, earlyOut, dupeThreash, numHashes);
    }

    void WaitForFinish()
    {
        m_stopCompare.request_stop();
        m_thread->join();
    }

protected:
    void EnterProcFunc(std::stop_source stop, LockableQueue< HasherThreadOutputData* >* hashedDataQueue,
        std::list< ComparerThreadOutputData* >* allComparedItems, 
        LockableQueue< ComparerThreadOutputData* >* duplicateItems, 
        int chunkSize,
        double earlyOut, double dupeThreash, int numHashes);
};

void ComparerThread::EnterProcFunc(std::stop_source stop, LockableQueue< HasherThreadOutputData* >* hashedDataQueue, std::list< ComparerThreadOutputData* >* allComparedItems, LockableQueue< ComparerThreadOutputData* >* duplicateItems, int chunkSize, double earlyOut, double dupeThreash, int numHashes)
{
    std::vector< InternalComparerThread* > internalCompareThread(numberOfInternalThreads, nullptr);
    chunkSize = chunkSize; //do nothing, unreferenced param warning

    //this guy needs to compare each incoming hashed data against all prexisting data, gonna be slow.
    std::queue<HasherThreadOutputData* > workQueue;
    HasherThreadOutputData* workItem;

    while (!stop.stop_requested() || hashedDataQueue->Length() > 0)
    {
        if (hashedDataQueue->try_pop_range(&workQueue, numberOfInternalThreads, 1ms) == 0)
        {
            std::this_thread::sleep_for(50ms);
            continue;
        }

        //ConsoleLogDebug(std::format("compare hashes work items length {}, pending items {}\n", workQueue.size(), hashedDataQueue->Length()));

        //TODO: PERF: split into multiple threads, compare against all existing, then after join, compare each against each other before adding to list of uniques
        while (workQueue.size() > 0)
        {
            //early out since no checks
            if (allComparedItems->size() == 0)
            {
                workItem = workQueue.front();
                workQueue.pop();

                ComparerThreadOutputData* citem = new ComparerThreadOutputData();
                citem->maxMatchedData = nullptr;
                citem->maxMatchedVal = 0.0;
                citem->myHashData = workItem;

                allComparedItems->push_back(citem);
                continue;
            }

            //spread the work of comparing across threads..            
            for (size_t i = 0; i < numberOfInternalThreads && workQueue.size() > 0; ++i)
            {
                workItem = workQueue.front();
                workQueue.pop();

                ComparerThreadOutputData* citem = new ComparerThreadOutputData();
                citem->maxMatchedData = nullptr;
                citem->maxMatchedVal = 0.0;
                citem->myHashData = workItem;

                auto thread = new InternalComparerThread();
                thread->Start(allComparedItems, earlyOut, dupeThreash, numHashes, std::move(citem));
                internalCompareThread[i] = thread;
            }

            //let them crunch and wait for all to finish
            for (size_t i = 0; i < internalCompareThread.size(); ++i)
            {
                if(internalCompareThread[i] != nullptr)
                    internalCompareThread[i]->WaitForFinish();
            }

            //run through each and look at result, if its a dupe, pass it to dupes, if its not, we need to compare all the
            std::list<ComparerThreadOutputData* > potenialKeepers;
            for (size_t i = 0; i < internalCompareThread.size(); ++i)
            {
                if (internalCompareThread[i] == nullptr)
                    continue;

                double match = internalCompareThread[i]->GetMaxMatchVal();
                ComparerThreadOutputData* citem = internalCompareThread[i]->GetComparerItem();
                ComparerThreadOutputData* dupeItem = internalCompareThread[i]->GetDupeItem();

                if (match >= dupeThreash)
                {
                    citem->maxMatchedVal = match;

                    std::cout << "found dupe: " << citem->myHashData->batchNum << ", " << citem->myHashData->docId << ", " << citem->myHashData->stringArrayInd << " of "
                        << dupeItem->myHashData->batchNum << ", " << dupeItem->myHashData->docId << ", " << dupeItem->myHashData->stringArrayInd << std::endl;

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

            //delete threads for now TODO: PERF - reuse
            for (size_t i = 0; i < internalCompareThread.size(); ++i)
            {
                if (internalCompareThread[i] != nullptr)
                {
                    delete internalCompareThread[i];
                    internalCompareThread[i] = nullptr;
                }
            }

            //now, compare all potential keepers against each other...
            while(potenialKeepers.size() > 0)
            {
                ComparerThreadOutputData* citem = potenialKeepers.front();
                potenialKeepers.pop_front();

                //compare incoming against all others, update the its max value.
                //this wilkl prioritize removing later documents that match, not the first one
                for (auto it = potenialKeepers.begin(); it != potenialKeepers.end() && citem->maxMatchedVal < dupeThreash; it++)
                {
#pragma message("figure out intrinsics on gcc")
#ifndef __GNUC__
                    double match = JaccardTurbo(citem->myHashData->hashes.get(), numHashes,
                        (*it)->myHashData->hashes.get(), numHashes,
                        earlyOut);
#else
                    double match = JaccardFast(citem->myHashData->hashes.get(), numHashes,
                        (*it)->myHashData->hashes.get(), numHashes,
                        earlyOut);
#endif
                    if (match > citem->maxMatchedVal)
                    {
                        citem->maxMatchedVal = match;
                        citem->maxMatchedData = (*it)->myHashData;

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
                    allComparedItems->push_back(std::move(citem));
            }

        }
    }
}