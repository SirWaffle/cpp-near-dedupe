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


class ComparerThread
{
protected:
    bool m_throwOutDupes;
    std::thread* m_thread = nullptr;
    std::stop_source m_stopCompare;

public:
    ComparerThread(bool throwOutDupes)
        :m_throwOutDupes(throwOutDupes)
    {
    }

    void Start(LockableQueue< HasherThreadOutputData* >* hashedDataQueue, std::list< ComparerThreadOutputData* >* allComparedItems, LockableQueue< ComparerThreadOutputData* >* duplicateItems, int chunkSize, double earlyOut, double dupeThreash, int numHashes)
    {
        m_thread = new std::thread(&ComparerThread::EnterProcFunc, this, m_stopCompare, hashedDataQueue, allComparedItems, duplicateItems, 64, earlyOut, dupeThreash, numHashes);
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
    //this guy needs to compare each incoming hashed data against all prexisting data, gonna be slow.
    std::queue<HasherThreadOutputData* > workQueue;
    HasherThreadOutputData* workItem;

    while (!stop.stop_requested() || hashedDataQueue->Length() > 0)
    {
        std::this_thread::sleep_for(1ms);
        for (int i = 0; i < chunkSize; ++i)
        {
            if (hashedDataQueue->try_pop(&workItem, 1ms))
            {
                workQueue.push(workItem);
            }
            else
            {
                break;
            }
        }

        if (workQueue.size() == 0)
        {
            std::this_thread::sleep_for(10ms);
            continue;
        }

        //ConsoleLogDebug(std::format("compare hashes work items length {}, pending items {}\n", workQueue.size(), hashedDataQueue->Length()));

        while (workQueue.size() > 0)
        {
            workItem = workQueue.front();
            workQueue.pop();


            ComparerThreadOutputData* citem = new ComparerThreadOutputData();
            citem->maxMatchedData = nullptr;
            citem->maxMatchedVal = 0.0;
            citem->myHashData = workItem;

            if (allComparedItems->size() == 0)
            {
                allComparedItems->push_back(citem);
                continue;
            }

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
                allComparedItems->push_back(citem);
        }
    }
}