#pragma once

#include "arrow/api.h"
#include "arrow/io/api.h"
#include "arrow/result.h"
#include "arrow/util/type_fwd.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"
#include <arrow/csv/api.h>
#include <arrow/ipc/api.h>

#include <chrono>
#include <iostream>

#include "ArrowLoaderThread.h"
#include "ComparerThread.h"
#include "Hashing.h"
#include "LockableQueue.h"

//resolves the dupe files, writes out to arrow memory mapped files to mimic hugging faces datasets
//only processes files completed by the loader thread, so we dont operate on the same files at the same time
class DupeResolverThread
{
protected:
    std::thread* m_thread = nullptr;
    std::stop_source m_stop;

    //for storing duplicate items by file id
    std::map<uint32_t, std::list<ComparerThreadOutputData*> > fileIdToDuplicate;

public:
    DupeResolverThread()
    {
    }

    void Start(std::list< ComparerThreadOutputData* >* allComparedItems, 
        LockableQueue< ComparerThreadOutputData* >* duplicates, 
        ArrowLoaderThread* arrowLoaderThread,
        std::vector<std::string>* fileNamesVector)
    {
        m_thread = new std::thread(&DupeResolverThread::EnterProcFunc, this, m_stop, allComparedItems, duplicates, arrowLoaderThread, fileNamesVector);
    }

    void WaitForFinish()
    {
        m_stop.request_stop();
        m_thread->join();
    }

protected:
    void EnterProcFunc(std::stop_source stop, std::list< ComparerThreadOutputData* >* allComparedItems, 
        LockableQueue< ComparerThreadOutputData* >* duplicates, 
        ArrowLoaderThread* arrowLoaderThread,
        std::vector<std::string>* fileNamesVector)
    {
        std::queue<ComparerThreadOutputData* > workQueue;
        ComparerThreadOutputData* workItem;

        //ok, we have a lot of info to work with here. items contain fingerprints for the docs, the doc id, and the batchnum it came from
        //TODO: verify this is enough to locate the original entry so we can remove it.
        //TODO: theoretically we could work in place and scrub files we already processed... dangerous to destroy data though
        //for now, dump to a new location, but for future tests see about editing in place, once this thing is trustworthy
        while (!stop.stop_requested() || duplicates->Length() > 0)
        {
            //snooze
            std::this_thread::sleep_for(10ms);
            if (duplicates->try_pop_range(&workQueue, 64, 1ms) == 0)
            {
                std::this_thread::sleep_for(200ms);
                continue;
            }

            while (workQueue.size() > 0)
            {
                workItem = workQueue.front();
                workQueue.pop();

                //we cant do anything until the arrowStreamer has completed streaming in files...
                //lets jsut start stuffing them into the map as they filter in
                auto docIdList = fileIdToDuplicate.find(workItem->myHashData->docId);
                if (docIdList == fileIdToDuplicate.end())
                {
                    fileIdToDuplicate.insert(std::pair{ workItem->myHashData->docId, std::list<ComparerThreadOutputData*>({ workItem }) } );
                }
                else
                {
                    docIdList->second.push_back(workItem);
                }
            }
        }

        
        int count = 0;
        for (auto it = fileIdToDuplicate.begin(); it != fileIdToDuplicate.end(); it++)
        {
            std::string fname = (*fileNamesVector)[(*it).first];
            std::cout << "DocId: " << (*it).first << "  Count: " << (*it).second.size() << "  Dataset path: " << fname << std::endl;
        }
        
    }
};
