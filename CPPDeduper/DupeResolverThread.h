#pragma once

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
        m_thread->join();
    }

protected:
    void EnterProcFunc(std::stop_source stop, std::list< ComparerThreadOutputData* >* allComparedItems, 
        LockableQueue< ComparerThreadOutputData* >* duplicates, 
        ArrowLoaderThread* arrowLoaderThread,
        std::vector<std::string>* fileNamesVector)
    {
        int count = 0;
        for (auto it = allComparedItems->begin(); it != allComparedItems->end(); it++)
        {
            std::cout << "Item " << ++count << ": (" << (*it)->maxMatchedVal << ")" << std::endl;
        }
    }
};
