#pragma once

#include "ComparerThread.h"
#include "Hashing.h"
#include "LockableQueue.h"


class DupeResolverThread
{
protected:
    std::thread* m_thread = nullptr;
    std::stop_source m_stop;

public:
    DupeResolverThread()
    {
    }

    void Start(std::list< ComparerThreadOutputData* >* allComparedItems)
    {
        m_thread = new std::thread(&DupeResolverThread::EnterProcFunc, this, m_stop, allComparedItems);
    }

    void WaitForFinish()
    {
        m_thread->join();
    }

protected:
    void EnterProcFunc(std::stop_source stop, std::list< ComparerThreadOutputData* >* allComparedItems)
    {
        int count = 0;
        for (auto it = allComparedItems->begin(); it != allComparedItems->end(); it++)
        {
            std::cout << "Item " << ++count << ": (" << (*it)->maxMatchedVal << ")" << std::endl;
        }
    }
};
