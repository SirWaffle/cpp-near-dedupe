#pragma once

#include <list>
#include <thread>

#include "ThreadPool.h"
#include "LockableQueue.h"




template<typename IN_TYPE, typename OUT_TYPE>
class LongRunningWorkerThread
{
public:
	std::stop_source m_stop;
	BS::thread_pool* threadPool;


	LockableQueue< IN_TYPE >* inQueue;
	LockableQueue< OUT_TYPE >* outQueue;

    const uint32_t workOutQueueSize = 1024;
	std::queue< OUT_TYPE > workOutQueue;

    uint32_t workChunkSize;
	std::queue< IN_TYPE > workQueue;


	LongRunningWorkerThread(BS::thread_pool* _threadPool, LockableQueue< IN_TYPE >* _inQueue, LockableQueue< OUT_TYPE >* _outQueue,  uint32_t _workChunkSize)
		:threadPool(_threadPool),
        workChunkSize(_workChunkSize),
		inQueue(_inQueue),
        outQueue(_outQueue)
	{
        if (outQueue == nullptr)
            outQueue = new LockableQueue< OUT_TYPE >();
	}

	LockableQueue< OUT_TYPE >* GetOutputQueuePtr()
	{
		return outQueue;
	}

    size_t GetRemainingWork()
    {
        return inQueue->Length();
    }

    void WaitForFinish()
    {
        m_stop.request_stop();
    }

    virtual bool DoWork(std::queue< IN_TYPE >* workQueue, std::queue< OUT_TYPE >* workOutQueue) = 0;

    virtual void Run()
    {
        while (!m_stop.stop_requested() || inQueue->Length() > 0)
        {
            if (inQueue->try_pop_range(&workQueue, workChunkSize, 10000ms) == 0)
            {
                std::this_thread::sleep_for(50ms);
                continue;
            }

            while (workQueue.size() > 0)
            {
                //do work
                if (!DoWork(&workQueue, &workOutQueue))
                {
                    m_stop.request_stop();
                }

                if (workOutQueueSize < workOutQueue.size())
                    outQueue->push_queue(&workOutQueue);
            }
        }

        if (workOutQueue.size() > 0)
            outQueue->push_queue(&workOutQueue);
    }
};