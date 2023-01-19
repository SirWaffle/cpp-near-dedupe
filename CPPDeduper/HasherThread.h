#pragma once

#include "LockableQueue.h"
#include "ArrowLoaderThread.h"
#include "Hashing.h"


template<typename UINT_HASH_TYPE>
struct HasherThreadOutputData
{
    HasherThreadOutputData(ArrowLoaderThreadOutputData* _arrowLoaderData, std::unique_ptr<UINT_HASH_TYPE[]> _hashes, uint32_t _hashLen)
        :arrowData(_arrowLoaderData),
        hashes(std::move(_hashes)),
        hashLen(_hashLen)

    {}

    ~HasherThreadOutputData()
    {
        DeleteArrowData();
    }

    void DeleteArrowData()
    {
        delete arrowData;
        arrowData = nullptr;
    }

    ArrowLoaderThreadOutputData* arrowData;
    std::unique_ptr<UINT_HASH_TYPE[]> hashes;
    uint32_t hashLen;
};

template<int HASH_LEN_SHINGLES, int NUM_HASHES, typename UINT_HASH_TYPE>
class HasherThread
{
protected:
    std::stop_source m_stop;
    uint32_t readChunkSize;
    LockableQueue< HasherThreadOutputData<UINT_HASH_TYPE>* >* hashedDataQueue;
    const uint32_t workOutQueueSize = 1024;

public:
    HasherThread(LockableQueue< HasherThreadOutputData<UINT_HASH_TYPE>* >* hashedDataQueue, uint32_t _readChunkSize)
        :readChunkSize(_readChunkSize),
        hashedDataQueue(hashedDataQueue)
    {
    }

    void WaitForFinish()
    {
        m_stop.request_stop();
    }

    LockableQueue< HasherThreadOutputData<UINT_HASH_TYPE>* >* GetOutputQueuePtr()
    {
        return hashedDataQueue;
    }

    void EnterProcFunc(LockableQueue< ArrowLoaderThreadOutputData* >* batchQueueIn)
    {
        std::queue< HasherThreadOutputData<UINT_HASH_TYPE>* > workOutQueue;
        std::queue<ArrowLoaderThreadOutputData* > workQueue;
        ArrowLoaderThreadOutputData* workItem;

        while (!m_stop.stop_requested() || batchQueueIn->Length() > 0)
        {
            if (batchQueueIn->try_pop_range(&workQueue, readChunkSize, 10000ms) == 0)
            {
                std::this_thread::sleep_for(50ms);
                continue;
            }

            while (workQueue.size() > 0)
            {
                //loop over the batchQueueIn, grab some items, hash em, stuff em in the hashedDataQueue
                workItem = workQueue.front();
                workQueue.pop();

                std::unique_ptr<UINT_HASH_TYPE[]> hashes;
                uint32_t hashLen = MakeFingerprint<HASH_LEN_SHINGLES, NUM_HASHES>(*(workItem->data), &hashes);
                workItem->DeleteData();

                //push into hashedQueue
                HasherThreadOutputData< UINT_HASH_TYPE>* hashed = new HasherThreadOutputData< UINT_HASH_TYPE>(workItem, std::move(hashes), hashLen);
                hashed->arrowData = std::move(workItem);

                workOutQueue.push(std::move(hashed));

                if (workOutQueueSize < workOutQueue.size())
                    hashedDataQueue->push_queue(&workOutQueue);
            }
        }

        if(workOutQueue.size() > 0)
            hashedDataQueue->push_queue(&workOutQueue);
    }
};
