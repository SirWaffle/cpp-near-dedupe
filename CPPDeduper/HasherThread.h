#pragma once

#include "LockableQueue.h"
#include "ArrowLoaderThread.h"
#include "Hashing.h"

struct HasherThreadOutputData
{
    HasherThreadOutputData(ArrowLoaderThreadOutputData* _arrowLoaderData, std::unique_ptr<uint32_t[]> _hashes, uint32_t _hashLen)
        :arrowData(_arrowLoaderData),
        hashes(std::move(_hashes)),
        hashLen(_hashLen)

    {}

    ArrowLoaderThreadOutputData* arrowData;
    std::unique_ptr<uint32_t[]> hashes;
    uint32_t hashLen;
};


template<int HASH_LEN_SHINGLES, int NUM_HASHES>
class HasherThread
{
protected:
    std::stop_source m_stop;
    uint32_t readChunkSize;

public:
    HasherThread(uint32_t _readChunkSize)
        :readChunkSize(_readChunkSize)
    {
    }

    void WaitForFinish()
    {
        m_stop.request_stop();
    }

    void EnterProcFunc(LockableQueue< ArrowLoaderThreadOutputData* >* batchQueueIn, LockableQueue< HasherThreadOutputData* >* hashedDataQueue)
    {
        std::queue<ArrowLoaderThreadOutputData* > workQueue;
        ArrowLoaderThreadOutputData* workItem;

        while (!m_stop.stop_requested() || batchQueueIn->Length() > 0)
        {
            if (batchQueueIn->try_pop_range(&workQueue, readChunkSize, 10ms) == 0)
            {
                std::this_thread::sleep_for(100ms);
                continue;
            }

            while (workQueue.size() > 0)
            {
                //loop over the batchQueueIn, grab some items, hash em, stuff em in the hashedDataQueue
                workItem = workQueue.front();
                workQueue.pop();

                std::unique_ptr<uint32_t[]> hashes;
                uint32_t hashLen = MakeFingerprint<HASH_LEN_SHINGLES, NUM_HASHES>(*(workItem->data), &hashes);
                delete(workItem->data);
                workItem->data = nullptr;

                //push into hashedQueue
                HasherThreadOutputData* hashed = new HasherThreadOutputData(workItem, std::move(hashes), hashLen);
                hashed->arrowData = workItem;

                hashedDataQueue->push(std::move(hashed));
            }
        }
    }
};
