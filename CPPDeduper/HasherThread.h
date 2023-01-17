#pragma once

#include "LockableQueue.h"
#include "ArrowLoaderThread.h"
#include "Hashing.h"

template<int HASH_LEN_SHINGLES, int NUM_HASHES, typename UINT_HASH_TYPE>
class HasherThread
{
public:
    struct HasherThreadOutputData
    {
        HasherThreadOutputData(ArrowLoaderThreadOutputData* _arrowLoaderData, std::unique_ptr<uint32_t[]> _hashes, uint32_t _hashLen)
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

protected:
    std::stop_source m_stop;
    uint32_t readChunkSize;
    static LockableQueue< HasherThreadOutputData* > hashedDataQueue;

public:
    HasherThread(uint32_t _readChunkSize)
        :readChunkSize(_readChunkSize)
    {
    }

    void WaitForFinish()
    {
        m_stop.request_stop();
    }

    LockableQueue< HasherThreadOutputData* >* GetOutputQueuePtr()
    {
        return &hashedDataQueue;
    }

    void EnterProcFunc(LockableQueue< ArrowLoaderThreadOutputData* >* batchQueueIn)
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

                std::unique_ptr<UINT_HASH_TYPE[]> hashes;
                uint32_t hashLen = MakeFingerprint<HASH_LEN_SHINGLES, NUM_HASHES>(*(workItem->data), &hashes);
                workItem->DeleteData();

                //push into hashedQueue
                HasherThreadOutputData* hashed = new HasherThreadOutputData(workItem, std::move(hashes), hashLen);
                hashed->arrowData = std::move(workItem);

                hashedDataQueue->push(std::move(hashed));
            }
        }
    }
};
