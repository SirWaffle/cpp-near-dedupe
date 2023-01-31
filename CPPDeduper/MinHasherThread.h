#pragma once

#include "PipelineThread.h"
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



#define IN_TYPE ArrowLoaderThreadOutputData* 
#define OUT_TYPE HasherThreadOutputData<UINT_HASH_TYPE>* 


template<int HASH_LEN_SHINGLES, int NUM_HASHES, typename UINT_HASH_TYPE>
class MinHasherThread : public PipelineThread<IN_TYPE, OUT_TYPE >
{
public:
    MinHasherThread(BS::thread_pool* _threadPool, LockableQueue< IN_TYPE >* _inQueue, LockableQueue< OUT_TYPE >* _outQueue, uint32_t _workChunkSize)
        :PipelineThread<IN_TYPE, OUT_TYPE >(_threadPool, _inQueue, _outQueue, _workChunkSize)
    {
    }

    ArrowLoaderThreadOutputData* workItem;
    bool DoWork(std::queue< IN_TYPE >* workQueue, std::queue< OUT_TYPE >* workOutQueue) final
    {
        //loop over the batchQueueIn, grab some items, hash em, stuff em in the hashedDataQueue
        workItem = workQueue->front();
        workQueue->pop();

        std::unique_ptr<UINT_HASH_TYPE[]> hashes;
        uint32_t hashLen = MakeFingerprint<HASH_LEN_SHINGLES, NUM_HASHES>(*(workItem->data), &hashes);
        workItem->DeleteData();

        //push into hashedQueue
        HasherThreadOutputData< UINT_HASH_TYPE>* hashed = new HasherThreadOutputData< UINT_HASH_TYPE>(workItem, std::move(hashes), hashLen);
        hashed->arrowData = std::move(workItem);

        workOutQueue->push(std::move(hashed));

        return true;
    }
};
