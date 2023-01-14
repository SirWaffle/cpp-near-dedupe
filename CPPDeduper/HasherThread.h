#pragma once

#include "LockableQueue.h"
#include "ArrowLoaderThread.h"
#include "Hashing.h"

struct HasherThreadOutputData
{
    uint32_t docId;
    uint32_t batchNum;
    std::unique_ptr<uint32_t[]> hashes;
};


template<int HASH_LEN_SHINGLES, int NUM_HASHES>
class HasherThread
{
protected:
    std::thread* m_thread = nullptr;
    std::stop_source m_stop;

public:
    HasherThread()
    {
    }

    void Start(LockableQueue< ArrowLoaderThreadOutputData* >* batchQueueIn, LockableQueue< HasherThreadOutputData* >* hashedDataQueue, int chunkSize, std::string dataColumnName)
    {
        m_thread = new std::thread(&HasherThread::EnterProcFunc, this, m_stop, batchQueueIn, hashedDataQueue, chunkSize, dataColumnName);
    }

    void WaitForFinish()
    {
        m_stop.request_stop();
        m_thread->join();
    }

protected:
    void EnterProcFunc(std::stop_source stop, LockableQueue< ArrowLoaderThreadOutputData* >* batchQueueIn,
        LockableQueue< HasherThreadOutputData* >* hashedDataQueue, 
        int chunkSize, std::string dataColumnName)
        {
            std::queue<ArrowLoaderThreadOutputData* > workQueue;
            ArrowLoaderThreadOutputData* workItem;

            while (!stop.stop_requested() || batchQueueIn->Length() > 0)
            {
                std::this_thread::sleep_for(1ms);
                for (int i = 0; i < chunkSize; ++i)
                {
                    if (batchQueueIn->try_pop(&workItem, 1ms))
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
                    std::this_thread::sleep_for(100ms);
                    continue;
                }

                //ConsoleLogDebug(std::format("hashing recordbatch work items length {}, pending items {}\n", workQueue.size(), batchQueueIn->Length()));

                while (workQueue.size() > 0)
                {
                    //loop over the batchQueueIn, grab some items, hash em, stuff em in the hashedDataQueue
                    workItem = workQueue.front();
                    workQueue.pop();

                    //TODO: this grabbing of the specific buffer ind seems suspect. verify
                    std::shared_ptr<arrow::Array> array = workItem->recordbatch->GetColumnByName(dataColumnName);
                    auto& arrowBuffers = (*(*array).data()).buffers;
                    auto& text = arrowBuffers[2];

                    //hash...
                    U16String u16str;
                    ArrowBuffToUStr(text->data(), text->size(), u16str);
                    std::unique_ptr<uint32_t[]> hashes;
                    MakeFingerprint<HASH_LEN_SHINGLES, NUM_HASHES>(u16str, &hashes);

                    //push into hashedQueue
                    HasherThreadOutputData* hashed = new HasherThreadOutputData();
                    hashed->docId = workItem->docId;
                    hashed->batchNum = workItem->batchNum;
                    hashed->hashes = std::move(hashes);
                    hashedDataQueue->push(std::move(hashed));

                    delete workItem;
                }
            }
        }
};