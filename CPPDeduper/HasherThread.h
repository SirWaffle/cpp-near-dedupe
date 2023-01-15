#pragma once

#include "LockableQueue.h"
#include "ArrowLoaderThread.h"
#include "Hashing.h"

struct HasherThreadOutputData
{
    uint32_t docId;
    int64_t stringArrayInd;
    uint32_t batchNum;
    std::unique_ptr<uint32_t[]> hashes;
    uint32_t hashLen;
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
                if (batchQueueIn->try_pop_range(&workQueue, chunkSize, 1ms) == 0)
                {
                    std::this_thread::sleep_for(100ms);
                    continue;
                }

                while (workQueue.size() > 0)
                {
                    //loop over the batchQueueIn, grab some items, hash em, stuff em in the hashedDataQueue
                    workItem = workQueue.front();
                    workQueue.pop();

                    std::shared_ptr<arrow::Array> array = workItem->recordbatch->GetColumnByName(dataColumnName);

                    arrow::StringArray stringArray(array->data());
                   
                    for (int64_t i = 0; i < stringArray.length(); ++i)
                    {
                        std::string_view view = stringArray.GetView(i);

                        //hash...
                        U16String u16str;

                        CharPtrToUStr(view.data(), view.size(), u16str);

                        std::unique_ptr<uint32_t[]> hashes;
                        uint32_t hashLen = MakeFingerprint<HASH_LEN_SHINGLES, NUM_HASHES>(u16str, &hashes);

                        //push into hashedQueue
                        HasherThreadOutputData* hashed = new HasherThreadOutputData();
                        hashed->docId = workItem->docId;
                        hashed->stringArrayInd = i;
                        hashed->batchNum = workItem->batchNum;
                        hashed->hashes = std::move(hashes);
                        hashed->hashLen = hashLen;

                        hashedDataQueue->push(std::move(hashed));
                    }

                    delete workItem;
                }
            }
        }
};
