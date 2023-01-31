#pragma once

#include "PipelineThread.h"
#include "LockableQueue.h"
#include "MinHasherThread.h"
#include "Hashing.h"



#define IN_TYPE HasherThreadOutputData<UINT_HASH_TYPE>* 


template <typename UINT_HASH_TYPE>
struct SetCompare{
    bool operator() (const IN_TYPE a, const IN_TYPE b) const {
        return a->arrowData->rowNumber < b->arrowData->rowNumber;
    }
};

template <typename UINT_HASH_TYPE>
struct CoalescedItems
{
    uint32_t fileId;
    std::set< IN_TYPE, SetCompare<UINT_HASH_TYPE> >* itemSet;

    CoalescedItems(uint32_t _fileId, std::set< IN_TYPE, SetCompare<UINT_HASH_TYPE> >* set)
        :fileId(_fileId),
        itemSet(std::move(set))
    {}

    ~CoalescedItems()
    {
        for (auto item : itemSet)
        {
            delete item;
            item = nullptr;
        }

        delete itemSet;
        itemSet = nullptr;
    }
};


#define OUT_TYPE CoalescedItems<UINT_HASH_TYPE>*


template<typename UINT_HASH_TYPE>
class CoalesceByFileThread : public PipelineThread<IN_TYPE, OUT_TYPE >
{
    //for storing duplicate items by file id
    std::map<uint32_t, std::set< IN_TYPE , SetCompare<UINT_HASH_TYPE> >* > itemToFile;

    std::vector<FileInfo*>* fileInfos;

public:
    CoalesceByFileThread(BS::thread_pool* _threadPool, LockableQueue< IN_TYPE >* _inQueue, uint32_t _workChunkSize, std::vector<FileInfo*>* _fileInfos)
        :PipelineThread<IN_TYPE, OUT_TYPE >(_threadPool, _inQueue, nullptr, _workChunkSize),
        fileInfos(_fileInfos)
    {
    }

    IN_TYPE workItem;
    bool DoWork(std::queue< IN_TYPE >* workQueue, std::queue< OUT_TYPE >* workOutQueue) final
    {
        workItem = workQueue->front();
        workQueue->pop();

        //we cant do anything until the arrowStreamer has completed streaming in files...
        //lets jsut start stuffing them into the map as they filter in
        auto docIdList = itemToFile.find(workItem->arrowData->docId);
        if (docIdList == itemToFile.end())
        {
            auto insertPair = itemToFile.insert(std::pair{ workItem->arrowData->docId, new std::set<IN_TYPE , SetCompare<UINT_HASH_TYPE> >({ workItem }) });
            docIdList = insertPair.first;
        }
        else
        {
            docIdList->second->insert(workItem);
        }

        //check if file is finished getting items..if its done, push to outqueue, so an iothread can write it out
        FileInfo* fi = (*fileInfos)[workItem->arrowData->docId];
        if (fi->finishedLoad.load() == true)
        {
            if (fi->numRowsLoaded.load() == docIdList->second->size())
            {
                //done!
                workOutQueue->push(std::move(new CoalescedItems<UINT_HASH_TYPE>(workItem->arrowData->docId, std::move(docIdList->second))) );
            }
        }

        return true;
    }
};
