#pragma once

#include "LongRunningWorkerThread.h"
#include "MinHasherThread.h"
#include "Hashing.h"
#include "LockableQueue.h"
#include "Jaccard.h"
#include <list>
#include <thread>

#include "ThreadPool.h"
#include "CompareStrategies.h"

#include <immintrin.h>
#include <nmmintrin.h>
#include <emmintrin.h>



#define LOAD_TEST false

#define ALREADY_PROCESSED_CHECK true
#define USE_INTRIN true



struct CompareThreadDupeItem
{
    CompareThreadDupeItem(uint32_t _docId, int64_t _rowNumber)
        :docId(_docId),
        rowNumber(_rowNumber)
    {}

    uint32_t docId;
    int64_t rowNumber;
};


template<typename UINT_HASH_TYPE>
struct CompareItem
{
    CompareItem(HasherThreadOutputData< UINT_HASH_TYPE>* _myHashData)
        :myHashData(_myHashData)
    {}

    ~CompareItem()
    {
        delete myHashData;
    }

    HasherThreadOutputData< UINT_HASH_TYPE>* myHashData;
};

/*
 if ((it).second >= (uint32_t)(float)bands * 0.70)
 */

 //flat container
template<typename UINT_HASH_TYPE, uint32_t MAX_HASH_LEN, uint32_t BLOCK_SIZE>
bool CompareThreadWorkerFuncFlatMap(
    std::stop_source workerThreadStopper,
    auto startIt,
    auto endIt,
    uint32_t iterIncrementAmount,
    double earlyOut, double dupeThreash, CompareItem<UINT_HASH_TYPE>* citem, uint32_t bands)
{

    uint32_t mapCountRequired = (uint32_t)((float)bands * dupeThreash);

    //compare incoming against all others, update the its max value.
    //this will prioritize removing later documents that match, not the first one
#if USE_INTRIN
    __m128i itemflags = _mm_set_epi64x(citem->myHashData->arrowData->docId, citem->myHashData->arrowData->rowNumber);
#endif
    auto dbgIt = startIt;
    for (; startIt != endIt && !workerThreadStopper.stop_requested();)
    {
        auto hashes = startIt->first;

        if (startIt->second < mapCountRequired)
        {
            //increment the start iterator by provided amount
            for (uint32_t i = 0; i < iterIncrementAmount && startIt != endIt; ++i, ++startIt)
            {
            }
            continue;
        }

#if USE_INTRIN 
        //used to not check the same document for the same item across threads, since docs can be in multiple buckets
        __m128i flags = _mm_loadu_si128((__m128i*) (&hashes->flags[0]));
        __m128i cmp = _mm_cmpeq_epi64(itemflags, flags);

        if (_mm_extract_epi64(cmp, 0) != 0 && _mm_extract_epi64(cmp, 1) != 0)
        {
            //increment the start iterator by provided amount
            for (uint32_t i = 0; i < iterIncrementAmount && startIt != endIt; ++i, ++startIt)
            {
            }
            continue;
        }
        else
        {
            _mm_storeu_si128((__m128i*) (&hashes->flags[0]), itemflags);
        }
#else
        if (hashes.flags[0] == citem->myHashData->arrowData->docId
            && hashes.flags[1] == citem->myHashData->arrowData->rowNumber)
        {
            continue;
        }

        hashes.flags[0] = citem->myHashData->arrowData->docId;
        hashes.flags[1] = citem->myHashData->arrowData->rowNumber;
#endif


        double match = JaccardTurbo2(citem->myHashData->hashes.get(), (int)citem->myHashData->hashLen,
            &hashes->hashes[0], (int)hashes->hashLen, earlyOut);

        if (match > dupeThreash)
        {
            //we are done
            workerThreadStopper.request_stop();
            return true;
        }

        //increment the start iterator by provided amount
        for (uint32_t i = 0; i < iterIncrementAmount && startIt != endIt; ++i, ++startIt)
        {
        }
        continue;
    }

    return false;
}


//flat container
template<typename UINT_HASH_TYPE, uint32_t MAX_HASH_LEN, uint32_t BLOCK_SIZE>
bool CompareThreadWorkerFunc(
    std::stop_source workerThreadStopper,
    auto startIt,
    auto endIt,
    uint32_t iterIncrementAmount,
    double earlyOut, double dupeThreash, CompareItem<UINT_HASH_TYPE>* citem)
{
    //compare incoming against all others, update the its max value.
    //this will prioritize removing later documents that match, not the first one
#if USE_INTRIN
    __m128i itemflags = _mm_set_epi64x(citem->myHashData->arrowData->docId, citem->myHashData->arrowData->rowNumber);
#endif
    auto dbgIt = startIt;
    for (; startIt != endIt && !workerThreadStopper.stop_requested();)
    {
        auto hashes = *startIt;

#if USE_INTRIN 
        //used to not check the same document for the same item across threads, since docs can be in multiple buckets
        __m128i flags = _mm_loadu_si128((__m128i*) (&hashes->flags[0]));
        __m128i cmp = _mm_cmpeq_epi64(itemflags, flags);

        if (_mm_extract_epi64(cmp, 0) != 0 && _mm_extract_epi64(cmp, 1) != 0)
        {
            //increment the start iterator by provided amount
            for (uint32_t i = 0; i < iterIncrementAmount && startIt != endIt; ++i, ++startIt)
            {
            }
            continue;
        }
        else
        {
            _mm_storeu_si128((__m128i*) (&hashes->flags[0]), itemflags);
        }
#else
        if (hashes.flags[0] == citem->myHashData->arrowData->docId
            && hashes.flags[1] == citem->myHashData->arrowData->rowNumber)
        {
            continue;
        }

        hashes.flags[0] = citem->myHashData->arrowData->docId;
        hashes.flags[1] = citem->myHashData->arrowData->rowNumber;
#endif


        double match = JaccardTurbo2(citem->myHashData->hashes.get(), (int)citem->myHashData->hashLen,
            &hashes->hashes[0], (int)hashes->hashLen, earlyOut);

        if (match > dupeThreash)
        {
            //we are done
            workerThreadStopper.request_stop();
            return true;
        }

        //increment the start iterator by provided amount
        for (uint32_t i = 0; i < iterIncrementAmount && startIt != endIt; ++i, ++startIt)
        {
        }
    }

    return false;
}

//container of containers
template<typename UINT_HASH_TYPE, uint32_t MAX_HASH_LEN, uint32_t BLOCK_SIZE>
bool CompareThreadWorkerFunc(std::stop_source workerThreadStopper,
    auto startIt,
    auto endIt,
    double earlyOut, double dupeThreash, CompareItem<UINT_HASH_TYPE>* citem)
{
    bool matched = false;
    for (; startIt != endIt && matched == false && !workerThreadStopper.stop_requested(); ++startIt)
    {
        auto listStartIt = (*startIt)->begin();
        auto listEndIt = (*startIt)->end();
        matched = CompareThreadWorkerFunc<UINT_HASH_TYPE, MAX_HASH_LEN, BLOCK_SIZE>(
            workerThreadStopper, listStartIt, listEndIt, 1, earlyOut, dupeThreash, citem);
    }

    return matched;
}




class IComparerThread
{
public:
    virtual ~IComparerThread() {}

    virtual void IncreaseMaxWorkerThreads(int amt) = 0;
    virtual uint32_t GetWorkerThreadCount() = 0;
    virtual size_t GetUniqueItemsCount() = 0;
    virtual uint64_t GetNumLSHEntries() = 0;
    virtual uint64_t GetEstimatedLSHMemoryUsageMB() = 0;
    virtual uint64_t GetMemUsageMB() = 0;
    virtual uint64_t GetComparedItems() = 0;
};




#define IN_TYPE HasherThreadOutputData<UINT_HASH_TYPE>* 
#define OUT_TYPE CompareThreadDupeItem*



template<typename UINT_HASH_TYPE, uint32_t MAX_HASH_LEN, uint32_t BLOCK_SIZE >
class ComparerThread: public IComparerThread, public LongRunningWorkerThread<IN_TYPE, OUT_TYPE >
{
protected:
    bool m_throwOutDupes;
    std::atomic<uint32_t> maxThreadWorkers;
    uint64_t comparedItems;

    ICompareStrat<UINT_HASH_TYPE, MAX_HASH_LEN, BLOCK_SIZE>* compareStrat;

    double earlyOut;
    double dupeThreash;

public:
    ComparerThread(BS::thread_pool* _threadPool, LockableQueue< IN_TYPE >* _inQueue, LockableQueue< OUT_TYPE >* _outQueue, uint32_t _workChunkSize
        , ICompareStrat<UINT_HASH_TYPE, MAX_HASH_LEN, BLOCK_SIZE>* strat, double _earlyOut, double _dupeThreash, uint32_t maxThreadWorkers = 0)
        : LongRunningWorkerThread<IN_TYPE, OUT_TYPE >(_threadPool, _inQueue, _outQueue, _workChunkSize),
        m_throwOutDupes(true),
        maxThreadWorkers(maxThreadWorkers),
        comparedItems(0),
        compareStrat(strat),
        earlyOut(_earlyOut),
        dupeThreash(_dupeThreash)
    {
        compareStrat->Init();
    }

    ~ComparerThread() final
    {
    }

    void IncreaseMaxWorkerThreads(int amt) final
    {
        maxThreadWorkers.fetch_add(amt);
    }

    uint32_t GetWorkerThreadCount() final
    {
        return maxThreadWorkers.load();
    }

    size_t GetUniqueItemsCount() final
    {
        return compareStrat->GetUniqueItemsCount();
    }

    uint64_t GetNumLSHEntries() final
    {
        return compareStrat->GetNumEntries();
    }

    uint64_t GetEstimatedLSHMemoryUsageMB() final
    {
        return compareStrat->GetEstimatedLSHMemoryUsageBytes() / (1024ULL * 1024ULL);
    }

    uint64_t GetMemUsageMB() final
    {
        return compareStrat->GetMemUsageBytes() / (1024ULL * 1024ULL);
    }


    uint64_t GetComparedItems() final
    {
        return comparedItems;
    }


    HasherThreadOutputData< UINT_HASH_TYPE>* workItem;
    virtual bool DoWork(std::queue< IN_TYPE >* workQueue, std::queue< OUT_TYPE >* workOutQueue)
    {
        ++comparedItems;

        workItem = workQueue->front();
        workQueue->pop();

        CompareItem< UINT_HASH_TYPE>* citem = new CompareItem< UINT_HASH_TYPE>(std::move(workItem));

        compareStrat->CheckAgainstHashes(citem);


        bool isDupe = false;
        if (compareStrat->HasPotentialMatches() == true) [[likely]]
        {
            isDupe = compareStrat->CheckFormatches(citem, earlyOut, dupeThreash);
        }

        if (isDupe)
        {
            //for processing in the removal of dupes
            CompareThreadDupeItem* dupeItem = new CompareThreadDupeItem(citem->myHashData->arrowData->docId, citem->myHashData->arrowData->rowNumber);
            workOutQueue->push(std::move(dupeItem));
        }
        else
        {
            //for testing, add a lot more
                    
#if LOAD_TEST
            static bool add = true;
            if (add == true)
            {
                add = false;
                for (int i = 0; i < 5000000; ++i)
                {
                    auto entry = hashblocks.AddItem(citem->myHashData->hashes.get(), citem->myHashData->hashLen);
                    compareStrat.AddToLSH(bandHashes.begin(), entry);
                }
            }
#endif            
            compareStrat->AddToLSH(citem);
        }

        delete citem;
        citem = nullptr;

        return true;

    }//thread func
};