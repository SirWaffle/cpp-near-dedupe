// CPPDeduper.cpp : Defines the entry point for the application.
//

#include <filesystem>
#include <chrono>
#include <thread>
#include <queue>
#include <condition_variable>
#include <iostream>

#include "DupeResolverThread.h"
#include "ComparerThread.h"
#include "HasherThread.h"
#include "ArrowLoaderThread.h"
#include "LockableQueue.h"
#include "ThreadPool.h"


/*
* TODO:
* - command line params (more, better, etc)
* - fix all the really slow stuff in duperesolver, easy perf gains
*/

static constexpr int HASH_LENGTH_SHINGLES = 5; //words used per hash
static constexpr int NUM_HASHES = 256; //number of hashes for comparison
#define HASH_TYPE uint64_t

static constexpr int MAX_RECORDS_LOADED = 4096 * 32; //the higher this is, the higher memory usage can get

//thread counts
static constexpr int NUM_HASHER_THREADS = 4; // 4; //more threads crunch through mroe input faster


//==========
// main
//==========
int main(int argc, const char** argv)
{
    if (argc < 6)
    {
        std::cout << "usage: (this.exe) \"path\" \".ext\" \"dataColumn\" dupeThreshold \"outdir\"" << std::endl;
        std::cout << "example: (this.exe) \"c:\\baseDirWithLotsOfArrowInSubdirs\" \".arrow\" \"text\" 0.7 \"c:\\outDir\"" << std::endl;
        return 1;
    }

    //TODO: pass in arguments, for whatever params we want to control
    std::filesystem::path basePath = argv[1];
    std::string extension = argv[2];
    std::string dataColumnName = argv[3];

    char* p;
    double matchThresh = strtod(argv[4], &p);

    std::string output = argv[5];


    std::cout << "Running with params: " << basePath << ", " << extension << ", " << dataColumnName << ", " << matchThresh << ", " << output << std::endl;
    std::cout << "Scanning for " << extension << " files in " << basePath;

    //quick and sloppy lookups of filenames, so we dont have to store in each unit of data
    //saves # of loaded docs * string length of filepaths woth of memory
    static std::vector<std::string> fileNamesVector;

    for (auto itEntry = std::filesystem::recursive_directory_iterator(basePath);
        itEntry != std::filesystem::recursive_directory_iterator();
        ++itEntry)
    {
        const auto filenameStr = itEntry->path().filename().string();
        std::string ext = std::filesystem::path(filenameStr).extension().string();

        if (ext == extension)
        {
            std::cout << "adding found file:  " << itEntry->path().string() << '\n';
            fileNamesVector.push_back(itEntry->path().string());
        }
    }

    if (fileNamesVector.size() == 0)
    {
        std::cout << "Failed to locate any datasets to parse, exiting";
        return 1;
    }
    else
    {
        std::cout << "Found " + fileNamesVector.size() << " files to parse...";
    }


    //track how long we take...
    auto startTime = std::chrono::high_resolution_clock::now();

    //======
    //start threads
    //======

    //minimum threads...
    //1 loader thread, x hasher threads, 1 comparer thread, 1 output thread
    //the rest will operate as comparer workers
    uint32_t baseThreads = 3 + NUM_HASHER_THREADS;
    BS::thread_pool threadPool(baseThreads, 0);
    uint32_t numThreads = threadPool.get_thread_count();

    //reader thread
    ArrowLoaderThread* arrowLoaderThread = new ArrowLoaderThread(MAX_RECORDS_LOADED);
    std::future<void> arrowLoaderThreadFuture = threadPool.submit(&ArrowLoaderThread::EnterProcFunc, arrowLoaderThread, fileNamesVector, dataColumnName);

    //hasher threads
    std::vector< HasherThread<HASH_LENGTH_SHINGLES, NUM_HASHES, HASH_TYPE>* > hasherThreads;
    BS::multi_future<void> hasherThreadFutures;

    for (int i = 0; i < NUM_HASHER_THREADS; ++i)
    {
        auto hasherThread = new HasherThread<HASH_LENGTH_SHINGLES, NUM_HASHES, HASH_TYPE>(2048);
        hasherThreads.push_back(hasherThread);

        hasherThreadFutures.push_back(threadPool.submit(&HasherThread<HASH_LENGTH_SHINGLES, NUM_HASHES, HASH_TYPE>::EnterProcFunc, hasherThread, arrowLoaderThread->GetOutputQueuePtr()));
    }

    //comparer
    ComparerThread<HASH_TYPE>* comparerThread = new ComparerThread<HASH_TYPE>(true, 4096, &threadPool, std::max(1U, numThreads - baseThreads));

    //for binary 'dupe or not', we dont need to score, so use the threshval for early out as well
    //hashers have a static queue, one shared across them all
    std::future<void> comparerThreadFuture = threadPool.submit(&ComparerThread<HASH_TYPE>::EnterProcFunc, 
        comparerThread, hasherThreads[0]->GetOutputQueuePtr(), /*JACCARD_EARLY_OUT*/ matchThresh, matchThresh);

    //and as the comparer spits out the dupes, we can start removing them from the datasets...
    DupeResolverThread* dupeResolverThread = new DupeResolverThread(basePath, output, 4096, false);
    std::future<void> dupeResolverThreadFuture = threadPool.submit(&DupeResolverThread::EnterProcFunc, dupeResolverThread, &duplicates, &fileNamesVector);
    

    //wait for tasks to complete
    //guly state thing until i sort out a better wau
    int state = 0;
    uint32_t jaccardThreads = comparerThread->GetWorkerThreadCount();
    while (true)
    {     
        std::this_thread::sleep_for(10s);

        if (state == 0)
        {
            //reader thread is first to finish in this pipeline
            if (arrowLoaderThreadFuture.wait_for(0ms) == std::future_status::ready)
            {
                comparerThread->IncreaseMaxWorkerThreads(1);
                jaccardThreads = comparerThread->GetWorkerThreadCount();

                state++;
                //hashers are next to finish
                //tell them all their parent task has finished
                for (int i = 0; i < NUM_HASHER_THREADS; ++i)
                    hasherThreads[i]->WaitForFinish();
            }
        }
        else if (state == 1)
        {
            if (hasherThreadFutures.wait_for(0ms) == std::future_status::ready)
            {
                for (int i = 0; i < NUM_HASHER_THREADS; ++i)
                    delete hasherThreads[i];

                comparerThread->IncreaseMaxWorkerThreads(NUM_HASHER_THREADS);
                jaccardThreads = comparerThread->GetWorkerThreadCount();

                state++;
                //comparer now, tell it to finish...
                comparerThread->WaitForFinish();
            }
        }
        else if (state == 2)
        {
            if (comparerThreadFuture.wait_for(0ms) == std::future_status::ready)
            {
                state++;
                //and the final peice, writting out the new datasets
                dupeResolverThread->WaitForFinish();
            }
        }
        else if (state == 3)
        {
            if (dupeResolverThreadFuture.wait_for(0ms) == std::future_status::ready)
                break; //done!
        }

        auto curTime = std::chrono::high_resolution_clock::now();
        auto duration = duration_cast<std::chrono::seconds>(curTime - startTime);
        std::cout << "[ " << duration.count() << "s ] Stats:";

        if(state < 1)
            std::cout << "   [1]Docs Loading..." << arrowLoaderThread->GetTotalDocs();
        else
            std::cout << "   [0]Docs Loaded: " << arrowLoaderThread->GetTotalDocs();

        if(state < 2)
            std::cout << "   [" << NUM_HASHER_THREADS << "] Pending Hash..." << batchQueue.Length();
        else
            std::cout << "   [0]hashing Done!";

        std::cout << "   [" << jaccardThreads << "] Pending Jaccard..." << hashedDataQueue.Length();
        std::cout << "   Unique Docs: " << allComparedItems.size();
        std::cout << "   Dupe Docs: " << dupeResolverThread->PendingDuplicates();
        std::cout << std::endl;
    }

    auto stopTime = std::chrono::high_resolution_clock::now();

    //aaaand done
    auto duration = duration_cast<std::chrono::seconds>(stopTime - startTime);
    std::cout << "Finished in " << (uint64_t)(duration.count()) << "s" << std::endl;
    std::cout << "Found " << allComparedItems.size() << " Unique documents" << std::endl;
    std::cout << "Found " << dupeResolverThread->TotalDupes() << " duplicates, and removed " << dupeResolverThread->TotalDupesRemoved() << std::endl;
    std::cout << "Total batches: " << arrowLoaderThread->GetTotalBatches() << " and total docs: " << arrowLoaderThread->GetTotalDocs() << std::endl;


    //clean up...
    threadPool.reset(0);
    delete arrowLoaderThread;
    delete comparerThread;
    delete dupeResolverThread;
    while (allComparedItems.size() > 0)
    {
        delete allComparedItems.front();
        allComparedItems.pop_front();
    }
   
}
