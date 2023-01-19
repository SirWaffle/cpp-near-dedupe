// CPPDeduper.cpp : Defines the entry point for the application.
//

#include <filesystem>
#include <chrono>
#include <thread>
#include <queue>
#include <iostream>

#include "DupeResolverThread.h"
#include "ComparerThread.h"
#include "HasherThread.h"
#include "ArrowLoaderThread.h"
#include "LockableQueue.h"
#include "ThreadPool.h"
#include "CLI11.hpp"

/*
* TODO:
* - command line params (more, better, etc)
* - fix all the really slow stuff in duperesolver, easy perf gains
*/

static constexpr int HASH_LENGTH_SHINGLES = 5; //words used per hash
static constexpr int NUM_HASHES = 256; //number of hashes for comparison



template<typename HASH_TYPE>
int Run(std::string inputPath, std::string outPath, std::string dataColumnName, std::string extension,
    double matchThresh, int numHasherThreads, int maxRecordsLoaded, int hashSize);

//==========
// main
//==========
int main(int argc, const char** argv)
{
    //lets try CLI11
    CLI::App app("SquishBrains CPPDeduper");
    app.set_version_flag("--version", "pre-early-alpha-divided-by-zero");


    //required params
    std::string inputPath;
    CLI::Option* opt = app.add_option("-i,--input", inputPath, "Input Folder")->required();

    std::string outPath;
    opt = app.add_option("-o,--output", outPath, "Output Folder")->required();

    std::string dataColumnName;
    opt = app.add_option("-d,--columnName", dataColumnName, "data column name")->required();


    //optional
    std::string extension = ".arrow";
    opt = app.add_option("-e,--ext", extension, "arrow IPC extension");

    double matchThresh = 0.7;
    opt = app.add_option("-j,--jaccardSim", matchThresh, "min jaccard similarity value ( 0.0 to 1.0 )");

    //thread counts
    int numHasherThreads = 6; // 4; //more threads crunch through mroe input faster
    opt = app.add_option("-t,--hashThreads", numHasherThreads, "threads dedicated to hashing");

    int maxRecordsLoaded = 4096 * 16; //the higher this is, the higher memory usage can get
    opt = app.add_option("-r,--maxRecords", maxRecordsLoaded, "max arrow rows to load at once");

    int hashSize = 64; //the higher this is, the higher memory usage can get
    opt = app.add_option("-s,--hashSize", hashSize, "hash size in bits, valid values ( 32 or 64 )");

    try {
        app.parse(argc, argv);
    }
    catch (const CLI::ParseError& e) {
        std::cout << app.help("", CLI::AppFormatMode::All);
        return app.exit(e);
    }

    if (hashSize == 32)
    {
        return Run<uint32_t>(inputPath, outPath, dataColumnName, extension,
            matchThresh, numHasherThreads, maxRecordsLoaded, hashSize);
    }
    else
    {
        return Run<uint64_t>(inputPath, outPath, dataColumnName, extension,
            matchThresh, numHasherThreads, maxRecordsLoaded, hashSize);
    }
}

template<typename HASH_TYPE>
int Run(std::string inputPath, std::string outPath, std::string dataColumnName, std::string extension,
    double matchThresh, int numHasherThreads, int maxRecordsLoaded, int hashSize)
{
    std::cout << "Running with all options:" << std::endl;

    //TODO: pass in arguments, for whatever params we want to control
    std::filesystem::path basePath = inputPath;


    std::cout << "Running with params: " << basePath << ", " << extension << ", " << dataColumnName << ", " << matchThresh << ", " << outPath << std::endl;
    std::cout << "Hash size of: " << hashSize << std::endl;
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
    uint32_t baseThreads = 3 + numHasherThreads;
    BS::thread_pool threadPool(baseThreads, 0);
    uint32_t numThreads = threadPool.get_thread_count();

    //reader thread
    ArrowLoaderThread* arrowLoaderThread = new ArrowLoaderThread(maxRecordsLoaded);
    std::future<void> arrowLoaderThreadFuture = threadPool.submit(&ArrowLoaderThread::EnterProcFunc, arrowLoaderThread, fileNamesVector, dataColumnName);

    //hasher threads
    LockableQueue< HasherThreadOutputData<HASH_TYPE>* > hashedDataQueue;
    std::vector< HasherThread<HASH_LENGTH_SHINGLES, NUM_HASHES, HASH_TYPE>* > hasherThreads;
    BS::multi_future<void> hasherThreadFutures;

    for (int i = 0; i < numHasherThreads; ++i)
    {
        auto hasherThread = new HasherThread<HASH_LENGTH_SHINGLES, NUM_HASHES, HASH_TYPE>(&hashedDataQueue, 2048);
        hasherThreads.push_back(hasherThread);

        hasherThreadFutures.push_back(threadPool.submit(&HasherThread<HASH_LENGTH_SHINGLES, NUM_HASHES, HASH_TYPE>::EnterProcFunc, hasherThread, arrowLoaderThread->GetOutputQueuePtr()));
    }

    //comparer
    ComparerThread<HASH_TYPE, NUM_HASHES>* comparerThread = new ComparerThread<HASH_TYPE, NUM_HASHES>(true, 4096, &threadPool, std::max(1U, numThreads - baseThreads));

    //for binary 'dupe or not', we dont need to score, so use the threshval for early out as well
    //hashers have a static queue, one shared across them all
    std::future<void> comparerThreadFuture = threadPool.submit(&ComparerThread<HASH_TYPE, NUM_HASHES>::EnterProcFunc,
        comparerThread, &hashedDataQueue, /*JACCARD_EARLY_OUT*/ matchThresh, matchThresh);

    //and as the comparer spits out the dupes, we can start removing them from the datasets...
    DupeResolverThread* dupeResolverThread = new DupeResolverThread(basePath, outPath, 4096, false);
    std::future<void> dupeResolverThreadFuture = threadPool.submit(&DupeResolverThread::EnterProcFunc, dupeResolverThread, comparerThread->GetOutputQueuePtr(), &fileNamesVector);
    

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
                for (int i = 0; i < numHasherThreads; ++i)
                    hasherThreads[i]->WaitForFinish();
            }
        }
        else if (state == 1)
        {
            if (hasherThreadFutures.wait_for(0ms) == std::future_status::ready)
            {
                for (int i = 0; i < numHasherThreads; ++i)
                    delete hasherThreads[i];

                comparerThread->IncreaseMaxWorkerThreads(numHasherThreads);
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

        if (state < 2)
            std::cout << "   [" << numHasherThreads << "] Pending Hash..." << arrowLoaderThread->GetOutputQueuePtr()->Length();
        else
            std::cout << "   [0]hashing Done!";

        std::cout << "   [" << jaccardThreads << "] Pending Jaccard..." << hashedDataQueue.Length();
        std::cout << "   Unique Docs: " << comparerThread->GetUniqueItemsCount();
        std::cout << "   Dupe Docs: " << dupeResolverThread->PendingDuplicates();
        std::cout << std::endl;
    }

    auto stopTime = std::chrono::high_resolution_clock::now();

    //aaaand done
    auto duration = duration_cast<std::chrono::seconds>(stopTime - startTime);
    std::cout << "Finished in " << (uint64_t)(duration.count()) << "s" << std::endl;
    std::cout << "Found " << comparerThread->GetUniqueItemsCount() << " Unique documents" << std::endl;
    std::cout << "Found " << dupeResolverThread->TotalDupes() << " duplicates, and removed " << dupeResolverThread->TotalDupesRemoved() << std::endl;
    std::cout << "Total batches: " << arrowLoaderThread->GetTotalBatches() << " and total docs: " << arrowLoaderThread->GetTotalDocs() << std::endl;


    //clean up...
    threadPool.reset(0);
    delete arrowLoaderThread;
    delete comparerThread;
    delete dupeResolverThread; 

    return 0;
}
