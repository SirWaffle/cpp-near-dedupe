// CPPDeduper.cpp : Defines the entry point for the application.
//

#include <filesystem>
#include <chrono>
#include <thread>
#include <queue>
#include <iostream>

#include "PipelineThread.h"

#include "DupeResolverThread.h"
#include "ComparerThread.h"
#include "ComparerThreadBruteForce.h"
#include "MinHasherThread.h"
#include "ArrowLoaderThread.h"
#include "LockableQueue.h"
#include "ThreadPool.h"
#include "CLI11.hpp"

#include "CoalesceByFileThread.h"
#include "WriteMinhashArrowFileThread.h"



static constexpr int HASH_LENGTH_SHINGLES = 5; //words used per hash
const uint64_t expectedDocs = 6000000000; //TODO: make this a parameter



//forward decl
int ParseArguments(int argc, const char** argv);

int main(int argc, const char** argv)
{
    return ParseArguments(argc, argv);
}



//logic for running a pass of everything

template<typename HASH_TYPE, int HASH_BLOCK_SIZE, int NUM_HASHES>
class Runner {
public:
    Runner()
    {}

    int Run(std::string inputPath, std::string outPath, std::string dataColumnName, std::string extension,
        double matchThresh, int numHasherThreads, int maxRecordsLoaded, int hashSize, bool noFileOut, 
        uint32_t numBands, uint64_t numBuckets, std::string lshMethod)
    {
        std::filesystem::path basePath = inputPath;


        std::cout << "Running with params: " << basePath << ", " << extension << ", " << dataColumnName << ", " << matchThresh << ", " << outPath << std::endl;
        std::cout << "Hash size of: " << hashSize << std::endl;
        std::cout << "Hash block size of: " << HASH_BLOCK_SIZE << std::endl;

        std::cout << "Scanning for " << extension << " files in " << basePath;

        //quick and sloppy lookups of filenames, so we dont have to store in each unit of data
        //saves # of loaded docs * string length of filepaths woth of memory
        static std::vector<FileInfo*> fileNamesVector;

        for (auto itEntry = std::filesystem::recursive_directory_iterator(basePath);
            itEntry != std::filesystem::recursive_directory_iterator();
            ++itEntry)
        {
            const auto filenameStr = itEntry->path().filename().string();
            std::string ext = std::filesystem::path(filenameStr).extension().string();

            if (ext == extension)
            {
                std::cout << "adding found file:  " << itEntry->path().string() << '\n';
                FileInfo* fi = new FileInfo(itEntry->path().string(), (uint32_t)fileNamesVector.size());                
                fileNamesVector.push_back(std::move(fi));
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

        //put files into a lockable queue so we can have multiple fileloaders
        LockableQueue<FileInfo*> inputFileQueue;
        int fileId = 0;
        for (FileInfo* fi : fileNamesVector)
        {
            inputFileQueue.push(std::move(fi));
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
        ArrowLoaderThread* arrowLoaderThread = new ArrowLoaderThread(&threadPool, 1u, (uint32_t)maxRecordsLoaded, &inputFileQueue, dataColumnName);

        std::future<void> arrowLoaderThreadFuture = threadPool.submit(&ArrowLoaderThread::Run, arrowLoaderThread);

        //hasher threads
        LockableQueue< HasherThreadOutputData<HASH_TYPE>* > hashedDataQueue;
        std::vector< MinHasherThread<HASH_LENGTH_SHINGLES, NUM_HASHES, HASH_TYPE>* > hasherThreads;
        BS::multi_future<void> hasherThreadFutures;

        for (int i = 0; i < numHasherThreads; ++i)
        {
            auto hasherThread = new MinHasherThread<HASH_LENGTH_SHINGLES, NUM_HASHES, HASH_TYPE>(&threadPool, arrowLoaderThread->GetOutputQueuePtr(), &hashedDataQueue, 1024);
            hasherThreads.push_back(hasherThread);

            hasherThreadFutures.push_back(threadPool.submit(&MinHasherThread<HASH_LENGTH_SHINGLES, NUM_HASHES, HASH_TYPE>::Run, hasherThread));
        }



       
        ICompareStrat<HASH_TYPE, NUM_HASHES, HASH_BLOCK_SIZE>* compareStrategy;

//#define COMPARE_STRAT AllBucketsCompareStrat
#define COMPARE_STRAT SetReduceCompareStrat
        if (lshMethod == "rbs32")
        {
            compareStrategy = new COMPARE_STRAT<HASH_TYPE, NUM_HASHES, uint32_t, HASH_BLOCK_SIZE>(&threadPool, numBands, numBuckets, LSHBandHashMap<HASH_TYPE, uint32_t, NUM_HASHES>::RANDOM_BIT, expectedDocs);
        }
        else if (lshMethod == "rbs64")
        {
            compareStrategy = new COMPARE_STRAT<HASH_TYPE, NUM_HASHES, uint64_t, HASH_BLOCK_SIZE>(&threadPool, numBands, numBuckets, LSHBandHashMap<HASH_TYPE, uint64_t, NUM_HASHES>::RANDOM_BIT, expectedDocs);
        }
        else if(lshMethod == "hpb64")
        {
            numBuckets = UINT64_MAX;
            compareStrategy = new COMPARE_STRAT<HASH_TYPE, NUM_HASHES, uint64_t, HASH_BLOCK_SIZE>(&threadPool, numBands, numBuckets, LSHBandHashMap<HASH_TYPE, uint64_t, NUM_HASHES>::ONLY_HASH_MAP, expectedDocs);
        }
        else if (lshMethod == "hpb32")
        {
            numBuckets = UINT32_MAX;
            compareStrategy = new COMPARE_STRAT<HASH_TYPE, NUM_HASHES, uint64_t, HASH_BLOCK_SIZE>(&threadPool, numBands, numBuckets, LSHBandHashMap<HASH_TYPE, uint64_t, NUM_HASHES>::ONLY_HASH_MAP, expectedDocs);
        }
        else
        {
            std::string str = "invalid lsh method";
            std::cout << str << std::endl;
            throw std::runtime_error(str);
        }

        // do something
        ComparerThread<HASH_TYPE, NUM_HASHES, HASH_BLOCK_SIZE>* comparerThread =
            new ComparerThread<HASH_TYPE, NUM_HASHES, HASH_BLOCK_SIZE>(&threadPool, &hashedDataQueue, nullptr, 2048,
                                                                        compareStrategy, matchThresh, matchThresh, std::max(1U, numThreads - baseThreads));

        //for binary 'dupe or not', we dont need to score, so use the threshval for early out as well
        //hashers have a static queue, one shared across them all
        std::future<void> comparerThreadFuture = threadPool.submit(&ComparerThread<HASH_TYPE, NUM_HASHES, HASH_BLOCK_SIZE>::Run, comparerThread);


        //and as the comparer spits out the dupes, we can start removing them from the datasets...
        DupeResolverThread* dupeResolverThread = new DupeResolverThread(basePath, outPath, 4096, false, noFileOut);
        std::future<void> dupeResolverThreadFuture = threadPool.submit(&DupeResolverThread::EnterProcFunc, dupeResolverThread, comparerThread->GetOutputQueuePtr(), &fileNamesVector);


        //wait for tasks to complete
        //ugly state thing until i sort out a better way
        int logEvery = 10;
        int curLoop = 0;
        int state = 0;
        uint32_t jaccardThreads = comparerThread->GetWorkerThreadCount();

        uint64_t lastCompareCount = 0;
        auto lastStatusReport = std::chrono::high_resolution_clock::now();

        while (true)
        {
            std::this_thread::sleep_for(1s);

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
            
            curLoop++;
            if (curLoop >= logEvery)
            {
                curLoop = 0;
                auto curTime = std::chrono::high_resolution_clock::now();
                auto totalElapsedTime = duration_cast<std::chrono::seconds>(curTime - startTime);
                auto elapsedSincelastStatus = duration_cast<std::chrono::milliseconds>(curTime - lastStatusReport);

                //calculate velocity of compares...
                size_t pendingCompare = hashedDataQueue.Length() + comparerThread->GetRemainingWork();

                uint64_t compared = comparerThread->GetComparedItems();
                double velocity = (double)(compared - lastCompareCount) / (double)elapsedSincelastStatus.count();
                velocity *= 1000.0;

                lastCompareCount = compared;
                lastStatusReport = curTime;

                std::cout << std::endl;
                std::cout << "[ " << totalElapsedTime.count() << "s ] Stats:";

                if (state < 1)
                    std::cout << "   [1]Docs Loading: " << arrowLoaderThread->GetTotalDocs();
                else
                    std::cout << "   [0]Docs Loaded: " << arrowLoaderThread->GetTotalDocs();

                if (state < 2)
                    std::cout << "   [" << numHasherThreads << "]Pending Hash: " << arrowLoaderThread->GetOutputQueuePtr()->Length();
                else
                    std::cout << "   [0]hashing Done!";

                std::cout << "   [" << jaccardThreads << "]Pending Jaccard: " << pendingCompare;                
                std::cout << std::endl;

                double remainingSeconds = pendingCompare / velocity;
                std::cout << "Compares per second: " << (uint32_t)velocity << ", estimated time reamining at current speed: " 
                    << remainingSeconds << " s (" 
                    << (uint32_t)(remainingSeconds / 60) << " m) "
                    << " (" << ((remainingSeconds / 60)/60)  << "h)" <<std::endl;
                std::cout << "Unique Docs: " << comparerThread->GetUniqueItemsCount() << "    estimated memory usage MB: " << comparerThread->GetMemUsageMB() << std::endl;
                std::cout << "Dupe Docs: " << dupeResolverThread->PendingDuplicates() << "   estimated memory usage MB: " << dupeResolverThread->GetEstimatedDupeMemeroyUsageMB() << std::endl;
                std::cout << "Number of LSH entries: " << comparerThread->GetNumLSHEntries() << "   estimated memory usage MB: " << comparerThread->GetEstimatedLSHMemoryUsageMB() << std::endl;
            }
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

        for (FileInfo* fi : fileNamesVector)
            delete fi;

        return 0;
    }
};


#define DO_RUN_AND_RETURN_IF(type,hashBlockSize,size,numhashes)         if (hashBlockSize <= size) \
{ \
    Runner<type, size, numhashes> runner;\
    return runner.Run(inputPath, outPath, dataColumnName, extension,\
        matchThresh, numHasherThreads, maxRecordsLoaded, hashSize, noFileOut, numBands, numBuckets, lshMethod);\
}

#define DO_RUN_AND_RETURN_IF_WITH_NUM_HASHES(hashSize,hashBlockSize,numhashes)  {\
    if (hashSize == 32)\
    {\
        DO_RUN_AND_RETURN_IF(uint32_t, hashBlockSize, 256, numhashes);\
        DO_RUN_AND_RETURN_IF(uint32_t, hashBlockSize, 512, numhashes);\
        DO_RUN_AND_RETURN_IF(uint32_t, hashBlockSize, 1024, numhashes);\
        DO_RUN_AND_RETURN_IF(uint32_t, hashBlockSize, 524288, numhashes);\
    }\
    else\
    {\
        DO_RUN_AND_RETURN_IF(uint64_t, hashBlockSize, 256, numhashes);\
        DO_RUN_AND_RETURN_IF(uint64_t, hashBlockSize, 512, numhashes);\
        DO_RUN_AND_RETURN_IF(uint64_t, hashBlockSize, 1024, numhashes);\
        DO_RUN_AND_RETURN_IF(uint64_t, hashBlockSize, 524288, numhashes);\
    }\
}





//minhash only pass
template<typename HASH_TYPE, int NUM_HASHES>
class MinhashOnlyRunner {
public:
    MinhashOnlyRunner()
    {}

    int Run(std::string inputPath, std::string outPath, std::string dataColumnName, std::string extension,
            int numHasherThreads, int maxRecordsLoaded, bool noFileOut)
    {
        std::filesystem::path basePath = inputPath;


        std::cout << "Running with params: " << basePath << ", " << extension << ", " << dataColumnName << ", " << outPath << std::endl;

        std::cout << "Scanning for " << extension << " files in " << basePath;

        //quick and sloppy lookups of filenames, so we dont have to store in each unit of data
        //saves # of loaded docs * string length of filepaths woth of memory
        static std::vector<FileInfo*> fileNamesVector;

        for (auto itEntry = std::filesystem::recursive_directory_iterator(basePath);
            itEntry != std::filesystem::recursive_directory_iterator();
            ++itEntry)
        {
            const auto filenameStr = itEntry->path().filename().string();
            std::string ext = std::filesystem::path(filenameStr).extension().string();

            if (ext == extension)
            {
                std::cout << "adding found file:  " << itEntry->path().string() << '\n';
                FileInfo* fi = new FileInfo(itEntry->path().string(), (uint32_t)fileNamesVector.size());
                fileNamesVector.push_back(std::move(fi));
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

        //put files into a lockable queue so we can have multiple fileloaders
        LockableQueue<FileInfo*> inputFileQueue;
        int fileId = 0;
        for (FileInfo* fi : fileNamesVector)
        {
            inputFileQueue.push(std::move(fi));
        }


        //track how long we take...
        auto startTime = std::chrono::high_resolution_clock::now();

        //======
        //start threads
        //======

        //minimum threads...
        // //TODO: change to accept number of IO in and out threads, the rest as hashers
        //1 loader thread, x hasher threads, 1 coallescing thread, 1 output thread
        uint32_t baseThreads = 3 + numHasherThreads;
        BS::thread_pool threadPool(baseThreads, 0);
        uint32_t numThreads = threadPool.get_thread_count();

        //reader thread
        ArrowLoaderThread* arrowLoaderThread = new ArrowLoaderThread(&threadPool, 1u, (uint32_t)maxRecordsLoaded, &inputFileQueue, dataColumnName);

        std::future<void> arrowLoaderThreadFuture = threadPool.submit(&ArrowLoaderThread::Run, arrowLoaderThread);

        //hasher threads
        LockableQueue< HasherThreadOutputData<HASH_TYPE>* > hashedDataQueue;
        std::vector< MinHasherThread<HASH_LENGTH_SHINGLES, NUM_HASHES, HASH_TYPE>* > hasherThreads;
        BS::multi_future<void> hasherThreadFutures;

        for (int i = 0; i < numHasherThreads; ++i)
        {
            auto hasherThread = new MinHasherThread<HASH_LENGTH_SHINGLES, NUM_HASHES, HASH_TYPE>(&threadPool, arrowLoaderThread->GetOutputQueuePtr(), &hashedDataQueue, 1024);
            hasherThreads.push_back(hasherThread);

            hasherThreadFutures.push_back(threadPool.submit(&MinHasherThread<HASH_LENGTH_SHINGLES, NUM_HASHES, HASH_TYPE>::Run, hasherThread));
        }


        CoalesceByFileThread<HASH_TYPE> coalesceThread(&threadPool, &hashedDataQueue, 1024, &fileNamesVector);
        std::future<void> coalesceThreadFuture = threadPool.submit(&CoalesceByFileThread<HASH_TYPE>::Run, coalesceThread);

        WriteMinhashArrowFileThread<HASH_TYPE> ioOutThread(&threadPool, coalesceThread.GetOutputQueuePtr(), 1, &fileNamesVector, basePath, outPath);

        //wait for tasks to complete
        //ugly state thing until i sort out a better way
        int logEvery = 10;
        int curLoop = 0;

        uint64_t lastCompareCount = 0;
        auto lastStatusReport = std::chrono::high_resolution_clock::now();

        while (true)
        {
            curLoop++;
            if (curLoop >= logEvery)
            {
                curLoop = 0;
                auto curTime = std::chrono::high_resolution_clock::now();
                auto totalElapsedTime = duration_cast<std::chrono::seconds>(curTime - startTime);
                auto elapsedSincelastStatus = duration_cast<std::chrono::milliseconds>(curTime - lastStatusReport);

                std::cout << std::endl;
                std::cout << "[ " << totalElapsedTime.count() << "s ] Stats:";


                std::cout << "   [ ]Docs Loading: " << arrowLoaderThread->GetTotalDocs();
                std::cout << "   [" << numHasherThreads << "]Pending Hash: " << arrowLoaderThread->GetOutputQueuePtr()->Length();
            }
        }

        auto stopTime = std::chrono::high_resolution_clock::now();

        //aaaand done
        auto duration = duration_cast<std::chrono::seconds>(stopTime - startTime);
        std::cout << "Finished in " << (uint64_t)(duration.count()) << "s" << std::endl;
        std::cout << "Total batches: " << arrowLoaderThread->GetTotalBatches() << " and total docs: " << arrowLoaderThread->GetTotalDocs() << std::endl;


        //clean up...
        threadPool.reset(0);
        delete arrowLoaderThread;

        for (FileInfo* fi : fileNamesVector)
            delete fi;

        return 0;
    }
};



//==========
// main
//==========
int ParseArguments(int argc, const char** argv)
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
    int numHasherThreads = 10; // 4; //more threads crunch through mroe input faster
    opt = app.add_option("-t,--hashThreads", numHasherThreads, "threads dedicated to hashing");

    int maxRecordsLoaded = 4096 * 48; //the higher this is, the higher memory usage can get
    opt = app.add_option("-r,--maxRecords", maxRecordsLoaded, "max arrow rows to load at once");

    int hashSize = 64; //hash key size for minhash
    opt = app.add_option("-s,--minhashKeySize", hashSize, "hash size in bits, valid values ( 32 or 64 )");

    int hashBlockSize = 524288; //size of contiguous memory blocks of hashes
    opt = app.add_option("-b,--hashBlockSize", hashBlockSize, "size of memory blocks of unique hashes, valid values ( 256, 512, 1024, 524288 )");

    int numHashes = 256; 
    opt = app.add_option("-n,--numMinhashKeys", numHashes, "number of hashes per minhash fingerprint ( 64, 128, 256 )");

    bool noFileOut = false;
    opt = app.add_option("-q,--noFileOut", noFileOut, "dont write out deduped files, useful for testing");

    uint32_t numBands = 32;
    opt = app.add_option("-l,--bands", numBands, "LSH bands ( numMinhashKeys (default 256) must be divisible by numBands evenly )");

    uint64_t numBuckets = UINT32_MAX;
    opt = app.add_option("-m,--buckets", numBuckets, "LSH buckets - number of buckets used inside LSH. larger = more memory)");

    std::string lshMethod = "rbs32";
    opt = app.add_option("--lsh", lshMethod, "rbs32 / rbs64: random bit sampling with 32 or 64 bit key, hpb64 / hpb32: hash map entry per bucket with 64 bit key");

    bool writeMinhashOnly = false;
    opt = app.add_option("--writeMinhashesOnly", writeMinhashOnly, "load, minhash, and write results to arrow files");

    for (char& c : lshMethod)
        c = ::tolower(c);

    try {
        app.parse(argc, argv);
    }
    catch (const CLI::ParseError& e) {
        std::cout << app.help("", CLI::AppFormatMode::All);
        return app.exit(e);
    }
    
    if (writeMinhashOnly == true)
    {
        MinhashOnlyRunner<uint64_t, 256> runner;
        return runner.Run(inputPath, outPath, dataColumnName, extension, numHasherThreads, maxRecordsLoaded, noFileOut);
    }
    
    if (numHashes == 4)
        DO_RUN_AND_RETURN_IF_WITH_NUM_HASHES(hashSize, hashBlockSize, 4);
    if (numHashes == 8)
        DO_RUN_AND_RETURN_IF_WITH_NUM_HASHES(hashSize, hashBlockSize, 8);
    if (numHashes == 32)
        DO_RUN_AND_RETURN_IF_WITH_NUM_HASHES(hashSize, hashBlockSize, 32);
    if (numHashes == 64)
        DO_RUN_AND_RETURN_IF_WITH_NUM_HASHES(hashSize, hashBlockSize, 64);
    if(numHashes == 128)
        DO_RUN_AND_RETURN_IF_WITH_NUM_HASHES(hashSize, hashBlockSize, 128);
    if(numHashes == 256)
        DO_RUN_AND_RETURN_IF_WITH_NUM_HASHES(hashSize, hashBlockSize, 256);

    return 0;
}
