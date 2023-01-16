// CPPDeduper.cpp : Defines the entry point for the application.
//

#include "CPPDeduper.h"


/*
* TODO:
* - command line params (more, better, etc)
* - dont need to hang onto hashed data in the compare objects, just there for debugging ( RAM savings )
* - write out deduped files to another location
* 
* - fix all the really slow stuff in duperesolver, easy perf gains
* - fix structs passed between threads to use shared_ptr's
*/

static constexpr int HASH_LENGTH_SHINGLES = 5; //5 words used per hash
static constexpr int NUM_HASHES = 256; //256 //number of hashes for comparison
static constexpr int MAX_RECORDS_LOADED = 4096 * 16; //the higher this is, the higher memory usage can get
static constexpr double JACCARD_EARLY_OUT = 0.5; //speeds up the comparisons by early outting

//thread counts
static constexpr int NUM_HASHER_THREADS = 1; // 4; //more threads crunch through mroe input faster
static constexpr uint32_t NUM_INTERNAL_COMPARE_THREADS = 1; //12 is good //speeds up compares via multithreading

//quick and sloppy lookups of filenames, so we dont have to store in each unit of data
//saves # of laoded docs * string length of filepaths woth of memory
static std::vector<std::string> fileNamesVector;


//some stats to watch as the thing crunches. values are questionable since im
//no tbothering with thread saftey or anything, its all reads so w/e
static void StatsOutputThread_func(std::stop_source* threadstop, LockableQueue< ArrowLoaderThreadOutputData* >* batchQueue, LockableQueue< HasherThreadOutputData* >* hashedDataQueue, std::list< ComparerThreadOutputData* >* allComparedItems)
{
    auto startStats = std::chrono::high_resolution_clock::now();
    while (!threadstop->stop_requested())
    {
        std::this_thread::sleep_for(10s);
        auto curTime = std::chrono::high_resolution_clock::now();
        auto duration = duration_cast<std::chrono::seconds>(curTime - startStats);
        std::cout << "[" << duration << "] Backlogs:"
            << "   Awaiting Hashing: " << batchQueue->Length()
            << "   Awaiting Jaccard: " << hashedDataQueue->Length()
            << "   Awaiting output: " << allComparedItems->size()
            << std::endl;
    }
}



//==========
// main
//==========
int main(int argc, const char** argv)
{
    if (argc < 6)
    {
        std::cout << "usage: (this.exe) \"path\" \".ext\" \"dataColumn\" dupeThreashold \"outdir\"" << std::endl;
        std::cout << "example: (this.exe) \"c:\\baseDirWithLotsOfArrowInSubdirs\" \".arrow\" \"text\" 0.7 \"c:\\outDir\"" << std::endl;
        return 1;
    }

    //TODO: pass in arguments, for whatever params we want to control
    std::filesystem::path basePath = argv[1];
    std::string extension = argv[2];
    std::string dataColumnName = argv[3];

    char* p;
    double matchThreash = strtod(argv[4], &p);

    std::string output = argv[5];


    std::cout << "Running with params: " << basePath << ", " << extension << ", " << dataColumnName << ", " << matchThreash << ", " << output << std::endl;
    std::cout << "Scanning for " << extension << " files in " << basePath;

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

    //reader thread
    LockableQueue< ArrowLoaderThreadOutputData* > batchQueue;
    ArrowLoaderThread arrowLoaderThread;
    arrowLoaderThread.Start(fileNamesVector, &batchQueue, MAX_RECORDS_LOADED);

    //hasher threads
    LockableQueue< HasherThreadOutputData* > hashedDataQueue;
    std::vector< HasherThread<HASH_LENGTH_SHINGLES, NUM_HASHES>* > hasherThreads;
    for (int i = 0; i < NUM_HASHER_THREADS; ++i)
    {
        auto hasherThread = new HasherThread<HASH_LENGTH_SHINGLES, NUM_HASHES>();
        hasherThread->Start(&batchQueue, &hashedDataQueue, 64, dataColumnName);
        hasherThreads.push_back(hasherThread);
    }

    //comparer
    std::list< ComparerThreadOutputData* > allComparedItems;
    LockableQueue< ComparerThreadOutputData* > duplicates;
    ComparerThread comparerThread(true, NUM_INTERNAL_COMPARE_THREADS);
    comparerThread.Start(&hashedDataQueue, &allComparedItems, &duplicates, 64, JACCARD_EARLY_OUT, matchThreash, NUM_HASHES);

    //and as the comparer spits out the dupes, we can start removing them from the datasets...
    //or something... not entirely sure how i want to handle this yet...
    DupeResolverThread dupeResolverThread(basePath, output);
    dupeResolverThread.Start(&allComparedItems, &duplicates, &arrowLoaderThread, &fileNamesVector);
    
    //lets start a stats out thread so we dont get bored
    std::stop_source statThreadStopper;
    std::thread statsThread(StatsOutputThread_func, &statThreadStopper, &batchQueue, &hashedDataQueue, &allComparedItems);

    //reader thread is first to finish in this pipeline
    arrowLoaderThread.WaitForFinish();

    //hashers are next to finish
    for (int i = 0; i < NUM_HASHER_THREADS; ++i)
    {
        hasherThreads[i]->WaitForFinish();
        delete hasherThreads[i];
        hasherThreads[i] = nullptr;
    }

    //comparer now
    comparerThread.WaitForFinish();

    //and the final peice, writting out the new datasets
    dupeResolverThread.WaitForFinish();


    auto stopTime = std::chrono::high_resolution_clock::now();

    statThreadStopper.request_stop();
    statsThread.join();

    //aaaand done
    auto duration = duration_cast<std::chrono::microseconds>(stopTime - startTime);
    std::cout << "Finished in " << (uint64_t)(duration.count() / 1000) << "ms" << std::endl;
    std::cout << "Found " << allComparedItems.size() << " Unique documents" << std::endl;
    std::cout << "Found " << dupeResolverThread.TotalDupes() << " duplicates, and removed " << dupeResolverThread.TotalDupesRemoved() << std::endl;
    std::cout << "Total batches: " << arrowLoaderThread.GetTotalBatches() << " and total docs: " << arrowLoaderThread.GetTotalDocs() << std::endl;

}
