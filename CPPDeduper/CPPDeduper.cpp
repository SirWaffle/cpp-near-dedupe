// CPPDeduper.cpp : Defines the entry point for the application.
//

#include "CPPDeduper.h"


/*
* TODO:
* - command line params
* - can squash filename to a lookup table to reduce instances of filename strings in memory (RAM savings )
* - dont need to hang onto hashed data in the compare objects, just there for debugging ( RAM savings )
* - write out deduped files to another location
*/

static constexpr int HASH_LENGTH_SHINGLES = 5; //5 words used per hash
static constexpr int NUM_HASHES = 256; //number of hashes for comparison
static constexpr int NUM_HASHER_THREADS = 8; //more threads crunch through mroe input faster
static constexpr int MAX_RECORDS_LOADED = 4096 * 16; //the higher this is, the higher memory usage can get

//quick and sloppy lookups of filenames, so we dont have to store in each unit of data
static std::vector<std::string> fileNamesVector;



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
//dedpue "path to dir" ".arrow" 
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

    //hard coded stuff for testing
    //std::string dataColumnName = "text";
    double jaccardEarlyOut = 0.5; //speed up
    //double matchThresh = 0.7f; //Minhash threshold for match
    //std::string output = "d:/testOutput-dedup/"; //not sure
    //basePath = "D:\\carp\\pile-v2-eda\\local_dedup";

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
    ComparerThread comparerThread(true);
    comparerThread.Start(&hashedDataQueue, &allComparedItems, 64, jaccardEarlyOut, matchThreash, NUM_HASHES);

    //lets start a stats out thread so we dont get bored
    std::stop_source statThreadStopper;
    std::thread statsThread(StatsOutputThread_func, &statThreadStopper, &batchQueue, &hashedDataQueue, &allComparedItems);

    //run everything until the reader thread finishes
    arrowLoaderThread.WaitForFinish();

    //now set the stop for the hasher, it will quit when the stop flag is on and the queue is empty of work
    for (int i = 0; i < NUM_HASHER_THREADS; ++i)
    {
        hasherThreads[i]->WaitForFinish();
    }

    //now that all the work has gone to the writer, request it to exit, and wait till its done
    comparerThread.WaitForFinish();

    //write out scores / dupes 
    DupeResolverThread dupeResolverThread;
    dupeResolverThread.Start(&allComparedItems);
    dupeResolverThread.WaitForFinish();


    auto stopTime = std::chrono::high_resolution_clock::now();

    statThreadStopper.request_stop();
    statsThread.join();

    //aaaand done
    auto duration = duration_cast<std::chrono::microseconds>(stopTime - startTime);
    std::cout << "Finished in " << (uint64_t)(duration.count() / 1000) << "ms" << std::endl;
}
