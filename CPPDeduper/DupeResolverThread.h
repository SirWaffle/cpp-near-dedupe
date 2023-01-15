#pragma once

#include "arrow/api.h"
#include "arrow/io/api.h"
#include "arrow/result.h"
#include "arrow/util/type_fwd.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"
#include <arrow/csv/api.h>
#include <arrow/ipc/api.h>

#include <chrono>
#include <iostream>

#include "ArrowLoaderThread.h"
#include "ComparerThread.h"
#include "Hashing.h"
#include "LockableQueue.h"

//resolves the dupe files, writes out to arrow memory mapped files to mimic hugging faces datasets
//only processes files completed by the loader thread, so we dont operate on the same files at the same time

//insertion of elements into the dupes per file list..
bool compare_batchNums(const ComparerThreadOutputData* first, ComparerThreadOutputData* second)
{
    return (first->myHashData->batchNum < second->myHashData->batchNum);
}


class DupeResolverThread
{
protected:
    std::thread* m_thread = nullptr;
    std::stop_source m_stop;

    //for storing duplicate items by file id
    std::map<uint32_t, std::list<ComparerThreadOutputData*> > fileIdToDuplicate;

    std::filesystem::path baseInPath;
    std::string baseOutPath;

    //some stats
    uint32_t totalDuplicates = 0;
    uint32_t totalDuplicatesSkipped = 0;

public:
    DupeResolverThread(std::filesystem::path _baseInPath, std::string _baseOutPath)
        :baseInPath(_baseInPath),
        baseOutPath(_baseOutPath)
    {
    }

    void Start(std::list< ComparerThreadOutputData* >* allComparedItems, 
        LockableQueue< ComparerThreadOutputData* >* duplicates, 
        ArrowLoaderThread* arrowLoaderThread,
        std::vector<std::string>* fileNamesVector)
    {
        m_thread = new std::thread(&DupeResolverThread::EnterProcFunc, this, m_stop, allComparedItems, duplicates, arrowLoaderThread, fileNamesVector);
    }

    void WaitForFinish()
    {
        m_stop.request_stop();
        m_thread->join();
    }

    uint32_t TotalDupes()
    {
        return totalDuplicates;
    }

    uint32_t TotalDupesRemoved()
    {
        return totalDuplicatesSkipped;
    }

protected:
    arrow::Status CopyFileSansDupes(std::string sourcePath, std::string outPath, std::list<ComparerThreadOutputData*> sortedDupes)
    {
        //open the arrow file, stream it in batches, write it in batchs ( if and only if its not in the list of dupes )
        //some magic to mirror the same dir structure and filenames from the source...somehow... 

        //===================
        //open file for read
        //====================
        std::shared_ptr<arrow::io::RandomAccessFile> input;
        ARROW_ASSIGN_OR_RAISE(input, arrow::io::MemoryMappedFile::Open(sourcePath, arrow::io::FileMode::READ));
        ARROW_ASSIGN_OR_RAISE(auto ipc_reader, arrow::ipc::RecordBatchStreamReader::Open(input));   

        //===================
        //open file for write
        //====================
        auto options = arrow::ipc::IpcWriteOptions::Defaults();
        ARROW_ASSIGN_OR_RAISE(auto output_file, arrow::io::FileOutputStream::Open(outPath));
        std::shared_ptr<arrow::Schema> schema = ipc_reader->schema();
        ARROW_ASSIGN_OR_RAISE(auto batch_writer, arrow::ipc::MakeStreamWriter(output_file.get(), schema, options));

        return arrow::Status::OK();
        /*
        //read
        uint32_t batchCount = 0;
        std::shared_ptr<arrow::RecordBatch> record_batch;
        while (ipc_reader->ReadNext(&record_batch) == arrow::Status::OK() && record_batch != NULL)
        {
            //will need to ahndle having to remove ROWS // or rebuilding the batch without the dupes
            //TODO:
            if(sortedDupes.size() > 0 && sortedDupes.front()->maxMatchedData->batchNum == batchCount)
            {
                //TODO: use the hashing to determin if the fingerprints are the same to be extra careful and verify
                //dupe, dont copy...
                sortedDupes.pop_front();
                batchCount++;
                ++totalDuplicatesSkipped;
                continue;
            }

            batchCount++;

            //todo, check against out list of dupes, if its there, do not* write it out
            arrow::RecordBatch* batch = record_batch.get();
            if (batch_writer->WriteRecordBatch(*batch) != arrow::Status::OK())
            {
                //TODO:: ERROR
                std::cout << "OH SNAP 2!" << std::endl;
            }
        }

        //close stuff
        arrow::Status status = ipc_reader->Close();
        status = input->Close();

        ARROW_RETURN_NOT_OK(output_file->Close());

        return arrow::Status::OK();*/
    }


    void EnterProcFunc(std::stop_source stop, std::list< ComparerThreadOutputData* >* allComparedItems, 
        LockableQueue< ComparerThreadOutputData* >* duplicates, 
        ArrowLoaderThread* arrowLoaderThread,
        std::vector<std::string>* fileNamesVector)
    {
        (void*)arrowLoaderThread;
        (void*)allComparedItems;

        std::queue<ComparerThreadOutputData* > workQueue;
        ComparerThreadOutputData* workItem;

        //ok, we have a lot of info to work with here. items contain fingerprints for the docs, the doc id, and the batchnum it came from
        //TODO: verify this is enough to locate the original entry so we can remove it.
        //TODO: theoretically we could work in place and scrub files we already processed... dangerous to destroy data though
        //for now, dump to a new location, but for future tests see about editing in place, once this thing is trustworthy
        while (!stop.stop_requested() || duplicates->Length() > 0)
        {
            //snooze
            if (duplicates->try_pop_range(&workQueue, 64, 1ms) == 0)
            {
                std::this_thread::sleep_for(50ms);
                continue;
            }

            while (workQueue.size() > 0)
            {
                workItem = workQueue.front();
                workQueue.pop();

                //we cant do anything until the arrowStreamer has completed streaming in files...
                //lets jsut start stuffing them into the map as they filter in
                auto docIdList = fileIdToDuplicate.find(workItem->myHashData->docId);
                if (docIdList == fileIdToDuplicate.end())
                {
                    fileIdToDuplicate.insert(std::pair{ workItem->myHashData->docId, std::list<ComparerThreadOutputData*>({ workItem }) } );
                }
                else
                {
                    docIdList->second.push_back(workItem);
                }
            }
        }

        //we can be super safe and handle removing the dupes after we have finished processing them all...
        //not as fast, but safe. do this for now, then improve it later
        //TODO: perf improvement
        for (auto it = fileIdToDuplicate.begin(); it != fileIdToDuplicate.end(); it++)
        {
            totalDuplicates += (uint32_t)(*it).second.size();

            std::string fname = (*fileNamesVector)[(*it).first];
            std::cout << "DocId: " << (*it).first << "  Count: " << (*it).second.size() << "  Dataset path: " << fname << std::endl;

            //lets use the baseIn, baseOut, and full filepath to mirror the initial datastructe and what not
            std::filesystem::path datasetPath = fname;
            std::filesystem::path outPath = baseOutPath;

            //need to figure out the common path, then copy the diff parts to the outpath...
            std::filesystem::path parent = datasetPath.parent_path();
            std::list<std::string> parentPaths;
            while (parent.has_parent_path())
            {
                //some kind of compare to see if we're at the same spot as the base path..
                if (parent == baseInPath)
                {
                    //now append everything from this spot forwards...
                    while (parentPaths.size() > 0)
                    {
                        outPath /= parentPaths.front();
                        parentPaths.pop_front();
                    }

                    break;
                }

                parentPaths.push_front(parent.stem().string());
                parent = parent.parent_path();
            }

            //make the dirs in the output folder
            std::filesystem::create_directories(outPath);

            //append filename and extension
            outPath /= datasetPath.stem(); //filename
            outPath += datasetPath.extension();

            //lazy, sort the list. would be better ot insert sorted but, do that once this works
            //TODO: perf
            (*it).second.sort(compare_batchNums);

            arrow::Status ret = CopyFileSansDupes(fname, outPath.string(), (*it).second);
        }
        
    }
};
