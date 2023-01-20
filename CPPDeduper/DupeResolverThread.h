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
#include <set>

#include "ArrowLoaderThread.h"
#include "ComparerThread.h"
#include "Hashing.h"
#include "LockableQueue.h"


#define DEBUG_MESSAGES false

//resolves the dupe files, writes out to arrow memory mapped files to mimic hugging faces datasets
//only processes files completed by the loader thread, so we dont operate on the same files at the same time

class DupeResolverThread
{
protected:
    std::stop_source m_stop;

    //for storing duplicate items by file id
    std::map<uint32_t, std::set<int64_t > > fileIdToDuplicate;

    std::filesystem::path baseInPath;
    std::string baseOutPath;

    //some stats
    uint32_t pendingDuplicates = 0;
    uint32_t totalDuplicates = 0;
    uint32_t totalDuplicatesSkipped = 0;

    uint32_t workChunkSize;

    bool m_validateOutput;

public:
    DupeResolverThread(std::filesystem::path _baseInPath, std::string _baseOutPath, uint32_t _workChunkSize, bool _validateOutput)
        :baseInPath(_baseInPath),
        baseOutPath(_baseOutPath),
        workChunkSize(_workChunkSize),
        m_validateOutput(_validateOutput)
    {
    }

    ~DupeResolverThread()
    {
        for (auto pair : fileIdToDuplicate)
        {
            pair.second.clear();
        }
        fileIdToDuplicate.clear();
    }

    void WaitForFinish()
    {
        m_stop.request_stop();
    }

    uint32_t TotalDupes()
    {
        return totalDuplicates;
    }

    uint32_t TotalDupesRemoved()
    {
        return totalDuplicatesSkipped;
    }

    uint32_t PendingDuplicates()
    {
        return pendingDuplicates;
    }

    void EnterProcFunc(LockableQueue< CompareThreadDupeItem* >* duplicates, 
        std::vector<std::string>* fileNamesVector)
    {

        std::queue<CompareThreadDupeItem* > workQueue;
        CompareThreadDupeItem* workItem;

        //ok, we have a lot of info to work with here. items contain fingerprints for the docs, the doc id, and the batchnum it came from
        //TODO: theoretically we could work in place and scrub files we already processed... dangerous to destroy data though
        //for now, dump to a new location, but for future tests see about editing in place, once this thing is trustworthy
        while (!m_stop.stop_requested() || duplicates->Length() > 0)
        {
            //snooze
            if (duplicates->try_pop_range(&workQueue, workChunkSize, 10ms) == 0)
            {
                std::this_thread::sleep_for(500ms);
                continue;
            }

            while (workQueue.size() > 0)
            {
                ++pendingDuplicates;
                workItem = workQueue.front();
                workQueue.pop();

                //we cant do anything until the arrowStreamer has completed streaming in files...
                //lets jsut start stuffing them into the map as they filter in
                auto docIdList = fileIdToDuplicate.find(workItem->docId);
                if (docIdList == fileIdToDuplicate.end())
                {
                    fileIdToDuplicate.insert(std::pair{ workItem->docId, std::set<int64_t >({ workItem->rowNumber }) } );
                }
                else
                {
                    docIdList->second.insert(workItem->rowNumber);
                }
            }
        }

        //we can be super safe and handle removing the dupes after we have finished processing them all...
        //not as fast, but safe. do this for now, then improve it later
        //TODO: perf improvement
        for (uint32_t sourceFileInd = 0; sourceFileInd < fileNamesVector->size(); ++sourceFileInd)
        {
            std::string fname = (*fileNamesVector)[sourceFileInd];

            //create source and dest path for datasets
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


            std::cout << std::endl << "Processing filecopies/deduping from: " << fname << "  and writing to: " << outPath << std::endl;

            //see if we have dupes, if so, slow copy
            //else we can fullcopy 
            auto dupeIt = fileIdToDuplicate.find(sourceFileInd);
            if(dupeIt != fileIdToDuplicate.end())
            {
                totalDuplicates += (uint32_t)(*dupeIt).second.size();
                std::cout << "     Removing duplicates ->  duplicate count: " << (uint32_t)(*dupeIt).second.size() << std::endl;

                arrow::Status ret = CopyFileSansDupes(fname, outPath.string(), (*dupeIt).second);
            }
            else
            {
                std::cout << "      No dupes, std::filecopy" << std::endl;
                //faster copy                
                std::filesystem::copy_file(fname, outPath.string(), std::filesystem::copy_options::overwrite_existing);
            }
        }
        
    }


    protected:
        arrow::Status CopyFileSansDupes(std::string sourcePath, std::string outPath, std::set<int64_t> sortedDupes)
        {
            //open the arrow file, stream it in batches, write it in batchs ( if and only if its not in the list of dupes )

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

            //read
            size_t duplicateCount = sortedDupes.size();
            uint32_t batchCount = 0;
            int64_t batchLineNumOffset = 0;
            uint64_t rowsWritten = 0;
            uint64_t rowsLoaded = 0;

            std::shared_ptr<arrow::RecordBatch> record_batch;
            while (ipc_reader->ReadNext(&record_batch) == arrow::Status::OK() && record_batch != NULL)
            {
#if DEBUG_MESSAGES
                std::cout << "Processing batch " << batchCount << "  with lineoffset " << batchLineNumOffset << "  and numrows: " << record_batch->num_rows() << std::endl;
#endif
                std::shared_ptr<arrow::RecordBatch> outbatch = record_batch;;
                rowsLoaded += record_batch->num_rows();

                //calc the actual ronum of the first item
                if (sortedDupes.size() > 0)
                {
                    int64_t dupeRowNumAbsolute = *sortedDupes.begin();
                    //dupeRowNumAbsolute += (*sortedDupes.begin())->myHashData->arrowData->rowNum;
                    //row num is the offset of the rows loaded from the batch lodaer, plus the actual rownum in that batch

                    //error catch:
                    if (batchLineNumOffset > dupeRowNumAbsolute)
                    {
                        std::cout << " missed dupe with vals " << dupeRowNumAbsolute;
                    }

                    std::vector<std::shared_ptr<arrow::RecordBatch>> batches; //use this to start slicing out the parts we want to keep...

                    int64_t batchStart = 0;
                    int64_t batchEnd = 0;
                    if (dupeRowNumAbsolute >= batchLineNumOffset && dupeRowNumAbsolute < batchLineNumOffset + record_batch->num_rows())
                    {
                        //this loops, and slices everything *bnefore* the dupe, and we combine them all together to reassemble the table without dupes
                        while (dupeRowNumAbsolute >= batchLineNumOffset && dupeRowNumAbsolute < batchLineNumOffset + record_batch->num_rows())
                        {
#if false //DEBUG_MESSAGES                
                            ComparerThreadOutputData* compareData = *sortedDupes.begin();
                            std::cout << " looking at dupe with vals " << compareData->myHashData->arrowData->batchNum << ", "
                                << compareData->myHashData->arrowData->batchLineNumOffset << ", "
                                << compareData->myHashData->arrowData->rowNum << std::endl;
#endif
                            sortedDupes.erase(sortedDupes.begin());
                            ++totalDuplicatesSkipped;

                            uint64_t dupeRowNumRelative = dupeRowNumAbsolute - batchLineNumOffset;
                            batchEnd = dupeRowNumRelative;
                            if (batchEnd > batchStart)
                            {
                                std::shared_ptr<arrow::RecordBatch> sliced = record_batch->Slice(batchStart, batchEnd - batchStart);
                                //store in our batchVector
                                batches.push_back(sliced);
#if DEBUG_MESSAGES
                                std::cout << "Slicing at batchLineOffset: " << batchLineNumOffset << ", "
                                    << "from relativeStart: " << batchStart << ", "
                                    << "to relative end: " << batchEnd << ", "
                                    << "for dupe on absRow: " << dupeRowNumAbsolute << ", "
                                    << "and rel: " << dupeRowNumRelative << std::endl;
#endif
                            }
#if DEBUG_MESSAGES
                            else
                            {
                                //we are slicing the first item, so we dont actually have to slice
                                std::cout << "skipping item at batchLineOffset: " << batchLineNumOffset << ", "
                                    << "from relativeStart: " << batchStart << ", "
                                    << "to relative end: " << batchEnd << ", "
                                    << "for dupe on absRow: " << dupeRowNumAbsolute << ", "
                                    << "and rel: " << dupeRowNumRelative << std::endl;
                            }
#endif

                            //we start right after the spot we slice
                            //since we only slice one at a time
                            batchStart = batchEnd + 1;

                            if (sortedDupes.size() == 0)
                                break;

                            //continue removing/skipping items in range
                            dupeRowNumAbsolute = *sortedDupes.begin();
                        }
#if DEBUG_MESSAGES
                        std::cout << "stopped slicing due to dupeabs " << dupeRowNumAbsolute << " is >= batchln " << batchLineNumOffset << " + " << record_batch->num_rows() << std::endl;
                        std::cout << "   or dupeRowNumAbsolute < batchLineNumOffset " << dupeRowNumAbsolute << " < " << batchLineNumOffset << std::endl;
#endif
                        //now, we need to handle the very last slice. possible cases
                        //the last item was far beyond the end of this, so we have to grab the remaining portion of the table
                        //the whole batch was dupes, so drop it all
                        if (batchStart < record_batch->num_rows())
                        {
                            //remaing portions here
                            std::shared_ptr<arrow::RecordBatch> sliced = record_batch->Slice(batchStart, record_batch->num_rows() - batchStart);
                            batches.push_back(sliced);
                        }
                        else if (batches.size() == 0)
                        {
                            //we cut out the whole table
                            batchCount++;
                            batchLineNumOffset += record_batch->num_rows();
                            continue;
                        }


                        auto tblResult = arrow::Table::FromRecordBatches(batches);
                        if (!tblResult.ok())
                        {
                            std::cout << "ERROR: arrow table failed to create with status error: " << tblResult.status().message() << std::endl;
                        }

                        std::shared_ptr<arrow::Table> tbl = tblResult.MoveValueUnsafe();

                        auto combRes = tbl->CombineChunksToBatch();
                        if (!combRes.ok())
                        {
                            std::cout << "ERROR: arrow table failed to combine chunks " << combRes.status().message() << std::endl;
                        }
                        outbatch = combRes.MoveValueUnsafe();
                    }
#if DEBUG_MESSAGES
                    else
                    {
                        //hrm...
                        ComparerThreadOutputData* compareData = *sortedDupes.begin();
                        std::cout << "dupe failed range check with vals " << compareData->myHashData->arrowData->batchNum << ", "
                            << compareData->myHashData->arrowData->batchLineNumOffset << ", "
                            << compareData->myHashData->arrowData->rowNum << std::endl;

                        std::cout << "oopsie" << std::endl;
                    }
#endif
                }

                batchCount++;
                batchLineNumOffset += record_batch->num_rows();


                arrow::RecordBatch* writeBatch = outbatch.get();

                arrow::Status validated = writeBatch->Validate();
                if (validated != arrow::Status::OK())
                {
                    std::cout << "ERROR: recordBatch invalid! " << validated.message() << std::endl << std::endl;
                }

                if (batch_writer->WriteRecordBatch(*writeBatch) != arrow::Status::OK())
                {
                    //TODO:: ERROR
                    std::cout << "OH SNAP! FAILED TO WRITE SOMETHING!" << std::endl;
                }

                rowsWritten += writeBatch->num_rows();
#if DEBUG_MESSAGES   
                std::cout << "Loaded rows: " << rowsLoaded << "   written rows: " << rowsWritten << std::endl;
#endif
            }

            auto writeStats = batch_writer->stats();

            std::cout << "   write complete, out of " << rowsLoaded << " rows, " << rowsWritten << " rows were written. There were " << duplicateCount << " duplicates" << std::endl;
            std::cout << "   reported written by stats: " << writeStats.num_record_batches << " written batches. " << std::endl;

            //close stuff
            //TODO; need to do error checking
            arrow::Status status = batch_writer->Close();
            status = output_file->Close();

            status = ipc_reader->Close();
            status = input->Close();

            //delete it if we wrote nothing...
            if (rowsWritten == 0)
            {
                std::filesystem::remove(outPath);
            }


            if (m_validateOutput)
            {
                //verify the data written matches in length and what not
                std::shared_ptr<arrow::io::RandomAccessFile> input_dbgoutput;
                ARROW_ASSIGN_OR_RAISE(input_dbgoutput, arrow::io::MemoryMappedFile::Open(outPath, arrow::io::FileMode::READ));
                ARROW_ASSIGN_OR_RAISE(auto ipc_reader_dbg, arrow::ipc::RecordBatchStreamReader::Open(input_dbgoutput));

                uint32_t batchCount_dbg = 0;
                uint64_t rowsLoaded_dbg = 0;

                std::shared_ptr<arrow::RecordBatch> record_batch_dbg;
                arrow::Status dbgStatus = ipc_reader_dbg->ReadNext(&record_batch_dbg);
                while (dbgStatus == arrow::Status::OK() && record_batch_dbg != NULL)
                {
                    batchCount_dbg += 1;
                    rowsLoaded_dbg += record_batch_dbg->num_rows();
                    dbgStatus = ipc_reader_dbg->ReadNext(&record_batch_dbg);
                }
                std::cout << "   dbg file status: " << dbgStatus.message() << std::endl;
                std::cout << "   debug check batch count: " << batchCount_dbg << ",  check row count: " << rowsLoaded_dbg << std::endl;

                if (rowsLoaded_dbg != rowsWritten)
                {
                    std::cout << "ERROR: mismatch between what was written and what was loaded! investigate" << std::endl;
                    std::cout << "       checked file: " << outPath << std::endl << std::endl;
                }

                arrow::Status stat = ipc_reader_dbg->Close();
                stat = input_dbgoutput->Close();
            }

            return arrow::Status::OK();
        }

};
