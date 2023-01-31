#pragma once

#include "PipelineThread.h"

#include "LockableQueue.h"

#include "arrow/api.h"
#include "arrow/io/api.h"
#include "arrow/result.h"
#include "arrow/util/type_fwd.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"
#include <arrow/csv/api.h>
#include <arrow/ipc/api.h>

#include "Hashing.h"

#include <chrono>
#include <iostream>

#define LOAD_TEST false

#if LOAD_TEST
#include <random>
#include <algorithm>
#endif

using namespace std::chrono_literals;

//data 
struct ArrowLoaderThreadOutputData
{
    ArrowLoaderThreadOutputData(uint32_t _docid, int64_t _batchLineNumOffset, int64_t _rowNum, U16String* _data)
        :docId(_docid),
        data(std::move(_data))
    {
        rowNumber = _batchLineNumOffset + _rowNum;
    }

    ~ArrowLoaderThreadOutputData()
    {
        DeleteData();
    }

    void DeleteData()
    {
        //data->clear();
        if (data != nullptr)
        {
            delete data;
            data = nullptr;
        }
    }

    uint32_t docId;
    int64_t rowNumber;
    U16String* data;

#ifdef _DEBUG
    std::string sourceFilePath;
#endif
};

struct FileInfo
{
    std::string filePath;
    uint32_t fileId = -1;
    std::atomic<bool> finishedLoad = 0;
    std::atomic<int64_t> numRowsLoaded = 0;

    FileInfo(std::string path, uint32_t id)
        :filePath(path),
        fileId(id)
    {       
    }
};


#define IN_TYPE FileInfo*
#define OUT_TYPE ArrowLoaderThreadOutputData*

class ArrowLoaderThread: public PipelineThread<IN_TYPE, OUT_TYPE >
{
protected:
    uint32_t fileIndex = 0;
    uint32_t totalbatches = 0;
    uint64_t totaldocs = 0;

    uint32_t maxLoadedRecordsQueued;

    LockableQueue< OUT_TYPE > batchQueue;

    const size_t pushWorkQueueToOutputSize = 200;

    std::string dataColumnName;

public:
    ArrowLoaderThread(BS::thread_pool* _threadPool, uint32_t _workChunkSize, 
                    uint32_t _maxLoadedRecordsQueued, LockableQueue< IN_TYPE >* _inQueue, std::string _dataColumnName)
        :PipelineThread( _threadPool, _inQueue, nullptr, _workChunkSize),
        maxLoadedRecordsQueued(_maxLoadedRecordsQueued),
        dataColumnName(_dataColumnName)
    {
    }

    uint32_t GetTotalBatches()
    {
        return totalbatches;
    }

    uint64_t GetTotalDocs()
    {
        return totaldocs;
    }

    virtual bool DoWork(std::queue< IN_TYPE >* workQueue, std::queue< OUT_TYPE >* workOutQueue) final
    {
        FileInfo* fi = workQueue->front();
        workQueue->pop();
        std::string& path_to_file = fi->filePath;

        uint64_t rowsLoaded = 0;

        std::cout << "File: " << fi->fileId << " -> Streaming from: " << path_to_file << std::endl;
        arrow::Status status = StreamArrowDataset(path_to_file, fi->fileId, maxLoadedRecordsQueued, dataColumnName, rowsLoaded);

        fi->finishedLoad.store(true);
        fi->numRowsLoaded.store(rowsLoaded);

        return true; 
    }

protected:
    arrow::Status StreamArrowDataset(std::string path_to_file, uint32_t fileIndex, int maxCapacity, std::string dataColumnName, uint64_t& rowsLoaded)
    {
        std::queue< ArrowLoaderThreadOutputData* > outWorkQueue;

        //open file
        std::shared_ptr<arrow::io::RandomAccessFile> input;
        ARROW_ASSIGN_OR_RAISE(input, arrow::io::MemoryMappedFile::Open(path_to_file, arrow::io::FileMode::READ));
        ARROW_ASSIGN_OR_RAISE(auto ipc_reader, arrow::ipc::RecordBatchStreamReader::Open(input));

        //no worky for this format
        //std::shared_ptr<arrow::io::ReadableFile> input;
        //ARROW_ASSIGN_OR_RAISE(input, arrow::io::ReadableFile::Open(path_to_file, arrow::default_memory_pool()));
        //ARROW_ASSIGN_OR_RAISE(auto ipc_reader, arrow::ipc::RecordBatchFileReader::Open(input));

        //read batches
        int batchNum = 0;
        int64_t lineNumOffset = 0;
        while (true)
        {
            if (batchQueue.Length() >= maxCapacity)
            {
                std::this_thread::sleep_for(200ms);
            }
            else
            {
                //ARROW_ASSIGN_OR_RAISE(batch, ipc_reader->ReadRecordBatch(batchNum));
                std::shared_ptr<arrow::RecordBatch> batch;
                if (ipc_reader->ReadNext(&batch) == arrow::Status::OK() && batch != NULL)
                {
                    ++totalbatches;
                    totaldocs += batch->num_rows();

                    std::shared_ptr<arrow::Array> array = batch->GetColumnByName(dataColumnName);
                    arrow::StringArray stringArray(array->data());

                    if (stringArray.length() == 0)
                    {
                        continue;
                    }
                    else
                    {
                        for (int64_t i = 0; i < stringArray.length(); ++i)
                        {
                            if (stringArray.length() != batch->num_rows())
                            {
                                std::cout << "what do we have here???" << std::endl;
                            }

                            std::string_view view = stringArray.GetView(i);

#if LOAD_TEST
                            for (int testLoad = 0; testLoad < 10000; ++testLoad)
#endif
                            {
                                //convert and send
                                U16String* u16str = new U16String();
                                u16str->reserve(view.size());
                                CharPtrToUStr(view.data(), view.size(), *u16str);

#if LOAD_TEST

                                //need to change line offset or the checker thread will ignore it
                                static uint32_t inc = 0;
                                ++inc;

                                //shuffle it good so we dont get duplicate docs...that just causes worst case scenario with hashing 
                                std::random_device rd;
                                std::mt19937 g(rd());
                                std::shuffle(u16str->begin(), u16str->end(), g);
                                ArrowLoaderThreadOutputData* data = new ArrowLoaderThreadOutputData(inc, lineNumOffset + 1, i, std::move(u16str));
#else
                                ArrowLoaderThreadOutputData* data = new ArrowLoaderThreadOutputData(fileIndex, lineNumOffset, i, std::move(u16str));
#endif

#ifdef _DEBUG
                                data->sourceFilePath = path_to_file;
#endif
                                outWorkQueue.push(std::move(data));

                                if (outWorkQueue.size() >= pushWorkQueueToOutputSize)
                                {
                                    //lock and push to outputQueue
                                    outQueue->push_queue(&outWorkQueue);
                                }
                            }
                        }
                    }
                    batchNum++;
                    lineNumOffset += batch->num_rows(); //next offset for the next batch

                    batch.reset();
                }
                else
                {
                    break;
                }
            }
        }

        rowsLoaded = lineNumOffset;

        //lock queue and push to outqueue with whatever remains
        if(outWorkQueue.size() > 0)
            outQueue->push_queue(&outWorkQueue);

        arrow::Status status = ipc_reader->Close();
        status = input->Close();
        return arrow::Status::OK();
    }

};