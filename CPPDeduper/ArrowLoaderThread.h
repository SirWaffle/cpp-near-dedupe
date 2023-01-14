#pragma once

#include "LockableQueue.h"
#include "SimpleLogging.h"

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

using namespace std::chrono_literals;

//data 
struct ArrowLoaderThreadOutputData
{
    std::string docName;
    uint32_t batchNum;
    std::shared_ptr<arrow::RecordBatch> recordbatch;
};


arrow::Status StreamArrowDataset(std::string path_to_file, LockableQueue< ArrowLoaderThreadOutputData* >* batchQueue, int maxCapacity);


class ArrowLoaderThread
{
protected:
    std::thread* m_thread = nullptr;

public:
    ArrowLoaderThread()
    {
    }

    void Start(std::list<std::string> paths_to_file, LockableQueue< ArrowLoaderThreadOutputData* >* batchQueue, int maxCapacity)
    {
        m_thread = new std::thread(&ArrowLoaderThread::EnterProcFunc, this, paths_to_file, batchQueue, maxCapacity);
    }

    void WaitForFinish()
    {
        m_thread->join();
    }

protected:
    void EnterProcFunc(std::list<std::string> paths_to_file, LockableQueue< ArrowLoaderThreadOutputData* >* batchQueue, int maxCapacity)
    {
        while (paths_to_file.size() > 0)
        {
            std::string path_to_file = paths_to_file.front();
            paths_to_file.pop_front();

            ConsoleLogAlways(std::format("Streaming in from file {}, files remaining {}\n", path_to_file, paths_to_file.size()));
            arrow::Status status = StreamArrowDataset(path_to_file, batchQueue, maxCapacity);
        }
    }
};

//=====
// arrow file streamer
//=====
arrow::Status StreamArrowDataset(std::string path_to_file, LockableQueue< ArrowLoaderThreadOutputData* >* batchQueue, int maxCapacity)
{
    //open file
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    std::shared_ptr<arrow::io::RandomAccessFile> input;
    ARROW_ASSIGN_OR_RAISE(input, arrow::io::MemoryMappedFile::Open(path_to_file, arrow::io::FileMode::READ));
    ARROW_ASSIGN_OR_RAISE(auto ipc_reader, arrow::ipc::RecordBatchStreamReader::Open(input));

    //read batches
    int batchNum = 0;
    while (true)
    {
        std::shared_ptr<arrow::RecordBatch> batch;
        if (batchQueue->Length() >= 256)
        {
            std::this_thread::sleep_for(5s);
        }
        else
        {
            if (ipc_reader->ReadNext(&batch) == arrow::Status::OK() && batch != NULL)
            {
                ArrowLoaderThreadOutputData* data = new ArrowLoaderThreadOutputData();
                data->docName = path_to_file;
                data->batchNum = batchNum++;
                data->recordbatch = std::move(batch);
                batchQueue->push(std::move(data));
            }
            else
            {
                break;
            }
        }
    }

    arrow::Status status = ipc_reader->Close();
    status = input->Close();
    return arrow::Status::OK();
}

