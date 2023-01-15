#pragma once

#include "LockableQueue.h"

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
    uint32_t docId;
    uint32_t batchNum;
    std::shared_ptr<arrow::RecordBatch> recordbatch;
};


arrow::Status StreamArrowDataset(std::string path_to_file, uint32_t fileIndex, LockableQueue< ArrowLoaderThreadOutputData* >* batchQueue, int maxCapacity);


class ArrowLoaderThread
{
protected:
    std::thread* m_thread = nullptr;
    uint32_t fileIndex = 0;

public:
    ArrowLoaderThread()
    {
    }

    void Start(std::vector<std::string> paths_to_file, LockableQueue< ArrowLoaderThreadOutputData* >* batchQueue, int maxCapacity)
    {
        m_thread = new std::thread(&ArrowLoaderThread::EnterProcFunc, this, paths_to_file, batchQueue, maxCapacity);
    }

    void WaitForFinish()
    {
        m_thread->join();
    }

    //returns the index of the file vector of the file we are processing
    //this is used by the write out thread, to know which files are safe to start operating on
    int GetCurrentlyProcessingFileID()
    {
        return fileIndex;
    }

protected:
    void EnterProcFunc(std::vector<std::string> paths_to_file, LockableQueue< ArrowLoaderThreadOutputData* >* batchQueue, int maxCapacity)
    {
        for(fileIndex = 0; fileIndex < paths_to_file.size(); ++fileIndex)
        {
            std::string& path_to_file = paths_to_file[fileIndex];

            std::cout << "Streaming in from file " << path_to_file  << ", files remaining " << (paths_to_file.size() - fileIndex) << "\n";
            arrow::Status status = StreamArrowDataset(path_to_file, fileIndex, batchQueue, maxCapacity);
        }
    }
};

//=====
// arrow file streamer
//=====
arrow::Status StreamArrowDataset(std::string path_to_file, uint32_t fileIndex, LockableQueue< ArrowLoaderThreadOutputData* >* batchQueue, int maxCapacity)
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
        if (batchQueue->Length() >= maxCapacity)
        {
            std::this_thread::sleep_for(5s);
        }
        else
        {
            if (ipc_reader->ReadNext(&batch) == arrow::Status::OK() && batch != NULL)
            {
                ArrowLoaderThreadOutputData* data = new ArrowLoaderThreadOutputData();
                data->docId = fileIndex;
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

