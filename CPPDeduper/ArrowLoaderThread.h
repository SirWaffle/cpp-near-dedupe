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

#include "Hashing.h"

#include <chrono>
#include <iostream>

using namespace std::chrono_literals;

//data 
struct ArrowLoaderThreadOutputData
{
    ArrowLoaderThreadOutputData(uint32_t _docid, uint32_t _batchNum, int64_t _batchLineNumOffset, int64_t _rowNum, U16String* _data)
        :docId(_docid),
        batchNum(_batchNum),
        batchLineNumOffset(_batchLineNumOffset),
        rowNum(_rowNum),
        data(std::move(_data))
    { }
    uint32_t docId;
    uint32_t batchNum;
    int64_t batchLineNumOffset;
    int64_t rowNum;
    U16String* data;
};


class ArrowLoaderThread
{
protected:
    std::thread* m_thread = nullptr;
    uint32_t fileIndex = 0;

    uint32_t totalbatches = 0;
    uint64_t totaldocs = 0;

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

    uint32_t GetTotalBatches()
    {
        return totalbatches;
    }

    uint64_t GetTotalDocs()
    {
        return totaldocs;
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

    arrow::Status StreamArrowDataset(std::string path_to_file, uint32_t fileIndex, LockableQueue< ArrowLoaderThreadOutputData* >* batchQueue, int maxCapacity);
};


//=====
// arrow file streamer
//=====
arrow::Status ArrowLoaderThread::StreamArrowDataset(std::string path_to_file, uint32_t curfileInd, LockableQueue< ArrowLoaderThreadOutputData* >* batchQueue, int maxCapacity)
{
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
        if (batchQueue->Length() >= maxCapacity)
        {
            std::this_thread::sleep_for(5s);
        }
        else
        {
            //ARROW_ASSIGN_OR_RAISE(batch, ipc_reader->ReadRecordBatch(batchNum));
            std::shared_ptr<arrow::RecordBatch> batch;
            if (ipc_reader->ReadNext(&batch) == arrow::Status::OK() && batch != NULL)
            {
                ++totalbatches;
                totaldocs += batch->num_rows();
                
                std::shared_ptr<arrow::Array> array = batch->GetColumnByName("text");
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

                        //convert and send
                        U16String* u16str = new U16String();
                        CharPtrToUStr(view.data(), view.size(), *u16str);

                        ArrowLoaderThreadOutputData* data = new ArrowLoaderThreadOutputData(curfileInd, batchNum, lineNumOffset, i, std::move(u16str));
                        batchQueue->push(std::move(data));
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

    std::cout << "Closing file " << path_to_file << std::endl;

    arrow::Status status = ipc_reader->Close();
    status = input->Close();
    return arrow::Status::OK();
}

