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
        delete data;
        data = nullptr;
    }

    uint32_t docId;
    int64_t rowNumber;
    U16String* data;

#ifdef _DEBUG
    std::string sourceFilePath;
#endif
};


class ArrowLoaderThread
{
protected:
    uint32_t fileIndex = 0;
    uint32_t totalbatches = 0;
    uint64_t totaldocs = 0;

    uint32_t maxLoadedRecordsQueued;

    LockableQueue< ArrowLoaderThreadOutputData* > batchQueue;

    const size_t pushWorkQueueToOutputSize = 200;

public:
    ArrowLoaderThread(uint32_t _maxLoadedRecordsQueued)
        :maxLoadedRecordsQueued(_maxLoadedRecordsQueued)
    {
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

    LockableQueue< ArrowLoaderThreadOutputData* >* GetOutputQueuePtr()
    {
        return &batchQueue;
    }


    void EnterProcFunc(std::vector<std::string> paths_to_file, std::string dataColumnName)
    {
        for (fileIndex = 0; fileIndex < paths_to_file.size(); ++fileIndex)
        {
            std::string& path_to_file = paths_to_file[fileIndex];

            std::cout << "File: " << (paths_to_file.size() - fileIndex) << " -> Streaming from: " << path_to_file << std::endl;
            arrow::Status status = StreamArrowDataset(path_to_file, fileIndex, maxLoadedRecordsQueued, dataColumnName);
        }
    }

protected:
    arrow::Status StreamArrowDataset(std::string path_to_file, uint32_t fileIndex, int maxCapacity, std::string dataColumnName)
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

                            //convert and send
                            U16String* u16str = new U16String();
                            CharPtrToUStr(view.data(), view.size(), *u16str);

                            ArrowLoaderThreadOutputData* data = new ArrowLoaderThreadOutputData(fileIndex, lineNumOffset, i, std::move(u16str));
#ifdef _DEBUG
                            data->sourceFilePath = path_to_file;
#endif
                            outWorkQueue.push(std::move(data));

                            if (outWorkQueue.size() >= pushWorkQueueToOutputSize)
                            {
                                //lock and push to outputQueue
                                batchQueue.push_queue(&outWorkQueue);
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

        //lock queue and push to outqueue with whatever remains
        if(outWorkQueue.size() > 0)
            batchQueue.push_queue(&outWorkQueue);

        arrow::Status status = ipc_reader->Close();
        status = input->Close();
        return arrow::Status::OK();
    }

};