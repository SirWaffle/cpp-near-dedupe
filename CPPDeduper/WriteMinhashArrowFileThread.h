#pragma once

#include "PipelineThread.h"
#include "LockableQueue.h"
#include "CoalesceByFileThread.h"

#include "arrow/api.h"
#include "arrow/io/api.h"
#include "arrow/result.h"
#include "arrow/util/type_fwd.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"
#include <arrow/csv/api.h>
#include <arrow/ipc/api.h>

#define IN_TYPE CoalescedItems<UINT_HASH_TYPE>*
#define OUT_TYPE void*


template<typename UINT_HASH_TYPE>
class WriteMinhashArrowFileThread : public PipelineThread<IN_TYPE, OUT_TYPE >
{
    //for storing duplicate items by file id
    std::vector<FileInfo*>* fileInfos;

    std::filesystem::path baseInPath;
    std::string baseOutPath;

public:
    WriteMinhashArrowFileThread(BS::thread_pool* _threadPool, LockableQueue< IN_TYPE >* _inQueue, uint32_t _workChunkSize, 
                                std::vector<FileInfo*>* _fileInfos, std::filesystem::path _baseInPath, std::string _baseOutPath)
        :PipelineThread<IN_TYPE, OUT_TYPE >(_threadPool, _inQueue, nullptr, _workChunkSize),
        fileInfos(_fileInfos),
        baseInPath(_baseInPath),
        baseOutPath(_baseOutPath)
    {
    }

    IN_TYPE workItem;
    bool DoWork(std::queue< IN_TYPE >* workQueue, std::queue< OUT_TYPE >* workOutQueue) final
    {
        workItem = workQueue->front();
        workQueue->pop();

        uint32_t fileId = workItem->fileId;

        //figure out the output file, create the folder if it needs to exist
        std::string fname = (*fileInfos)[fileId]->filePath;

        std::filesystem::path outPath = PrepareOutputPathFromInputPathName(fname);

        //loop through set, create arrow table, write out. also batch by some arbitrary amount to make laoding a bit nicer
        //just need the column of minhash data, as long as the row and filename stay the same
        //maybe jsut add the source row for ease of use...

        std::shared_ptr<arrow::Schema> schema =
            arrow::schema({ arrow::field("originRow", arrow::int64()), arrow::field("minhash", arrow::list(arrow::uint64())) });

        //TODO: error checking, remove some hardcoded stuff...

        const uint32_t batchSize = 256;

        //===================
        //open file for write
        //====================
        auto options = arrow::ipc::IpcWriteOptions::Defaults();
        auto output_file = arrow::io::FileOutputStream::Open(outPath.string()).ValueOrDie();
        auto batch_writer = arrow::ipc::MakeStreamWriter(output_file.get(), schema, options).ValueOrDie();


        uint32_t curCount = 0;

        auto arrowListDataType = arrow::list(arrow::uint64());

        arrow::ArrayBuilder ab(arrow::default_memory_pool());
        auto listBuild = arrow::FixedSizeListBuilder(arrow::default_memory_pool(), ab, arrowListDataType);

        for(auto iter = workItem->itemSet->begin(); iter != workItem->itemSet->end(); ++iter)
        {
            /*
            //arrow::int64 row_a;
            std::shared_ptr<arrow::Array> array_a;
            arrow::NumericBuilder<arrow::Int64Type> builder;
            ARROW_RETURN_NOT_OK(builder.AppendValues({ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 }));
            ARROW_RETURN_NOT_OK(builder.Finish(&array_a));

            arrow::Table::Make(schema, { row_a, array_a });
            */
        }



        return true;
    }

    std::filesystem::path PrepareOutputPathFromInputPathName(std::string fullInputFilePath)
    {
        //create source and dest path for datasets
        //lets use the baseIn, baseOut, and full filepath to mirror the initial datastructe and what not
        std::filesystem::path datasetPath = fullInputFilePath;
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


        std::cout << std::endl << "Processing filecopies/deduping from: " << fullInputFilePath << "  and writing to: " << outPath << std::endl;

        return outPath;
    }

};
