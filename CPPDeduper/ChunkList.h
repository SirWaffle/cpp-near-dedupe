#pragma once

#include <list>
#include <vector>



//just test with full blocks for now
template<typename TYPE, uint32_t BLOCK_SIZE>
class GrowOnlyChunkList
{
public:

    struct Block
    {
        uint32_t size = 0;
        std::array<TYPE, BLOCK_SIZE>* entries = nullptr;

        ~Block()
        {
            delete entries;
        }
    };

    typedef std::list< Block* >::iterator iterator;

public:
    GrowOnlyChunkList(uint64_t _initialCapacity, bool prealloc = false)
        :initialCapacity(_initialCapacity),
        numEntries(0)
    {
        AddUsingBlock();

        if (prealloc == true)
        {
            for (int i = 0; i < initialCapacity; ++i)
            {   
                AddFreeBlock();
            }
        }
    }

    ~GrowOnlyChunkList()
    {
        for (auto block : usedBlocks)
            delete block;
    }

    auto begin()
    {
        return usedBlocks.begin();
    }

    auto end()
    {
        return usedBlocks.end();
    }

    uint64_t NumEntries()
    {
        return numEntries;
    }

    size_t NumBlocks()
    {
        return usedBlocks.size();
    }

    iterator Begin()
    {
        return usedBlocks.begin();
    }

    iterator End()
    {
        return usedBlocks.end();
    }

    uint64_t MemoryUsage()
    {
        //TODO: update to be correct
        uint64_t items = uint64_t(usedBlocks.size() * BLOCK_SIZE) - BLOCK_SIZE + usedBlocks.back()->size;
        return items * (uint64_t)sizeof(TYPE);
    }

    bool IsEmpty()
    {
        return empty;
    }

    void* AllocNewItem()
    {
        ++numEntries;

        empty = false;
        Block* b = usedBlocks.back();

        void* entry = b->entries[b->size];

        b->size++;
        if (b->size == BLOCK_SIZE)
        {
            AddUsingBlock();
        }

        return entry;
    }

private:
    void AddUsingBlock()
    {
        if (freeBlocks.size() == 0)
        {
            Block b;
            b.entries = malloc(sizeof(TYPE) * BLOCK_SIZE);
            usedBlocks.push_back(std::move(b));
        }
        else
        {
            usedBlocks.push_back(std::move(freeBlocks[0]));
            freeBlocks.pop_front();
        }
    }

    void AddFreeBlock()
    {
        Block b;
        b.entries = malloc(sizeof(Block) * BLOCK_SIZE);
        freeBlocks.push_back(std::move(b));
    }

    //last entry is where we add stuff...
    std::list< Block > usedBlocks;
    std::list< Block > freeBlocks;
    bool empty = true;
    uint64_t initialCapacity;
    uint64_t numEntries = 0;
};

