#pragma once

#include <list>
#include <vector>

//used to allocate known length arrays for holding hashes




//just test with full blocks for now
template<typename TYPE, uint32_t BLOCK_SIZE>
class ChunkList
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
    ChunkList(uint64_t _initialCapacity)
        :initialCapacity(_initialCapacity),
        numEntries(0)
    {
        AddBlock();
    }

    ~ChunkList()
    {
        for (auto block : fullBlocks)
            delete block;
    }

    uint64_t NumEntries()
    {
        return numEntries;
    }

    size_t NumBlocks()
    {
        return fullBlocks.size();
    }

    iterator Begin()
    {
        return fullBlocks.begin();
    }

    iterator End()
    {
        return fullBlocks.end();
    }

    uint64_t MemoryUsage()
    {
        uint64_t items = uint64_t(fullBlocks.size() * BLOCK_SIZE) - BLOCK_SIZE + fullBlocks.back()->size;
        return items * (uint64_t)sizeof(TYPE);
    }

    bool IsEmpty()
    {
        return empty;
    }

    TYPE* AddItem()
    {
        ++numEntries;

        empty = false;
        Block* b = fullBlocks.back();

        TYPE* entry = new (b->entries[b->size]) TYPE();

        b->size++;
        if (b->size == BLOCK_SIZE)
        {
            AddBlock();
        }

        return entry;
    }

    void AddBlock()
    {
        auto* b = new Block();
        b->entries = malloc(sizeof(Block) * BLOCK_SIZE);
        fullBlocks.push_back(b);
    }

private:
    //last entry is where we add stuff...
    std::list< Block* > fullBlocks;
    bool empty = true;
    uint64_t initialCapacity;
    uint64_t numEntries = 0;
};

