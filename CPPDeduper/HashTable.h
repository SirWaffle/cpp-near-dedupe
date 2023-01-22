#pragma once

#include <vector>

//used to allocate known length arrays for holding hashes

template<typename UINT_HASH_TYPE, uint32_t HASH_COUNT>
struct alignas(64) HashBlockEntry
{
    uint64_t flags[2]; //used when searching to avoid testing against the same item more than once
    UINT_HASH_TYPE hashes[HASH_COUNT];
    uint64_t hashLen = 0;
};

template<typename UINT_HASH_TYPE, uint32_t HASH_COUNT, uint32_t BLOCK_SIZE>
struct Block
{
    UINT_HASH_TYPE size = 0;
    HashBlockEntry<UINT_HASH_TYPE, HASH_COUNT> entries[BLOCK_SIZE];
};


//just test with full blocks for now
template<typename UINT_HASH_TYPE, uint32_t MAX_HASH_LEN, uint32_t BLOCK_SIZE>
class HashBlockAllocator
{
    //last entry is where we add stuff...
    std::list< Block<UINT_HASH_TYPE, MAX_HASH_LEN, BLOCK_SIZE>* > fullBlocks;
    bool empty = true;
    uint64_t initialCapacity;

public:
    HashBlockAllocator(uint64_t _initialCapacity)
        :initialCapacity(_initialCapacity)
    {
        //reserve and add first block to fill
        fullBlocks.push_back(new Block<UINT_HASH_TYPE, MAX_HASH_LEN, BLOCK_SIZE>());
    }

    ~HashBlockAllocator()
    {
        for (auto block : fullBlocks)
            delete block;
    }

    uint64_t NumEntries()
    {
        return uint64_t(fullBlocks.size() * BLOCK_SIZE) - BLOCK_SIZE + fullBlocks.back()->size;
    }

    size_t NumBlocks()
    {
        return fullBlocks.size();
    }

    uint64_t MemoryUsage()
    {
        uint64_t items = uint64_t(fullBlocks.size() * BLOCK_SIZE) - BLOCK_SIZE + fullBlocks.back()->size;
        items *= MAX_HASH_LEN;
        return items * (uint64_t)sizeof(UINT_HASH_TYPE);
    }

    bool IsEmpty()
    {
        return empty;
    }

    HashBlockEntry<UINT_HASH_TYPE, MAX_HASH_LEN>* AddItem(UINT_HASH_TYPE* hashes, uint32_t len)
    {
        empty = false;
        Block<UINT_HASH_TYPE, MAX_HASH_LEN, BLOCK_SIZE>* b = fullBlocks.back();

#ifdef __GNUC__
        memcpy(&(b->entries[b->size].hashes), hashes, len * sizeof(UINT_HASH_TYPE));
#else
        memcpy_s(&(b->entries[b->size].hashes), MAX_HASH_LEN * sizeof(UINT_HASH_TYPE), hashes, len * sizeof(UINT_HASH_TYPE));
#endif

        HashBlockEntry<UINT_HASH_TYPE, MAX_HASH_LEN>* entry = &(b->entries[b->size]);
        b->entries[b->size].hashLen = len;

        b->size++;
        if (b->size == BLOCK_SIZE)
        {
            fullBlocks.push_back(new Block<UINT_HASH_TYPE, MAX_HASH_LEN, BLOCK_SIZE>());
        }

        return entry;
    }
};

