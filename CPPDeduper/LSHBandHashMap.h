#pragma once

#include <vector>
#include "HashTable.h"
/*
uint32_t fnv1a(const uint16_t* str, int len) {
    uint32_t hash = 2166136261;
    for (size_t i = 0; i < len; i++) {
        uint16_t c = str[i];
        hash = hash ^ (c & 0xff);
        hash = hash * 16777619;
        hash = hash ^ (c >> 16);
        hash = hash * 16777619;
    }
    return hash;
}*/

template<typename UINT_HASH_TYPE, typename UINT_BAND_HASH_TYPE, uint32_t MAX_HASH_LEN>
class LSHBandHashMap
{
public:
    typedef std::vector< HashBlockEntry<UINT_HASH_TYPE, MAX_HASH_LEN>* > BucketHashPointerList;
   // typedef std::vector< HashBlockEntry<UINT_HASH_TYPE, MAX_HASH_LEN>* >* BucketHashPointerListPtr;

    constexpr LSHBandHashMap(uint32_t _buckets)
        :bandSize(MAX_HASH_LEN / _buckets),
        buckets(_buckets)
    {
        if (buckets * bandSize != MAX_HASH_LEN)
        {
            std::string errorMessage = "Invalid number of bands chosen, must be able to evenly divide the hashkey size";
            throw std::runtime_error(errorMessage);
        }
    }

    constexpr uint32_t GetBandSize()
    {
        return bandSize;
    }

    constexpr uint32_t GetBuckets()
    {
        return buckets;
    }

    void Hash(UINT_HASH_TYPE* hash, uint32_t hashLen, UINT_BAND_HASH_TYPE* bandHashes)
    {
        for (uint32_t b = 0, hi = 0; b < hashLen / bandSize; b++, hi += bandSize)
            bandHashes[b] = hashBand(&hash[hi], bandHashes);
    }

    UINT_BAND_HASH_TYPE hashBand(uint32_t* hash, uint32_t*)
    {
#pragma message("LSHBandHashMap: u32 & u32 not implemented")
        std::string errorMessage = "LSHBandHashMap: u32 & u32 not implemented";
        throw std::runtime_error(errorMessage);
    }

    UINT_BAND_HASH_TYPE hashBand(uint32_t* hash, uint64_t*)
    {
#pragma message("LSHBandHashMap:  u32 & u64 not implemented")
        std::string errorMessage = "LSHBandHashMap:  u32 & u64 not implemented";
        throw std::runtime_error(errorMessage);
    }

    UINT_BAND_HASH_TYPE hashBand(uint64_t* hash, uint64_t * )
    {
#pragma message("LSHBandHashMap: hash algorithm for hashing large bands into buckets is wrong and needs work")
        UINT_BAND_HASH_TYPE bandHash = 0;

        //we want collisions, but not for everything. take one random bit from each hash for len...
        for (size_t i = 0; i < bandSize; ++i)
        {
            //TODO
            //these are all terrible. outside of 256, they are nearly garunteed not
            //to hash similar things into collisions
#           //*AND* the memory usgae for these buckets will be nuts unless they actually
            //hash down to a smaller value ( like 32bit value, or less )
            if (buckets < MAX_HASH_LEN / 16)
            {
                bandHash |= (hash[i] % 0x3) << (i);
            }
            else if (buckets < MAX_HASH_LEN / 16)
            {
                //what happens if we just modulo, and let things sort themselves out     
                bandHash |= (hash[i] % 0xF) << (i * 4);
                //bandHash |= (UINT_BAND_HASH_TYPE)(hash[i] & (0xAA << i));//0xFF
                //bandHash ^= (UINT_BAND_HASH_TYPE)(hash[i] ^ (0x55 << i));
            }
            else if(buckets < MAX_HASH_LEN / 8)
            {
                bandHash |= (hash[i] % 0xFF) << (i * 8);
            }
            else if (buckets == MAX_HASH_LEN / 4)
            {
                bandHash |= (hash[i] % 0xFFFF) << (i * 16);
            }
            else if (buckets == MAX_HASH_LEN / 2)
            {
                bandHash |= (hash[i] % 0xFFFFFFFF) << (i * 32);                
            }
            else if(buckets == MAX_HASH_LEN)//full value copy - max buckets
            {
                bandHash = (UINT_BAND_HASH_TYPE)(hash[i]);
            }
        }

        return bandHash;
    }

    UINT_BAND_HASH_TYPE hashBand(uint64_t* hash, uint32_t*)
    {
#pragma message("LSHBandHashMap: hash algorithm for hashing large bands into buckets is wrong and needs work")
        UINT_BAND_HASH_TYPE bandHash = 0;

        //we want collisions, but not for everything. take one random bit from each hash for len...
        for (size_t i = 0; i < bandSize; ++i)
        {
            if (buckets < MAX_HASH_LEN / 16)
            {
                bandHash |= (hash[i] % 0x3) << (i);
            }
            else if (buckets < MAX_HASH_LEN / 8)
            {
                bandHash |= (hash[i] % 0x3) << (i * 4);
            }
            else if (buckets == MAX_HASH_LEN / 4)
            {
                bandHash |= (hash[i] % 0xFF) << (i * 8);
            }
            else if (buckets == MAX_HASH_LEN / 2)
            {
                bandHash |= (hash[i] % 0xFFFF) << (i * 16);
            }
            else if (buckets == MAX_HASH_LEN)
            {
                bandHash |= (hash[i] % 0xFFFFFFFF);
            }
        }

        return bandHash;
    }

    constexpr void AddToMap(UINT_BAND_HASH_TYPE bandHash, HashBlockEntry<UINT_HASH_TYPE, MAX_HASH_LEN>* hashEntry)
    {
        auto it = map.find(bandHash);
        if (it == map.end())
        {
            auto list = BucketHashPointerList();
            list.reserve(4096);
            list.push_back(hashEntry);
            map.insert({ bandHash, list });
        }
        else
        {
            it->second.push_back(hashEntry);
        }
    }

    constexpr BucketHashPointerList* GetCollided(UINT_BAND_HASH_TYPE bandHash)
    {
        auto it = map.find(bandHash);
        if (it == map.end())
            return nullptr;
        return &(it->second);
    }

private:
    std::unordered_map< UINT_BAND_HASH_TYPE, BucketHashPointerList > map;
    uint32_t bandSize;
    uint32_t buckets;
};
