#pragma once

#include <vector>
#include "HashTable.h"

template<typename UINT_HASH_TYPE, typename UINT_BAND_HASH_TYPE, uint32_t MAX_HASH_LEN>
class LSHBandHashMap
{
public:
    typedef std::vector< HashBlockEntry<UINT_HASH_TYPE, MAX_HASH_LEN>* > BucketHashPointerList;
    typedef std::vector< HashBlockEntry<UINT_HASH_TYPE, MAX_HASH_LEN>* >* BucketHashPointerListPtr;

    constexpr LSHBandHashMap(uint32_t _bands, uint32_t _bandSize)
        :bandSize(_bandSize),
        bands(_bands)
    {
    }

    constexpr uint32_t GetBandSize()
    {
        return bandSize;
    }

    constexpr uint32_t GetBands()
    {
        bands;
    }

    constexpr void Hash(UINT_HASH_TYPE* hash, uint32_t hashLen, UINT_BAND_HASH_TYPE* bandHashes)
    {
        for (uint32_t b = 0, hi = 0; b < hashLen / bandSize; b++, hi += bandSize)
            bandHashes[b] = hashBand(&hash[hi]);
    }

    constexpr UINT_BAND_HASH_TYPE hashBand(UINT_HASH_TYPE* hash)
    {
#pragma message("needs some work")
        UINT_BAND_HASH_TYPE bandHash = 0;
        //we want collisions, but not for everything. take one random bit from each hash for len...
        for (size_t i = 0; i < bandSize; ++i)
        {
            bandHash |= (UINT_BAND_HASH_TYPE)(hash[i] & (0xAA << i));//0xFF
            bandHash ^= (UINT_BAND_HASH_TYPE)(hash[i] ^ (0x55 << i));
            //bandHash = (UINT_BAND_HASH_TYPE)(hash[i] & (0xFFFFFFFF));
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
    uint32_t bands;
};
