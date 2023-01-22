#pragma once

#include <vector>
#include <random>

#include "HashTable.h"

// rbls code pulled and modified from
// https://github.com/RSIA-LIESMARS-WHU/LSHBOX/blob/master/include/lshbox/lsh/rbslsh.h
//

struct rbsParameter
{
    /// Hash table size ( number is mod'd agaisnt this..and needed as length of incoming band size )
    uint64_t NumberOfBuckets;
    /// Dimension of the vector, it can be obtained from the instance of Matrix
    unsigned VectorLength;
    /// Binary code bytes
    unsigned NAlsoAppearsToBeVectorLength;
    /// The Difference between upper and lower bound of each dimension -- this one needs work
    uint32_t C;
};

template<typename UINT_HASH_TYPE>
class rbsLsh
{
public:
    void reset(const rbsParameter& param_)
    {
        param = param_;
        rndBits;
        rndArray.resize(param.NAlsoAppearsToBeVectorLength);
        std::mt19937 rng(unsigned(std::time(0)));

        //wont accept uint64_t as a type? downcasts to uint32_t?
        std::uniform_int_distribution<UINT_HASH_TYPE> usBits(0, param.C);// param.D* param.C - 1);

        while (rndBits.size() < param.NAlsoAppearsToBeVectorLength)
        {
            UINT_HASH_TYPE target = usBits(rng);
            if (std::find(rndBits.begin(), rndBits.end(), target) == rndBits.end())
            {
                rndBits.push_back(target);
            }
        }
        std::sort(rndBits.begin(), rndBits.end());

        std::uniform_int_distribution<uint64_t> usArray(0, param.NumberOfBuckets - 1);
        for (auto iter = rndArray.begin(); iter != rndArray.end(); ++iter)
        {
           *iter = usArray(rng);
        }
    }

    uint64_t getHashVal(const UINT_HASH_TYPE* domin)
    {
        uint64_t sum(0), seq(0);
        for (auto it = rndBits.begin(); it != rndBits.end(); ++it)
        {
            UINT_HASH_TYPE rnd = (*it % param.C);
            UINT_HASH_TYPE toHash = domin[*it % param.VectorLength]; //domin[*it / param.C];
            if (rnd <= toHash)
            {
                sum += rndArray[seq];
            }
            ++seq;
        }
        uint64_t hashVal = sum % param.NumberOfBuckets;
        return hashVal;
    }

private:
    rbsParameter param;
    std::vector<UINT_HASH_TYPE> rndBits;
    std::vector<uint64_t> rndArray;
};





template<typename UINT_HASH_TYPE, typename UINT_BAND_HASH_TYPE, uint32_t MAX_HASH_LEN>
class LSHBandHashMap
{
public:
    typedef std::vector< HashBlockEntry<UINT_HASH_TYPE, MAX_HASH_LEN>* > BucketHashPointerList;
   // typedef std::vector< HashBlockEntry<UINT_HASH_TYPE, MAX_HASH_LEN>* >* BucketHashPointerListPtr;

    //TODO: needs work to work with uint64_t
    rbsLsh<uint16_t> rbsLsher;

    constexpr LSHBandHashMap(uint32_t _buckets)
        :bandSize(MAX_HASH_LEN / _buckets),
        buckets(_buckets)
    {
        if (buckets * bandSize != MAX_HASH_LEN)
        {
            std::string errorMessage = "Invalid number of bands chosen, must be able to evenly divide the hashkey size";
            throw std::runtime_error(errorMessage);
        }

        //uniform distribution wotn take uint64, so lets cast to uint32...
        rbsParameter p;
        p.NumberOfBuckets = 256;
        p.VectorLength = bandSize * sizeof(UINT_HASH_TYPE) / sizeof(uint16_t); // MAX_HASH_LEN; //not sure
        p.NAlsoAppearsToBeVectorLength = bandSize * sizeof(UINT_HASH_TYPE) / sizeof(uint16_t); // sizeof(UINT_HASH_TYPE)* bandSize; //not sure
        p.C = UINT32_MAX; //not sure 
        rbsLsher.reset(p);
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

    UINT_BAND_HASH_TYPE hashBand(uint64_t* hash, uint32_t*)
    {
#pragma message("LSHBandHashMap:  u32 & u32 not implemented")
        std::string errorMessage = "LSHBandHashMap:  u32 & u32 not implemented";
        throw std::runtime_error(errorMessage);
    }

    UINT_BAND_HASH_TYPE hashBand(uint64_t* hash, uint64_t*)
    {
        return rbsLsher.getHashVal(reinterpret_cast<uint16_t*>(hash));
    }

    /*
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
    }*/

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
