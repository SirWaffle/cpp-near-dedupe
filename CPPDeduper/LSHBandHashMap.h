#pragma once

#include <vector>
#include <random>

#include "HashTable.h"

// rbls code pulled and modified from
// https://github.com/RSIA-LIESMARS-WHU/LSHBOX/blob/master/include/lshbox/lsh/rbslsh.h
//

#pragma message("LSHBandHashMap: LSH'ers and types need work... some only work with certain types")

template<typename UINT_HASH_TYPE>
class AbstractLSH
{
public:
    virtual void reset() {};
    virtual uint64_t getHashVal(const uint64_t* domin) { return 0; };
    virtual uint64_t getHashVal(const uint32_t* domin) { return 0; };
};


//only works with uint64_t for now
template<typename UINT_HASH_TYPE>
class FullCheckerLSH: public AbstractLSH< UINT_HASH_TYPE>
{
public:
    struct Parameter
    {
        uint32_t MAX_HASH_LEN;
        uint32_t bandSize;
        uint32_t buckets;

    };

    void Init(const Parameter& param_)
    {
        param = param_;
    }

    virtual void reset() final
    {
    }

    virtual uint64_t getHashVal(const uint32_t* hash) final
    {
        uint64_t bandHash = 0;

        //we want collisions, but not for everything. take one random bit from each hash for len...
        for (size_t i = 0; i < param.bandSize; ++i)
        {
            //TODO
            //these are all terrible. outside of 256, they are nearly garunteed not
            //to hash similar things into collisions
#           //*AND* the memory usgae for these buckets will be nuts unless they actually
            //hash down to a smaller value ( like 32bit value, or less )
            if (param.buckets < param.MAX_HASH_LEN / 16)
            {
                bandHash |= (hash[i] % param.buckets);
            }
            else if (param.buckets < param.MAX_HASH_LEN / 16)
            {  
                bandHash |= (hash[i] % 0x3) << (i * 2);
            }
            else if (param.buckets < param.MAX_HASH_LEN / 8)
            {
                bandHash |= (hash[i] % 0xF) << (i * 4);
            }
            else if (param.buckets == param.MAX_HASH_LEN / 4)
            {
                bandHash |= (hash[i] % 0xFF) << (i * 8);
            }
            else if (param.buckets == param.MAX_HASH_LEN / 2)
            {
                bandHash |= (hash[i] % 0xFFFF) << (i * 16);
            }
            else if (param.buckets == param.MAX_HASH_LEN)//full value copy - max buckets
            {
                bandHash = hash[i];
            }
        }

        return bandHash;
    }

    virtual uint64_t getHashVal(const uint64_t* hash) final
    {
        uint64_t bandHash = 0;

        //we want collisions, but not for everything. take one random bit from each hash for len...
        for (size_t i = 0; i < param.bandSize; ++i)
        {
            //TODO
            //these are all terrible. outside of 256, they are nearly garunteed not
            //to hash similar things into collisions
#           //*AND* the memory usgae for these buckets will be nuts unless they actually
            //hash down to a smaller value ( like 32bit value, or less )
            if (param.buckets < param.MAX_HASH_LEN / 16)
            {
                bandHash |= (hash[i] % 0x3) << (i);
            }
            else if (param.buckets < param.MAX_HASH_LEN / 16)
            {
                //what happens if we just modulo, and let things sort themselves out     
                bandHash |= (hash[i] % 0xF) << (i * 4);
                //bandHash |= (UINT_BAND_HASH_TYPE)(hash[i] & (0xAA << i));//0xFF
                //bandHash ^= (UINT_BAND_HASH_TYPE)(hash[i] ^ (0x55 << i));
            }
            else if (param.buckets < param.MAX_HASH_LEN / 8)
            {
                bandHash |= (hash[i] % 0xFF) << (i * 8);
            }
            else if (param.buckets == param.MAX_HASH_LEN / 4)
            {
                bandHash |= (hash[i] % 0xFFFF) << (i * 16);
            }
            else if (param.buckets == param.MAX_HASH_LEN / 2)
            {
                bandHash |= (hash[i] % 0xFFFFFFFF) << (i * 32);
            }
            else if (param.buckets == param.MAX_HASH_LEN)//full value copy - max buckets
            {
                bandHash = hash[i];
            }
        }

        return bandHash;
    }

private:
    Parameter param;
};


//only works with uint32_t for now / uint16_t
template<typename UINT_HASH_TYPE>
class rbsLsh : public AbstractLSH< UINT_HASH_TYPE>
{
    typedef uint16_t BIT_CHECK_SIZE;
    //uniform dist must be equal to or greater than bit check size
    typedef uint16_t UNIFORM_DISTRIBUTION_SIZE;

public:
    struct Parameter
    {
        /// Hash table size ( number is mod'd agaisnt this..and needed as length of incoming band size )
        uint32_t NumberOfBuckets;
        /// Dimension of the vector, it can be obtained from the instance of Matrix
        unsigned VectorLength;
        /// Binary code bytes
        unsigned NAlsoAppearsToBeVectorLength;
        /// The Difference between upper and lower bound of each dimension -- this one needs work
        uint32_t C;
    };

    void Init(const Parameter& param_)
    {
        param = param_;
    }

    virtual void reset() final
    {
        rndBits;
        rndArray.resize(param.NAlsoAppearsToBeVectorLength);
        std::mt19937 rng(unsigned(std::time(0)));

        //wont accept uint64_t as a type? downcasts to uint32_t?
        std::uniform_int_distribution<UNIFORM_DISTRIBUTION_SIZE> usBits(0, param.C);// param.D* param.C - 1);

        while (rndBits.size() < param.NAlsoAppearsToBeVectorLength)
        {
            UINT_HASH_TYPE target = usBits(rng);
            if (std::find(rndBits.begin(), rndBits.end(), target) == rndBits.end())
            {
                rndBits.push_back(target);
            }
        }
        std::sort(rndBits.begin(), rndBits.end());

        std::uniform_int_distribution<UNIFORM_DISTRIBUTION_SIZE> usArray(0, param.NumberOfBuckets - 1);
        for (auto iter = rndArray.begin(); iter != rndArray.end(); ++iter)
        {
           *iter = (BIT_CHECK_SIZE)usArray(rng);
        }
    }

    virtual uint64_t getHashVal(const uint64_t* hash) final
    {
        return getHashVal_16bit(reinterpret_cast<const BIT_CHECK_SIZE*>(hash));
    }

    virtual uint64_t getHashVal(const uint32_t* hash) final
    {
        return getHashVal_16bit(reinterpret_cast<const BIT_CHECK_SIZE*>(hash));
    }

    uint64_t getHashVal_16bit(const BIT_CHECK_SIZE* hash)
    {
        uint64_t sum(0), seq(0);
        for (auto it = rndBits.begin(); it != rndBits.end(); ++it)
        {
            UINT_HASH_TYPE rnd = (*it % param.C);
            UINT_HASH_TYPE toHash = hash[*it % param.VectorLength]; //domin[*it / param.C];
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
    Parameter param;
    std::vector<UINT_HASH_TYPE> rndBits;
    std::vector<uint64_t> rndArray;
};





template<typename UINT_HASH_TYPE, typename UINT_BAND_HASH_TYPE, uint32_t MAX_HASH_LEN>
class LSHBandHashMap
{
public:
    typedef std::vector< HashBlockEntry<UINT_HASH_TYPE, MAX_HASH_LEN>* > BucketHashPointerList;
    typedef std::unordered_map< UINT_BAND_HASH_TYPE, BucketHashPointerList > HashMap;

    enum LSH_TYPE_ENUM
    {
        ONLY_HASH_MAP,
        RANDOM_BIT
    };

private:
    std::vector< HashMap > hashMaps;
    uint32_t bandSize;
    uint32_t buckets;
    uint64_t numEntries;

    //TODO: needs work to work with uint64_t
    std::vector < AbstractLSH<UINT_BAND_HASH_TYPE>* > LSHHasher;

public:

    LSHBandHashMap(uint32_t _buckets, LSH_TYPE_ENUM lshTypeEnum)
        :bandSize(MAX_HASH_LEN / _buckets),
        buckets(_buckets),
        numEntries(0)
    {
        if (buckets * bandSize != MAX_HASH_LEN)
        {
            std::stringstream ss;
            ss << "*************  B: " << buckets << "  BS: " << bandSize << "  MHL:  " << MAX_HASH_LEN << std::endl;
            int* p = nullptr;
            *p = 10;
            //ss << "Invalid number of bands chosen, must be able to evenly divide the hashkey size";
            throw std::runtime_error(ss.str());
        }

        hashMaps.resize(buckets);
        LSHHasher.resize(buckets);

        for (auto& rbsLsher : LSHHasher)
        {
            if (lshTypeEnum == RANDOM_BIT)
            {
                //uniform distribution wont take uint64, so lets cast to uint32...
                typename rbsLsh<UINT_BAND_HASH_TYPE>::Parameter p;
                p.NumberOfBuckets = 256;
                p.VectorLength = bandSize * sizeof(UINT_HASH_TYPE) / sizeof(uint16_t); // MAX_HASH_LEN; //not sure
                p.NAlsoAppearsToBeVectorLength = bandSize * sizeof(UINT_HASH_TYPE) / sizeof(uint16_t); // sizeof(UINT_HASH_TYPE)* bandSize; //not sure
                p.C = UINT32_MAX; //not sure 

                auto rbs = new rbsLsh<UINT_BAND_HASH_TYPE>();
                rbs->Init(p);
                rbsLsher = rbs;
                
            }
            else if (lshTypeEnum == ONLY_HASH_MAP)
            {
                typename FullCheckerLSH<UINT_BAND_HASH_TYPE>::Parameter p;
                p.MAX_HASH_LEN = MAX_HASH_LEN;
                p.buckets = buckets;;
                p.bandSize = bandSize;

                auto rbs = new FullCheckerLSH<UINT_BAND_HASH_TYPE>();            
                rbs->Init(p);
                rbsLsher = rbs;
            }

            rbsLsher->reset();
        }
    }

    uint64_t GetNumEntries()
    {
        return numEntries;
    }

    uint64_t GetEstimatedMemoryUsageBytes()
    {
        return numEntries * sizeof(HashBlockEntry<UINT_HASH_TYPE, MAX_HASH_LEN>*);
    }

    uint32_t GetBandSize()
    {
        return bandSize;
    }

    uint32_t GetBuckets()
    {
        return buckets;
    }

    void Hash(UINT_HASH_TYPE* hash, uint32_t hashLen, std::vector<UINT_BAND_HASH_TYPE>::iterator bandHashes)
    {
        for (uint32_t b = 0, hi = 0; b < buckets; b++, hi += bandSize, ++bandHashes)
            *bandHashes = hashBand(b, &hash[hi]);
    }

    void AddToMap(std::vector<UINT_BAND_HASH_TYPE>::iterator bandHashes, HashBlockEntry<UINT_HASH_TYPE, MAX_HASH_LEN>* hashEntry)
    {
        for (size_t bk = 0; bk < buckets; ++bk, ++bandHashes)
        {
            AddToMap(bk, *bandHashes, hashEntry);
        }
    }


    void AddToMap(size_t bucketNum, UINT_BAND_HASH_TYPE bandHash, HashBlockEntry<UINT_HASH_TYPE, MAX_HASH_LEN>* hashEntry)
    {
        numEntries++;

        auto it = hashMaps[bucketNum].find(bandHash);
        if (it == hashMaps[bucketNum].end())
        {
            auto list = BucketHashPointerList();
            list.reserve(4096);
            list.push_back(hashEntry);
            hashMaps[bucketNum].insert({ bandHash, list });
        }
        else
        {
            it->second.push_back(hashEntry);
        }
    }

    size_t GetCollided(std::vector<UINT_BAND_HASH_TYPE>::iterator bandHashes, std::vector< std::vector< HashBlockEntry<UINT_HASH_TYPE, MAX_HASH_LEN>* >* >& matches)
    {
        size_t totalPotentialCandidates = 0;
        for (uint32_t b = 0; b < buckets; b++, bandHashes++)
        {
            auto matched = GetCollided(b, *bandHashes);
            if (matched != nullptr)
            {
                totalPotentialCandidates += matched->size();
                matches.push_back(matched);
            }
        }

        return totalPotentialCandidates;
    }

    BucketHashPointerList* GetCollided(size_t bucketNum, UINT_BAND_HASH_TYPE bandHash)
    {
        auto it = hashMaps[bucketNum].find(bandHash);
        if (it == hashMaps[bucketNum].end())
            return nullptr;
        return &(it->second);
    }

private:

    UINT_BAND_HASH_TYPE hashBand(size_t bucketNum, const UINT_HASH_TYPE* hash)
    {
        return LSHHasher[bucketNum]->getHashVal(hash);
    }

};
