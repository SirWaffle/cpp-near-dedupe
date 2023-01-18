#pragma once

// Code used / adapted from
//  https://gudok.xyz/minhash1/
//  https://github.com/andreigudkov/articles

#include "xxhash.h"

#include <string>
#include <stdexcept>
#include <vector>
#include <chrono>
#include <iostream>
#include "isalphanum.h"


typedef std::basic_string<uint16_t> U16String;

#pragma message("need to validate hashtable64 bit")
class HashTable64 {
public:
    HashTable64(int minCapacity) :
        kBits(log2ceil(minCapacity)),
        kSize(1 << kBits),
        kMask(kSize - 1),
        size_(0),
        data_(kSize, kEmpty),
        empty_(false)
    {
    }

    uint32_t size() const {
        return size_;
    }

    uint32_t capacity() const {
        return kSize;
    }

    void Clear() {
        int32_t value = kEmpty;
        std::fill(data_.begin(), data_.end(), value);
        empty_ = false;
        size_ = 0;
    }

    void InsertUnsafe(uint64_t v) {
        // assert(!contains(v))
        size_++;
        if (v == kEmpty) [[unlikely]]
        {
            empty_ = true;
            return;
        }
        uint32_t pos = (v & kMask);
        for (size_t i = pos; i < kSize; i++) {
            if (data_[i] == kEmpty) {
                data_[i] = v;
                return;
            }
        }
        for (size_t i = 0; i < pos; i++) {
            if (data_[i] == kEmpty) {
                data_[i] = v;
                return;
            }
        }
    }

    void Dump() const {
        for (uint32_t i = 0; i < kSize; i++) {
            std::cout << "[" << i << "] " << (int32_t)data_[i] << ' ' << (data_[i] & kMask) << std::endl;
        }
    }

    bool Contains(uint64_t v) const {
        if (v == kEmpty) [[unlikely]]
        {
            return empty_;
        }
        uint32_t pos = (v & kMask);
        for (size_t i = pos; i < kSize; i++) {
            if (data_[i] == v) {
                return true;
            }
            else if (data_[i] == kEmpty) {
                return false;
            }
        }
        for (size_t i = 0; i < pos; i++) {
            if (data_[i] == v) {
                return true;
            }
            else if (data_[i] == kEmpty) {
                return false;
            }
        }
        return false;
    }

private:
    static int log2ceil(int n) {
        int res = 0;
        while ((1 << res) < n) {
            res++;
        }
        return res;
    }

    static constexpr uint32_t kEmpty{ 43 }; //0
    const uint32_t kBits;
    const uint32_t kSize;
    const uint32_t kMask;

    uint32_t size_;
    std::vector<uint64_t> data_;
    bool empty_; // contains kEmpty as element
};


#pragma message("need to validate shingler64 bit")
template<int K, int N>
class Pipe3Shingler64 {
public:
    static constexpr int kHashLength = K;
    static constexpr int kNumHashes = N;

    XXH64_hash_t xxhseed = 0;

    Pipe3Shingler64(XXH64_hash_t _xxhseed) :
        htable_(N * 8),
        xxhseed(_xxhseed)
    {
    }

    static std::string name() {
        std::ostringstream str;
        str << "Pipe3Shingler<" << K << ',' << N << '>';
        return str.str();
    }

    void Process(const uint16_t* txt, size_t txt_length, uint64_t* fng, int* fng_length)
    {
        size_t i = 0;
        *fng_length = 0;
        XXH64_state_t* xxh64states[K];


        auto SkipDelim = [&]() {
            while (i < txt_length && !IsAlphanum(txt[i])) {
                i++;
            }
        };

        auto ReadWord = [&]() {
            while (i < txt_length && IsAlphanum(txt[i])) {
                uint16_t c = txt[i];
                for (int k = 0; k < K; k++) {
                    if(XXH64_update(xxh64states[k], &txt[i], sizeof(uint16_t)) == XXH_ERROR)
                        abort();
                }
                i++;
            }
        };

        auto ShiftPipeline = [&]() {
            //cycle states..
            XXH64_state_t* front = xxh64states[0];

            for (int k = 0; k < K - 1; k++) {
                xxh64states[k] = xxh64states[k + 1];
            }
            xxh64states[K - 1] = front;
            if (XXH64_reset(xxh64states[K - 1], xxhseed) == XXH_ERROR) 
                abort();
        };

        // preload pipeline
        htable_.Clear();
        SkipDelim();
        for (int k = 0; k < K; k++) {
            xxh64states[k] = XXH64_createState();
            if (XXH64_reset(xxh64states[k], xxhseed) == XXH_ERROR) 
                abort();
        }
        for (int k = 0; k < K - 1; k++) {
            ReadWord();
            ShiftPipeline();
            SkipDelim();
        }

        // collect first N hashes
        while (i < txt_length && *fng_length < N) {
            ReadWord();

            uint64_t h = XXH64_digest(xxh64states[0]);

            if (!htable_.Contains(h)) {
                fng[(*fng_length)++] = h;
                htable_.InsertUnsafe(h);

                if (htable_.size() >= htable_.capacity() / 2) {
                    htable_.Clear();
                    for (int j = 0; j < *fng_length; j++) {
                        htable_.InsertUnsafe(fng[j]);
                    }
                }
            }

            ShiftPipeline();

            SkipDelim();
        }

        if (i >= txt_length) {
            std::sort(fng, fng + *fng_length);
            return;
        }

        // process remaining hashes
        std::make_heap(fng, fng + *fng_length);
        while (i < txt_length) {
            ReadWord();

            uint64_t h = XXH64_digest(xxh64states[0]);

            if (h < fng[0]) {
                if (!htable_.Contains(h)) {
                    htable_.InsertUnsafe(h);
                    std::pop_heap(fng, fng + N);
                    fng[N - 1] = h;
                    std::push_heap(fng, fng + N);

                    if (htable_.size() >= htable_.capacity() / 2) {
                        htable_.Clear();
                        for (size_t j = 0; j < N; j++) {
                            htable_.InsertUnsafe(fng[j]);
                        }
                    }
                }
            }

            ShiftPipeline();

            SkipDelim();
        }
        std::sort_heap(fng, fng + *fng_length);

        for (int k = 0; k < K - 1; k++) {
            XXH64_freeState(xxh64states[k]);
        }
    }

private:
    HashTable64 htable_;
};