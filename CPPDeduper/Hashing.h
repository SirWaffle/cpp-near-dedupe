#pragma once


// Code used / adapted from
//  https://gudok.xyz/minhash1/
//  https://github.com/andreigudkov/articles


#include <string>
#include <stdexcept>
#include <vector>
#include <chrono>

#include "isalphanum.h"

static inline void Die(const std::string& msg) {
    throw std::runtime_error(msg);
}

typedef std::basic_string<uint16_t> U16String;

class HashTable {
public:
    HashTable(int minCapacity) :
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

    void InsertUnsafe(uint32_t v) {
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

    bool Contains(uint32_t v) const {
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
    std::vector<uint32_t> data_;
    bool empty_; // contains kEmpty as element
};



template<int K, int N>
class Pipe3Shingler {
public:
    static constexpr int kHashLength = K;
    static constexpr int kNumHashes = N;

    Pipe3Shingler() :
        htable_(N * 8)
    {
    }

    static std::string name() {
        std::ostringstream str;
        str << "Pipe3Shingler<" << K << ',' << N << '>';
        return str.str();
    }

    void Process(const uint16_t* txt, size_t txt_length, uint32_t* fng, int* fng_length)
    {
        uint32_t hashes[K];
        size_t i = 0;
        *fng_length = 0;

        auto SkipDelim = [&]() {
            while (i < txt_length && !IsAlphanum(txt[i])) {
                i++;
            }
        };

//#pragma warning( push )
//#pragma warning( disable : 4333)
        auto ReadWord = [&]() {
            while (i < txt_length && IsAlphanum(txt[i])) {
                uint16_t c = txt[i];
                for (int k = 0; k < K; k++) {
                    hashes[k] = hashes[k] ^ (c & 0xff);
                    hashes[k] = hashes[k] * 16777619;

                    //TODO: usually with hashing this is fine, perhaps something is off though, def. verify
                    // //UB optimization maybe? count is off in release mode
                    //warning C4333: '>>': right shift by too large amount, data loss
                    //hashes[k] = hashes[k] ^ (c >> 16);
#pragma message("figure out hashing warning")
                    hashes[k] = hashes[k] ^ (c << 16);

                    hashes[k] = hashes[k] * 16777619;
                }
                i++;
            }
        };
//#pragma warning( pop ) 

        auto ShiftPipeline = [&]() {
            for (int k = 0; k < K - 1; k++) {
                hashes[k] = hashes[k + 1];
            }
            hashes[K - 1] = 2166136261;
        };

        // preload pipeline
        htable_.Clear();
        SkipDelim();
        for (int k = 0; k < K; k++) {
            hashes[k] = 2166136261;
        }
        for (int k = 0; k < K - 1; k++) {
            ReadWord();
            ShiftPipeline();
            SkipDelim();
        }

        // collect first N hashes
        while (i < txt_length && *fng_length < N) {
            ReadWord();

            uint32_t h = hashes[0];
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

            uint32_t h = hashes[0];
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
    }

private:
    HashTable htable_;
};


//faster if we use const values for ahshing
template<int HASH_LEN_SHINGLES, int NUM_HASHES>
void MakeFingerprint(const U16String& str, std::unique_ptr<uint32_t[]>* hashes)
{
    Pipe3Shingler<HASH_LEN_SHINGLES, NUM_HASHES> shingler;
    hashes->reset(new uint32_t[decltype(shingler)::kNumHashes]);
    uint32_t* ptr = hashes->get();
    int len;
    shingler.Process(str.data(), str.length(), ptr, &len);
    ptr += len;
}

void CharPtrToUStr(const char* src, size_t srcLen, U16String& dst) {
    auto require = [&src, &srcLen](uint32_t offset, uint32_t count) -> void {
        if (offset + count > srcLen) {
            Die("String too short");
        }
        for (uint32_t i = 0; i < count; i++) {
            char c = src[offset + i];
            if ((c & 0xc0) != 0x80) {
                Die("Illegal utf8 sequence");
            }
        }
    };
    for (uint32_t i = 0; i < srcLen; ) {
        uint8_t c = (uint8_t)src[i];
        if ((c & 0x80) == 0) {
            // 0xxxxxxx
            dst += (uint16_t)(c);
            i++;
        }
        else if ((c & 0xe0) == 0xc0) {
            // 110xxxxx 10xxxxxx
            require(i + 1, 1);
            uint16_t c16 = 0;
            c16 |= ((uint16_t)(src[i + 0] & 0x1f)) << 6;
            c16 |= ((uint16_t)(src[i + 1] & 0x3f));
            dst += c16;
            i += 2;
        }
        else if ((c & 0xf0) == 0xe0) {
            // 1110xxxx 10xxxxxx 10xxxxxx
            require(i + 2, 1);
            uint16_t c16 = 0;
            c16 |= ((uint16_t)(src[i + 0] & 0x0f)) << 12;
            c16 |= ((uint16_t)(src[i + 1] & 0x3f)) << 6;
            c16 |= ((uint16_t)(src[i + 2] & 0x3f));
            dst += c16;
            i += 3;
        }
        else {
            dst += (uint16_t)' ';
            i++;
        }
    }
}
/*
void ArrowBuffToUStr(const uint8_t* src, size_t srcLen, U16String& dst) {
    auto require = [&src, &srcLen](int offset, int count) -> void {
        if (offset + count > srcLen) {
            Die("String too short");
        }
        for (int i = 0; i < count; i++) {
            char c = src[offset + i];
            if ((c & 0xc0) != 0x80) {
                Die("Illegal utf8 sequence");
            }
        }
    };
    for (int i = 0; i < srcLen; ) {
        uint8_t c = (uint8_t)src[i];
        if ((c & 0x80) == 0) {
            // 0xxxxxxx
            dst += (uint16_t)(c);
            i++;
        }
        else if ((c & 0xe0) == 0xc0) {
            // 110xxxxx 10xxxxxx
            require(i + 1, 1);
            uint16_t c16 = 0;
            c16 |= ((uint16_t)(src[i + 0] & 0x1f)) << 6;
            c16 |= ((uint16_t)(src[i + 1] & 0x3f));
            dst += c16;
            i += 2;
        }
        else if ((c & 0xf0) == 0xe0) {
            // 1110xxxx 10xxxxxx 10xxxxxx
            require(i + 2, 1);
            uint16_t c16 = 0;
            c16 |= ((uint16_t)(src[i + 0] & 0x0f)) << 12;
            c16 |= ((uint16_t)(src[i + 1] & 0x3f)) << 6;
            c16 |= ((uint16_t)(src[i + 2] & 0x3f));
            dst += c16;
            i += 3;
        }
        else {
            dst += (uint16_t)' ';
            i++;
        }
    }
}



void StrToUStr(const std::string& src, U16String& dst) {
    auto require = [&src](int offset, int count) -> void {
        if (offset + count > src.length()) {
            Die("String too short");
        }
        for (int i = 0; i < count; i++) {
            char c = src[offset + i];
            if ((c & 0xc0) != 0x80) {
                Die("Illegal utf8 sequence");
            }
        }
    };
    for (int i = 0; i < src.length(); ) {
        uint8_t c = (uint8_t)src[i];
        if ((c & 0x80) == 0) {
            // 0xxxxxxx
            dst += (uint16_t)(c);
            i++;
        }
        else if ((c & 0xe0) == 0xc0) {
            // 110xxxxx 10xxxxxx
            require(i + 1, 1);
            uint16_t c16 = 0;
            c16 |= ((uint16_t)(src[i + 0] & 0x1f)) << 6;
            c16 |= ((uint16_t)(src[i + 1] & 0x3f));
            dst += c16;
            i += 2;
        }
        else if ((c & 0xf0) == 0xe0) {
            // 1110xxxx 10xxxxxx 10xxxxxx
            require(i + 2, 1);
            uint16_t c16 = 0;
            c16 |= ((uint16_t)(src[i + 0] & 0x0f)) << 12;
            c16 |= ((uint16_t)(src[i + 1] & 0x3f)) << 6;
            c16 |= ((uint16_t)(src[i + 2] & 0x3f));
            dst += c16;
            i += 3;
        }
        else {
            dst += (uint16_t)' ';
            i++;
        }
    }
}*/

