#pragma once


// Code used / adapted from
//  https://gudok.xyz/minhash1/
//  https://github.com/andreigudkov/articles

#include "xxhash.h"

#include "Shingler32.hpp"
#include "Shingler64.hpp"

#include <string>
#include <stdexcept>
#include <vector>
#include <chrono>

#include "isalphanum.h"


static inline void Die(const std::string& msg) {
    throw std::runtime_error(msg);
}

//faster if we use const values for hshing
template<int HASH_LEN_SHINGLES, int NUM_HASHES>
int MakeFingerprint(const U16String& str, std::unique_ptr<uint32_t[]>* hashes)
{
    Pipe3Shingler32<HASH_LEN_SHINGLES, NUM_HASHES> shingler;
    hashes->reset(new uint32_t[decltype(shingler)::kNumHashes]);
    uint32_t* ptr = hashes->get();
    int len = 0;
    shingler.Process(str.data(), str.length(), ptr, &len);
    return len;
}

//faster if we use const values for hashing
template<int HASH_LEN_SHINGLES, int NUM_HASHES>
int MakeFingerprint(const U16String& str, std::unique_ptr<uint64_t[]>* hashes)
{
    Pipe3Shingler64<HASH_LEN_SHINGLES, NUM_HASHES> shingler(0);
    hashes->reset(new uint64_t[decltype(shingler)::kNumHashes]);
    uint64_t* ptr = hashes->get();
    int len = 0;
    shingler.Process(str.data(), str.length(), ptr, &len);
    return len;
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

