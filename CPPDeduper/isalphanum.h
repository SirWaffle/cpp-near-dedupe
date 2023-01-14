#pragma once
#include <cstdint>


extern alignas(64) const uint64_t kAlphanumTable[];

inline bool IsAlphanum(unsigned int c) {
  return (kAlphanumTable[c/64] >> (c%64)) & 0x1;
}

