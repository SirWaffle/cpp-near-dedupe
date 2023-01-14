#ifdef DONOTMAKE

#include "shingle.h"
#include <string>
#include <vector>
#include <cstdint>
#include <memory>
#include <iostream>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <functional>
#include <immintrin.h>
#include <emmintrin.h>
#include <algorithm>
#include <queue>
#include <unordered_set>
#include <set>
#include <fstream>
#include <sstream>
#include <cmath>



void Uconv(const std::string& src, String& dst) {
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
    uint8_t c = (uint8_t) src[i];
    if ((c & 0x80) == 0) {
      // 0xxxxxxx
      dst += (uint16_t)(c);
      i++;
    } else if ((c & 0xe0) == 0xc0) {
      // 110xxxxx 10xxxxxx
      require(i+1, 1);
      uint16_t c16 = 0;
      c16 |= ((uint16_t)(src[i+0] & 0x1f)) << 6;
      c16 |= ((uint16_t)(src[i+1] & 0x3f));
      dst += c16;
      i += 2;
    } else if ((c & 0xf0) == 0xe0) {
      // 1110xxxx 10xxxxxx 10xxxxxx
      require(i+2, 1);
      uint16_t c16 = 0;
      c16 |= ((uint16_t)(src[i+0] & 0x0f)) << 12;
      c16 |= ((uint16_t)(src[i+1] & 0x3f)) << 6;
      c16 |= ((uint16_t)(src[i+2] & 0x3f));
      dst += c16;
      i += 3;
    } else {
      dst += (uint16_t)' ';
      i++;
    }
  }
}

void ReadLines(const std::string& path, std::vector<std::string>* lines) {
  std::ifstream in;
  in.open(path.c_str(), std::ios::in);
  if (in.fail()) {
    Die("Failed to open file");
  }

  std::string str;
  while (true) {
    std::getline(in, str);
    if (in.eof()) {
      break;
    }
    if (in.fail()) {
      Die("Failed to read from file");
    }
    lines->push_back(str);
  }
}



/*
void split_naive(const std::string& text, std::vector<uint32_t>* ngrams) {
  if (text.length() < 3) {
    return;
  }
  ngrams->reserve(text.length()-2);
  for (size_t off = 0; off < text.length() - 2; off++) {
    uint32_t ngram = 0;
    ngram |= (uint8_t)text[off+0] << 16;
    ngram |= (uint8_t)text[off+1] << 8;
    ngram |= (uint8_t)text[off+2] << 0;
    ngrams->push_back(ngram);
  }
}
*/

/*
void split_pipeline(const std::string& text, std::vector<uint32_t>* ngrams) {
  if (text.length() < 3) {
    return;
  }
  ngrams->reserve(text.length()-2);
  uint32_t back = ((uint8_t)text[1] << 16) | ((uint8_t)text[0] << 24);
  uint32_t mid = (uint8_t)text[1] << 24;
  uint32_t front = 0;
  for (size_t off = 2; off < text.length(); off++) {
    uint8_t c = (uint8_t)text[off];
    back |= c << 8;
    mid |= c << 16;
    front |= c << 24;
    ngrams->push_back(back);
    back = mid;
    mid = front;
    front = 0;
  }
}
*/

/*
void split_sse(const std::string& text, std::vector<uint32_t>* ngrams) {
  if (text.length() < 3) {
    return;
  }
  // we require correct padding to avoid writing scalar epilogue
  if (text.capacity() < (text.length() + 3)/4*4) {
    throw std::runtime_error("bad text padding");
  }

  int nngrams = text.length() - 2;
  ngrams->reserve((nngrams + 3) / 4 * 4 + 100);
  ngrams->resize(nngrams);

  const __m128i mask = _mm_set_epi8(-128,3,4,5,
                                    -128,2,3,4,
                                    -128,1,2,3,
                                    -128,0,1,2);
  for (size_t i = 0; i < text.length(); i += 4) {
    // mem: abcdefxxxxxxxxxxxxxxxxx
    // src: zzzz_zzzz_xxfe_dcba
    // msk: -128,3,4,5 -128,2,3,4 -128,1,2,3 -128,0,1,2
    // dst:    0def_0cde_0bcd_0abc
    // mem:    cba0_dcb0_edc0_fed0
    // u32[]: {0abc,0bcd,0cde,0def}
    __m128i src = _mm_loadl_epi64((const __m128i*)(text.data() + i));
    __m128i dst = _mm_shuffle_epi8(src, mask);
    _mm_storeu_si128((__m128i*)(ngrams->data() + i), dst);
  }
}
*/

/*
void hash_scalar(std::vector<uint32_t>* ngrams) {
  for (size_t i = 0; i < ngrams->size(); i++) {
    uint32_t x = (*ngrams)[i];
    x ^= x >> 16;
    x *= UINT32_C(0x7feb352d);
    x ^= x >> 15;
    x *= UINT32_C(0x846ca68b);
    x ^= x >> 16;
    (*ngrams)[i] = x;
  }
}
*/

/*
void hash_sse(std::vector<uint32_t>* ngrams) {
  if (ngrams->capacity() < (ngrams->size() + 3)/4*4) {
    throw std::runtime_error("bad ngrams padding");
  }
  const __m128i c1 = _mm_set_epi32(0x7feb352d, 0x7feb352d, 0x7feb352d, 0x7feb352d);
  const __m128i c2 = _mm_set_epi32(0x846ca68b, 0x846ca68b, 0x846ca68b, 0x846ca68b);
  for (size_t i = 0; i < ngrams->size(); i += 4) {
    __m128i x = _mm_loadu_si128((__m128i*)(ngrams->data() + i));
    x = _mm_xor_si128(x, _mm_srli_epi32(x, 16));
    x = _mm_mullo_epi32(x, c1);
    x = _mm_xor_si128(x, _mm_srli_epi32(x, 15));
    x = _mm_mullo_epi32(x, c2);
    x = _mm_xor_si128(x, _mm_srli_epi32(x, 16));
    _mm_storeu_si128((__m128i*)(ngrams->data() + i), x);
  }
}
*/

/*
void combined(const std::string& text, std::vector<uint32_t>* ngrams) {
  if (text.length() < 3) {
    return;
  }
  // we require correct padding to avoid writing scalar epilogue
  if (text.capacity() < (text.length() + 3)/4*4) {
    throw std::runtime_error("bad text padding");
  }

  int nngrams = text.length() - 2;
  ngrams->reserve((nngrams + 3) / 4 * 4 + 100);
  ngrams->resize(nngrams);

  const __m128i mask = _mm_set_epi8(-128,3,4,5,
                                    -128,2,3,4,
                                    -128,1,2,3,
                                    -128,0,1,2);
  const __m128i c1 = _mm_set_epi32(0x7feb352d, 0x7feb352d, 0x7feb352d, 0x7feb352d);
  const __m128i c2 = _mm_set_epi32(0x846ca68b, 0x846ca68b, 0x846ca68b, 0x846ca68b);
  for (size_t i = 0; i < text.length(); i += 4) {
    // mem: abcdefxxxxxxxxxxxxxxxxx
    // src: zzzz_zzzz_xxfe_dcba
    // msk: -128,3,4,5 -128,2,3,4 -128,1,2,3 -128,0,1,2
    // dst:    0def_0cde_0bcd_0abc
    // mem:    cba0_dcb0_edc0_fed0
    // u32[]: {0abc,0bcd,0cde,0def}
    __m128i src = _mm_loadl_epi64((const __m128i*)(text.data() + i));
    __m128i x = _mm_shuffle_epi8(src, mask);
    x = _mm_xor_si128(x, _mm_srli_epi32(x, 16));
    x = _mm_mullo_epi32(x, c1);
    x = _mm_xor_si128(x, _mm_srli_epi32(x, 15));
    x = _mm_mullo_epi32(x, c2);
    x = _mm_xor_si128(x, _mm_srli_epi32(x, 16));
    _mm_storeu_si128((__m128i*)(ngrams->data() + i), x);
  }
}
*/


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
    if (unlikely(v == kEmpty)) {
      empty_ = true;
      return;
    }
    int pos = (v & kMask);
    for (int i = pos; i < kSize; i++) {
      if (data_[i] == kEmpty) {
        data_[i] = v;
        return;
      }
    }
    for (int i = 0; i < pos; i++) {
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
    if (unlikely(v == kEmpty)) {
      return empty_;
    }
    int pos = (v & kMask);
    for (int i = pos; i < kSize; i++) {
      if (data_[i] == v) {
        return true;
      } else if (data_[i] == kEmpty) {
        return false;
      }
    }
    for (int i = 0; i < pos; i++) {
      if (data_[i] == v) {
        return true;
      } else if (data_[i] == kEmpty) {
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

  static constexpr uint32_t kEmpty{43}; //0
  const uint32_t kBits;
  const uint32_t kSize;
  const uint32_t kMask;

  uint32_t size_;
  std::vector<int32_t> data_;
  bool empty_; // contains kEmpty as element
};


template<int K, int N>
class NaiveShingler {
public:
  static constexpr int kHashLength = K;
  static constexpr int kNumHashes = N;

  static std::string name() {
    std::ostringstream str;
    str << "NaiveShingler<" << K << ',' << N << '>';
    return str.str();
  }

  void NOINLINE Process(const uint16_t* txt, int txt_length,
                        uint32_t* fng, int* fng_length)
  {
    std::vector<String> words;
    MakeWords(txt, txt_length, &words);

    std::vector<uint32_t> hashes;
    MakeShingles(words, &hashes);

    std::sort(hashes.begin(), hashes.end());
    hashes.erase(std::unique(hashes.begin(), hashes.end()), hashes.end());
    if (hashes.size() > N) {
      hashes.erase(hashes.begin() + N, hashes.end());
    }

    std::copy(hashes.begin(), hashes.end(), fng);
    *fng_length = hashes.size();
  }

private:
  void MakeWords(const uint16_t* txt, int txt_length, std::vector<String>* words) {
    words->reserve(txt_length / 6);

    int i = 0;
    while (i < txt_length) {
      while (i < txt_length && !IsAlphanum(txt[i])) {
        i++;
      }
      String word;
      while (i < txt_length && IsAlphanum(txt[i])) {
        word += txt[i];
        i++;
      }
      if (!word.empty()) {
        words->push_back(word);
      }
    }
  }

  void MakeShingles(const std::vector<String>& words, std::vector<uint32_t>* hashes) {
    if (words.size() >= K) {
      hashes->reserve(words.size() - (K - 1));
    }
    for (size_t i = 0; i < words.size() - (K - 1); i++) {
      uint32_t hash = 2166136261;
      for (int k = 0; k < K; k++) {
        const String& word = words[i+k];
        for (size_t j = 0; j < word.length(); j++) {
          uint16_t c = word[j];
          hash = hash ^ (c & 0xff);
          hash = hash * 16777619;
          hash = hash ^ (c >> 16);
          hash = hash * 16777619;
        }
      }
      hashes->push_back(hash);
    }
  }
};

template<int K, int N>
class Pipe1Shingler {
public:
  static constexpr int kHashLength = K;
  static constexpr int kNumHashes = N;

  static std::string name() {
    std::ostringstream str;
    str << "Pipe1Shingler<" << K << ',' << N << '>';
    return str.str();
  }

  void NOINLINE Process(const uint16_t* txt, int txt_length,
                        uint32_t* fng, int* fng_length)
  {
    uint32_t hashes[K];
    size_t i = 0;
    std::vector<uint32_t> buffer;
    buffer.reserve(txt_length/6);

    auto SkipDelim = [&](){
      while (i < txt_length && !IsAlphanum(txt[i])) {
        i++;
      }
    };
    auto ReadWord = [&](){
      while (i < txt_length && IsAlphanum(txt[i])) {
        uint16_t c = txt[i];
        for (int k = 0; k < K; k++) {
          hashes[k] = hashes[k] ^ (c & 0xff);
          hashes[k] = hashes[k] * 16777619;
          hashes[k] = hashes[k] ^ (c >> 16);
          hashes[k] = hashes[k] * 16777619;
        }
        i++;
      }
    };
    auto ShiftPipeline = [&](){
      for (int k = 0; k < K-1; k++) {
        hashes[k] = hashes[k+1];
      }
      hashes[K-1] = 2166136261;
    };

    // preload pipeline
    SkipDelim();
    for (int k = 0; k < K; k++) {
      hashes[k] = 2166136261;
    }
    for (int k = 0; k < K-1; k++) {
      ReadWord();
      ShiftPipeline();
      SkipDelim();
    }

    // main loop
    while (i < txt_length) {
      ReadWord();
      buffer.push_back(hashes[0]);
      ShiftPipeline();
      SkipDelim();
    }

    std::sort(buffer.begin(), buffer.end());
    buffer.erase(std::unique(buffer.begin(), buffer.end()), buffer.end());
    if (buffer.size() > N) {
      buffer.erase(buffer.begin() + N, buffer.end());
    }

    std::copy(buffer.begin(), buffer.end(), fng);
    *fng_length = buffer.size();
  }
};


template<int K, int N>
class Pipe2Shingler {
public:
  static constexpr int kHashLength = K;
  static constexpr int kNumHashes = N;

  static std::string name() {
    std::ostringstream str;
    str << "Pipe2Shingler<" << K << ',' << N << '>';
    return str.str();
  }

  void NOINLINE Process(const uint16_t* txt, int txt_length,
                        uint32_t* fng, int* fng_length)
  {
    uint32_t hashes[K];
    size_t i = 0;
    *fng_length = 0;

    std::unordered_set<uint32_t> htable;

    auto SkipDelim = [&](){
      while (i < txt_length && !IsAlphanum(txt[i])) {
        i++;
      }
    };
    auto ReadWord = [&](){
      while (i < txt_length && IsAlphanum(txt[i])) {
        uint16_t c = txt[i];
        for (int k = 0; k < K; k++) {
          hashes[k] = hashes[k] ^ (c & 0xff);
          hashes[k] = hashes[k] * 16777619;
          hashes[k] = hashes[k] ^ (c >> 16);
          hashes[k] = hashes[k] * 16777619;
        }
        i++;
      }
    };
    auto ShiftPipeline = [&](){
      for (int k = 0; k < K-1; k++) {
        hashes[k] = hashes[k+1];
      }
      hashes[K-1] = 2166136261;
    };

    // preload pipeline
    SkipDelim();
    for (int k = 0; k < K; k++) {
      hashes[k] = 2166136261;
    }
    for (int k = 0; k < K-1; k++) {
      ReadWord();
      ShiftPipeline();
      SkipDelim();
    }

    // collect first N hashes
    while (i < txt_length && *fng_length < N) {
      ReadWord();

      uint32_t h = hashes[0];
      if (htable.find(h) == htable.end()) {
        fng[(*fng_length)++] = h;
        htable.insert(h);
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
        if (htable.find(h) == htable.end()) {
          htable.erase(fng[0]);
          htable.insert(h);
          std::pop_heap(fng, fng + *fng_length);
          fng[*fng_length - 1] = h;
          std::push_heap(fng, fng + *fng_length);
        }
      }

      ShiftPipeline();

      SkipDelim();
    }
    std::sort_heap(fng, fng + *fng_length);
  }
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

  void NOINLINE Process(const uint16_t* txt, int txt_length,
                        uint32_t* fng, int* fng_length)
  {
    uint32_t hashes[K];
    size_t i = 0;
    *fng_length = 0;

    auto SkipDelim = [&](){
      while (i < txt_length && !IsAlphanum(txt[i])) {
        i++;
      }
    };
    auto ReadWord = [&](){
      while (i < txt_length && IsAlphanum(txt[i])) {
        uint16_t c = txt[i];
        for (int k = 0; k < K; k++) {
          hashes[k] = hashes[k] ^ (c & 0xff);
          hashes[k] = hashes[k] * 16777619;
          hashes[k] = hashes[k] ^ (c >> 16);
          hashes[k] = hashes[k] * 16777619;
        }
        i++;
      }
    };
    auto ShiftPipeline = [&](){
      for (int k = 0; k < K-1; k++) {
        hashes[k] = hashes[k+1];
      }
      hashes[K-1] = 2166136261;
    };

    // preload pipeline
    htable_.Clear();
    SkipDelim();
    for (int k = 0; k < K; k++) {
      hashes[k] = 2166136261;
    }
    for (int k = 0; k < K-1; k++) {
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

        if (htable_.size() >= htable_.capacity()/2) {
          htable_.Clear();
          for (size_t i = 0; i < *fng_length; i++) {
            htable_.InsertUnsafe(fng[i]);
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

          if (htable_.size() >= htable_.capacity()/2) {
            htable_.Clear();
            for (size_t i = 0; i < N; i++) {
              htable_.InsertUnsafe(fng[i]);
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

double JaccardClassical(const uint32_t* fng1, int len1,
                        const uint32_t* fng2, int len2,
                        double alpha)
{
  int pos1 = 0;
  int pos2 = 0;
  int nintersect = 0;
  while (pos1 < len1 && pos2 < len2) {
    if (fng1[pos1] == fng2[pos2]) {
      nintersect++;
      pos1++;
      pos2++;
    } else if (fng1[pos1] < fng2[pos2]) {
      pos1++;
    } else {
      pos2++;
    }
  }
  int nunion = len1 + len2 - nintersect;
  return nintersect / (double) nunion;
}

double JaccardFast(const uint32_t* fng1, int len1,
                   const uint32_t* fng2, int len2,
                   double alpha)
{
  int smin = (int)std::ceil((1.0-alpha)/(1.0 + alpha) * (len1 + len2));
  int pos1 = 0;
  int pos2 = 0;
  int nintersect = 0;
  int s = 0;
  while (pos1 < len1 && pos2 < len2) {
    if (fng1[pos1] == fng2[pos2]) {
      nintersect++;
      pos1++;
      pos2++;
    } else if (fng1[pos1] < fng2[pos2]) {
      pos1++;
      s++;
    } else {
      pos2++;
      s++;
    }
    if (s > smin) {
      return 0.0;
    }
  }
  int nunion = len1 + len2 - nintersect;
  return nintersect / (double) nunion;
}

double JaccardTurbo(const uint32_t* fng1, int len1,
                    const uint32_t* fng2, int len2,
                    double alpha)
{
  int smin = (int)std::ceil((1.0-alpha)/(1.0 + alpha) * (len1 + len2));
  int pos1 = 0;
  int pos2 = 0;
  int nintersect = 0;
  int s = 0;
  while (pos1+4 <= len1 && pos2+4 <= len2) {
    __m128i v1 = _mm_loadu_si128((const __m128i*)(fng1 + pos1));
    __m128i v2 = _mm_loadu_si128((const __m128i*)(fng2 + pos2));
    uint64_t m = _mm_cvtsi128_si64(_mm_cmpestrm(v1, 8, v2, 8, _SIDD_UWORD_OPS|_SIDD_CMP_EQUAL_ANY|_SIDD_BIT_MASK));
    if (m) {
      for (int i = 0; i < 4; i++) {
        if (fng1[pos1] == fng2[pos2]) {
          nintersect++;
          pos1++;
          pos2++;
        } else if (fng1[pos1] < fng2[pos2]) {
          pos1++;
          s++;
        } else {
          pos2++;
          s++;
        }
      }
    } else {
     if (fng1[pos1+3] < fng2[pos2+3]) {
        pos1 += 4;
      } else {
        pos2 += 4;
      }
      s += 4;
    }
    if (s > smin) {
      return 0.0;
    }
  }
  while (pos1 < len1 && pos2 < len2) {
    if (fng1[pos1] == fng2[pos2]) {
      nintersect++;
      pos1++;
      pos2++;
    } else if (fng1[pos1] < fng2[pos2]) {
      pos1++;
    } else {
      pos2++;
    }
  }

  int nunion = len1 + len2 - nintersect;
  return nintersect / (double) nunion;
}

void LoadDocs(const std::string& path,
              int count,
              std::vector<std::string>* utfs,
              std::vector<String>* strs)
{
  ReadLines(path, utfs);
  std::sort(utfs->begin(), utfs->end(), [](const std::string& s1, const std::string& s2){
    return s1.length() < s2.length();
  });
  std::reverse(utfs->begin(), utfs->end());
  utfs->erase(utfs->begin() + count, utfs->end());
  std::cout << "NumDocs:" << utfs->size() << std::endl;

  int size = 0;
  for (size_t i = 0; i < utfs->size(); i++) {
    String s;
    Uconv((*utfs)[i], s);
    strs->push_back(s);
    size += s.length();
  }
  std::cout << "NumChars:" << size << std::endl;
}

template<typename Shingler>
void ShinglerWrapper(const std::vector<String>& docs) {
  uint64_t best_time = std::numeric_limits<uint64_t>::max();
  uint64_t crc = 0;
  for (int pass = 0; pass < 3; pass++) {
    Shingler shingler;
    std::unique_ptr<uint32_t[]> fng(new uint32_t[Shingler::kNumHashes]);
    int fng_length;
    crc = 0;
    uint64_t time = 0;
    for (size_t i = 0; i < docs.size(); i++) {
      Timespan ts;
      shingler.Process(docs[i].data(), docs[i].length(), fng.get(), &fng_length);
      time += ts.microseconds();
      for (int i = 0; i < fng_length; i++) {
        crc += fng[i] * i;
      }
    }
    best_time = std::min(best_time, time);
  }
  std::cout << Shingler::name() << " BestTime:" << best_time << " Checksum:" << crc << std::endl;
}


void MakeFingerprints(const std::vector<String>& strs,
                      std::unique_ptr<uint32_t[]>* hashes,
                      std::vector<int>* offsets)
{
  Pipe3Shingler<3, 128> shingler;
  hashes->reset(new uint32_t[strs.size() * decltype(shingler)::kNumHashes]);
  uint32_t* ptr = hashes->get();
  int len;
  for (size_t i = 0; i < strs.size(); i++) {
    offsets->push_back(ptr - hashes->get());
    shingler.Process(strs[i].data(), strs[i].length(), ptr, &len);
    ptr += len;
  }
  offsets->push_back(ptr - hashes->get());
}

template<typename Func>
void JaccardWrapper(Func func, const uint32_t* hashes, const std::vector<int>& offsets) {
  double crc = 0.0;
  uint64_t best_time = std::numeric_limits<uint64_t>::max();
  for (int pass = 0; pass < 3; pass++) {
    Timespan ts;
    crc = 0.0;
    for (size_t i = 0; i < offsets.size() - 1; i++) {
      for (size_t j = i+1; j < offsets.size() - 1; j++) {
        const double alpha = 0.5;
        double v = func(hashes+offsets[i],  offsets[i+1]-offsets[i],
                        hashes+offsets[j],  offsets[j+1]-offsets[j],
                        alpha);
        if (v >= alpha) {
          crc += v;
        }
      }
    }
    best_time = std::min(best_time, ts.microseconds());
  }
  std::cout << "Checksum:" << crc << " Time:" << best_time << std::endl;
}

void BenchmarkShingling() {
  std::vector<std::string> utfs;
  std::vector<String> lines;
  LoadDocs("./ruwiki.txt", 1000, &utfs, &lines);

  ShinglerWrapper<NaiveShingler<3, 128>>(lines);
  ShinglerWrapper<Pipe1Shingler<3, 128>>(lines);
  ShinglerWrapper<Pipe2Shingler<3, 128>>(lines);
  ShinglerWrapper<Pipe3Shingler<3, 128>>(lines);
}

void BenchmarkJaccard() {
  std::vector<std::string> utfs;
  std::vector<String> lines;
  LoadDocs("./ruwiki.txt", 2000, &utfs, &lines);

  std::unique_ptr<uint32_t[]> hashes;
  std::vector<int> offsets;
  MakeFingerprints(lines, &hashes, &offsets);

  JaccardWrapper(JaccardClassical, hashes.get(), offsets);
  JaccardWrapper(JaccardFast,      hashes.get(), offsets);
  JaccardWrapper(JaccardTurbo,     hashes.get(), offsets);
}

void JaccardPrintMatches() {
  std::vector<std::string> utfs;
  std::vector<String> lines;
  LoadDocs("./ruwiki.txt", 2000, &utfs, &lines);

  std::unique_ptr<uint32_t[]> hashes;
  std::vector<int> offsets;
  MakeFingerprints(lines, &hashes, &offsets);

  struct Match {
    int idx1_;
    int idx2_;
    double jac_;

    Match(int idx1, int idx2, double jac) :
      idx1_(idx1), idx2_(idx2), jac_(jac) {}

    void print(std::ostream& out, const std::vector<std::string>& docs) const {
      out << jac_ << std::endl;
      out << docs[idx1_].substr(0, std::min<size_t>(250, docs[idx1_].length())) << std::endl;
      out << docs[idx2_].substr(0, std::min<size_t>(250, docs[idx2_].length())) << std::endl;
      out << std::endl;
    }
  };

  std::vector<Match> matches;
  for (size_t i = 0; i < offsets.size() - 1; i++) {
    for (size_t j = i+1; j < offsets.size() - 1; j++) {
      const double alpha = 0.5;
      double v = JaccardTurbo(hashes.get()+offsets[i],  offsets[i+1]-offsets[i],
                              hashes.get()+offsets[j],  offsets[j+1]-offsets[j],
                              alpha);
      if (v >= alpha) {
        matches.push_back(Match(i, j, v));
      }
    }
  }
  std::sort(matches.begin(), matches.end(), [](const Match& m1, const Match& m2){
    return m1.jac_ < m2.jac_;
  });
  std::reverse(matches.begin(), matches.end());
  for (size_t i = 0; i < std::min<size_t>(matches.size(), 20); i++) {
    matches[i].print(std::cout, utfs);
  }
}

#endif
