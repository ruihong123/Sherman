#if !defined(_HOT_BUFFER_H_)
#define _HOT_BUFFER_H_

#include "Common.h"

#include <atomic>

enum class HotResult { SUCC, FAIL, OCCUPIED };

class HotBuffer {

  const int kBufferSize = 10240;

public:
  HotBuffer() {
    buf = new std::atomic<Key>[kBufferSize];
    for (int i = 0; i < kBufferSize; ++i) {
      buf[i].store(kKeyNull);
    }
  };

  HotResult set(const Key &key) {
    size_t index = key % kBufferSize;

    auto value = buf[index].load();
    if (value == key) {
      return HotResult::OCCUPIED;
    }
    if (value != kKeyNull) {
      return HotResult::FAIL;
    }
    
    Key null_key = kKeyNull;
    auto res =  buf[index].compare_exchange_strong(null_key, key);

    return res ? HotResult::SUCC : HotResult::FAIL;
  }

  void clear(const Key &key) {
    size_t index = key % kBufferSize;
    buf[index].store(kKeyNull);
  }

  bool wait(const Key &key) {
    size_t index = key % kBufferSize;
    return buf[index].load() != key;
  }

  std::atomic<Key> *buf;
};

#endif // _HOT_BUFFER_H_
