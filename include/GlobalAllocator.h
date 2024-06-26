#if !defined(_GLOBAL_ALLOCATOR_H_)
#define _GLOBAL_ALLOCATOR_H_

#include "Common.h"
#include "Debug.h"
#include "GlobalAddress.h"

#include <cstring>



// global allocator for coarse-grained (chunck level) alloc 
// used by home agent
// bitmap based
class GlobalAllocator {

public:
  GlobalAllocator(const GlobalAddress &start, size_t size)
      : start(start), size(size) {
    bitmap_len = size / define::kChunkSize;
    bitmap = new bool[bitmap_len];
    memset(bitmap, 0, bitmap_len);
     
    // null ptr
    bitmap[0] = true;
    bitmap_tail = 1;
  }

  ~GlobalAllocator() {
      printf("The remote memory usage is %lu MB", bitmap_tail*define::kChunkSize);
      delete[] bitmap;
  }

  GlobalAddress alloc_chunck() {
      static size_t counter = 0;
      if(counter++ % 32 == 0){
          printf("The remote memory usage is %lu MB\n", bitmap_tail*32);
      }

          GlobalAddress res = start;
    if (bitmap_tail >= bitmap_len) {
      assert(false);
      Debug::notifyError("shared memory space run out");
    }

    if (bitmap[bitmap_tail] == false) {
      bitmap[bitmap_tail] = true;
      res.offset += bitmap_tail * define::kChunkSize;

      bitmap_tail++;
    } else {
      assert(false);
      Debug::notifyError("TODO");
    }

    return res;
  }

  void free_chunk(const GlobalAddress &addr) {
    bitmap[(addr.offset - start.offset) / define::kChunkSize] = false;
  }

private:
  GlobalAddress start;
  size_t size;

  bool *bitmap;
  size_t bitmap_len;
  size_t bitmap_tail;
};

#endif
