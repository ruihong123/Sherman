#include "Tree.h"
#include "HotBuffer.h"
#include "IndexCache.h"
#include "RdmaBuffer.h"
#include "Timer.h"

#include <algorithm>
#include <city.h>
#include <iostream>
#include <queue>
#include <utility>
#include <vector>

bool enter_debug = false;

HotBuffer hot_buf;
uint64_t cache_miss[MAX_APP_THREAD][8];
uint64_t cache_hit[MAX_APP_THREAD][8];
uint64_t invalid_counter[MAX_APP_THREAD][8];
uint64_t lock_fail[MAX_APP_THREAD][8];
uint64_t pattern[MAX_APP_THREAD][8];
uint64_t hierarchy_lock[MAX_APP_THREAD][8];
uint64_t handover_count[MAX_APP_THREAD][8];
uint64_t hot_filter_count[MAX_APP_THREAD][8];
uint64_t latency[MAX_APP_THREAD][LATENCY_WINDOWS];
volatile bool need_stop = false;

thread_local CoroCall Tree::worker[define::kMaxCoro];
thread_local CoroCall Tree::master;
thread_local GlobalAddress path_stack[define::kMaxCoro]
                                     [define::kMaxLevelOfTree];

// for coroutine schedule
struct CoroDeadline {
  uint64_t deadline;
  uint16_t coro_id;

  bool operator<(const CoroDeadline &o) const {
    return this->deadline < o.deadline;
  }
};

thread_local Timer timer;
thread_local std::queue<uint16_t> hot_wait_queue;
thread_local std::priority_queue<CoroDeadline> deadline_queue;

Tree::Tree(DSM *dsm, uint16_t tree_id) : dsm(dsm), tree_id(tree_id){

  for (int i = 0; i < dsm->getClusterSize(); ++i) {
    local_locks[i] = new LocalLockNode[define::kNumOfLock];
    for (size_t k = 0; k < define::kNumOfLock; ++k) {
      auto &n = local_locks[i][k];
      n.ticket_lock.store(0);
      n.hand_over = false;
      n.hand_time = 0;
    }
  }

  assert(dsm->is_register());
  print_verbose();

  index_cache = new IndexCache(define::kIndexCacheSize);

  root_ptr_ptr = get_root_ptr_ptr();

  // try to init tree and install root pointer
  auto page_buffer = (dsm->get_rbuf(0)).get_page_buffer();
  auto root_addr = dsm->alloc(kLeafPageSize);
  auto root_page = new (page_buffer) LeafPage;

  root_page->set_consistent();
  dsm->write_sync(page_buffer, root_addr, kLeafPageSize);

  auto cas_buffer = (dsm->get_rbuf(0)).get_cas_buffer();
  bool res = dsm->cas_sync(root_ptr_ptr, 0, root_addr.val, cas_buffer);
  if (res) {
    std::cout << "Tree root pointer value " << root_addr << std::endl;
  } else {
     std::cout << "fail\n";
  }
}

void Tree::print_verbose() {

  int kLeafHdrOffset = STRUCT_OFFSET(LeafPage, hdr);
  int kInternalHdrOffset = STRUCT_OFFSET(InternalPage, hdr);
  assert(kLeafHdrOffset == kInternalHdrOffset);

  if (dsm->getMyNodeID() == 0) {
    std::cout << "Header size: " << sizeof(Header) << std::endl;
    std::cout << "Internal Page size: " << sizeof(InternalPage) << " ["
              << kInternalPageSize << "]" << std::endl;
    std::cout << "Internal per Page: " << kInternalCardinality << std::endl;
    std::cout << "Leaf Page size: " << sizeof(LeafPage) << " [" << kLeafPageSize
              << "]" << std::endl;
    std::cout << "Leaf per Page: " << kLeafCardinality << std::endl;
    std::cout << "LeafEntry size: " << sizeof(LeafEntry) << std::endl;
    std::cout << "InternalEntry size: " << sizeof(InternalEntry) << std::endl;
  }
}

inline void Tree::before_operation(CoroContext *cxt, int coro_id) {
  for (size_t i = 0; i < define::kMaxLevelOfTree; ++i) {
    path_stack[coro_id][i] = GlobalAddress::Null();
  }
}

GlobalAddress Tree::get_root_ptr_ptr() {
  GlobalAddress addr;
  addr.nodeID = 0;
  addr.offset =
      define::kRootPointerStoreOffest + sizeof(GlobalAddress) * tree_id;

  return addr;
}

extern GlobalAddress g_root_ptr;
extern int g_root_level;
extern bool enable_cache;
GlobalAddress Tree::get_root_ptr(CoroContext *cxt, int coro_id) {

  if (g_root_ptr == GlobalAddress::Null()) {
    auto page_buffer = (dsm->get_rbuf(coro_id)).get_page_buffer();
    dsm->read_sync(page_buffer, root_ptr_ptr, sizeof(GlobalAddress), cxt);
    GlobalAddress root_ptr = *(GlobalAddress *)page_buffer;
    std::cout << "Get new root" << root_ptr <<std::endl;
    g_root_ptr = root_ptr;
    return root_ptr;
  } else {
    return g_root_ptr;
  }

  // std::cout << "root ptr " << root_ptr << std::endl;
}

void Tree::broadcast_new_root(GlobalAddress new_root_addr, int root_level) {
  RawMessage m;
  m.type = RpcType::NEW_ROOT;
  m.addr = new_root_addr;
  m.level = root_level;
  if (root_level >= 5) {
        enable_cache = true;
  }
  //TODO: When we seperate the compute from the memory, how can we broad cast the new root
  // or can we wait until the compute node detect an inconsistent.
  for (int i = 0; i < dsm->getClusterSize(); ++i) {
    dsm->rpc_call_dir(m, i);
  }
}

bool Tree::update_new_root(GlobalAddress left, const Key &k,
                           GlobalAddress right, int level,
                           GlobalAddress old_root, CoroContext *cxt,
                           int coro_id) {

  auto page_buffer = dsm->get_rbuf(coro_id).get_page_buffer();
  auto cas_buffer = dsm->get_rbuf(coro_id).get_cas_buffer();
    assert(left != GlobalAddress::Null());
    assert(right != GlobalAddress::Null());
  auto new_root = new (page_buffer) InternalPage(left, k, right, level);

  auto new_root_addr = dsm->alloc(kInternalPageSize);
  // The code below is just for debugging
//    new_root_addr.mark = 3;
  new_root->set_consistent();
  // set local cache for root address
  g_root_ptr = new_root_addr;
  dsm->write_sync(page_buffer, new_root_addr, kInternalPageSize, cxt);
  if (dsm->cas_sync(root_ptr_ptr, old_root, new_root_addr, cas_buffer, cxt)) {
    broadcast_new_root(new_root_addr, level);
    std::cout << "new root level " << level << " " << new_root_addr
              << std::endl;
    return true;
  } else {
    std::cout << "cas root fail " << std::endl;
  }

  return false;
}

void Tree::print_and_check_tree(CoroContext *cxt, int coro_id) {
  assert(dsm->is_register());

  auto root = get_root_ptr(cxt, coro_id);
  // SearchResult result;

  GlobalAddress p = root;
  GlobalAddress levels[define::kMaxLevelOfTree];
  int level_cnt = 0;
  auto page_buffer = (dsm->get_rbuf(coro_id)).get_page_buffer();
  GlobalAddress leaf_head;

next_level:

  dsm->read_sync(page_buffer, p, kLeafPageSize);
  auto header = (Header *)(page_buffer + (STRUCT_OFFSET(LeafPage, hdr)));
  levels[level_cnt++] = p;
  if (header->level != 0) {
    p = header->leftmost_ptr;
    goto next_level;
  } else {
    leaf_head = p;
  }

next:
  dsm->read_sync(page_buffer, leaf_head, kLeafPageSize);
  auto page = (LeafPage *)page_buffer;
  for (int i = 0; i < kLeafCardinality; ++i) {
    if (page->records[i].value != kValueNull) {
    }
  }
  while (page->hdr.sibling_ptr != GlobalAddress::Null()) {
    leaf_head = page->hdr.sibling_ptr;
    goto next;
  }

  // for (int i = 0; i < level_cnt; ++i) {
  //   dsm->read_sync(page_buffer, levels[i], kLeafPageSize);
  //   auto header = (Header *)(page_buffer + (STRUCT_OFFSET(LeafPage, hdr)));
  //   // std::cout << "addr: " << levels[i] << " ";
  //   // header->debug();
  //   // std::cout << " | ";
  //   while (header->sibling_ptr != GlobalAddress::Null()) {
  //     dsm->read_sync(page_buffer, header->sibling_ptr, kLeafPageSize);
  //     header = (Header *)(page_buffer + (STRUCT_OFFSET(LeafPage, hdr)));
  //     // std::cout << "addr: " << header->sibling_ptr << " ";
  //     // header->debug();
  //     // std::cout << " | ";
  //   }
  //   // std::cout << "\n------------------------------------" << std::endl;
  //   // std::cout << "------------------------------------" << std::endl;
  // }
}

GlobalAddress Tree::query_cache(const Key &k) { return GlobalAddress::Null(); }

inline bool Tree::try_lock_addr(GlobalAddress lock_addr, uint64_t tag,
                                uint64_t *buf, CoroContext *cxt, int coro_id) {
  auto &pattern_cnt = pattern[dsm->getMyThreadID()][lock_addr.nodeID];

  bool hand_over = acquire_local_lock(lock_addr, cxt, coro_id);
  if (hand_over) {
    return true;
  }

  {

    uint64_t retry_cnt = 0;
    uint64_t pre_tag = 0;
    uint64_t conflict_tag = 0;
  retry:
    retry_cnt++;
    if (retry_cnt > 300000) {
      std::cout << "Deadlock " << lock_addr << std::endl;

      std::cout << dsm->getMyNodeID() << ", " << dsm->getMyThreadID()
                << " locked by " << (conflict_tag >> 32) << ", "
                << (conflict_tag << 32 >> 32) << std::endl;
//      assert(false);
//      exit(0);
    }

    bool res = dsm->cas_dm_sync(lock_addr, 0, tag, buf, cxt);
//      std::cout << "lock address " << lock_addr << std::endl;
    pattern_cnt++;
    if (!res) {
      conflict_tag = *buf - 1;
      if (conflict_tag != pre_tag) {
        retry_cnt = 0;
        pre_tag = conflict_tag;
      }
      lock_fail[dsm->getMyThreadID()][0]++;
      goto retry;
    }
  }

  return true;
}

inline void Tree::unlock_addr(GlobalAddress lock_addr, uint64_t tag,
                              uint64_t *buf, CoroContext *cxt, int coro_id,
                              bool async) {

  bool hand_over_other = can_hand_over(lock_addr);
  if (hand_over_other) {
    releases_local_lock(lock_addr);
    return;
  }

  auto cas_buf = dsm->get_rbuf(coro_id).get_cas_buffer();
//    std::cout << "unlock " << lock_addr << std::endl;
  *cas_buf = 0;
  if (async) {
    dsm->write_dm((char *)cas_buf, lock_addr, sizeof(uint64_t), false);
  } else {
    dsm->write_dm_sync((char *)cas_buf, lock_addr, sizeof(uint64_t), cxt);
  }

  releases_local_lock(lock_addr);
}

void Tree::write_page_and_unlock(char *page_buffer, GlobalAddress page_addr,
                                 int page_size, uint64_t *cas_buffer,
                                 GlobalAddress lock_addr, uint64_t tag,
                                 CoroContext *cxt, int coro_id, bool async) {

  bool hand_over_other = can_hand_over(lock_addr);
  if (hand_over_other) {
    dsm->write_sync(page_buffer, page_addr, page_size, cxt);
    releases_local_lock(lock_addr);
    return;
  }
    //TODO: make the write unlock not RDMA write, use RDMA cas.
  RdmaOpRegion rs[2];
  rs[0].source = (uint64_t)page_buffer;
  rs[0].dest = page_addr;
  rs[0].size = page_size;
  rs[0].is_lock_mr = false;

  rs[1].source = (uint64_t)dsm->get_rbuf(coro_id).get_cas_buffer();
  rs[1].dest = lock_addr;
  rs[1].size = sizeof(uint64_t);

  rs[1].is_lock_mr = true;
//    auto tag = dsm->getThreadTag();
  *(uint64_t *)rs[1].source = 0;
  if (async) {
//    dsm->write_batch(rs, 2, false);
      dsm->write_cas(rs[0],rs[1], tag, 0, false, cxt);
  } else {
    dsm->write_cas_sync(rs[0],rs[1], tag, 0, cxt);
  }

  releases_local_lock(lock_addr);
}

void Tree::lock_and_read_page(char *page_buffer, GlobalAddress page_addr,
                              int page_size, uint64_t *cas_buffer,
                              GlobalAddress lock_addr, uint64_t tag,
                              CoroContext *cxt, int coro_id) {

  try_lock_addr(lock_addr, tag, cas_buffer, cxt, coro_id);

  dsm->read_sync(page_buffer, page_addr, page_size, cxt);
  pattern[dsm->getMyThreadID()][page_addr.nodeID]++;
}

void Tree::lock_bench(const Key &k, CoroContext *cxt, int coro_id) {
  uint64_t lock_index = CityHash64((char *)&k, sizeof(k)) % define::kNumOfLock;

  GlobalAddress lock_addr;
  lock_addr.nodeID = 0;
  lock_addr.offset = lock_index * sizeof(uint64_t);
  auto cas_buffer = dsm->get_rbuf(coro_id).get_cas_buffer();

  // bool res = dsm->cas_sync(lock_addr, 0, 1, cas_buffer, cxt);
  try_lock_addr(lock_addr, 1, cas_buffer, cxt, coro_id);
  unlock_addr(lock_addr, 1, cas_buffer, cxt, coro_id, true);
}
// You need to make sure it is not the root level
// why there is no lock coupling?
void Tree::insert_internal(const Key &k, GlobalAddress v, CoroContext *cxt,
                           int coro_id, int level) {
  auto root = get_root_ptr(cxt, coro_id);
  SearchResult result;

  GlobalAddress p = root;
    //TODO: ADD support for root invalidate and update.
next:

  if (!page_search(p, k, result, cxt, coro_id)) {
    std::cout << "SEARCH WARNING insert" << std::endl;
    p = get_root_ptr(cxt, coro_id);
    sleep(1);
    goto next;
  }

  assert(result.level != 0);
  if (result.slibing != GlobalAddress::Null()) {
    p = result.slibing;
    goto next;
  }

  p = result.next_level;
  if (result.level != level + 1) {
    goto next;
  }

  internal_page_store(p, k, v, root, level, cxt, coro_id);
}

void Tree::insert(const Key &k, const Value &v, CoroContext *cxt, int coro_id) {
  assert(dsm->is_register());

  before_operation(cxt, coro_id);

//  auto res = hot_buf.set(k);
//
//  if (res == HotResult::OCCUPIED) {
//    hot_filter_count[dsm->getMyThreadID()][0]++;
//    if (cxt == nullptr) {
//      while (!hot_buf.wait(k))
//        ;
//    } else {
//      hot_wait_queue.push(coro_id);
//      (*cxt->yield)(*cxt->master);
//    }
//  }

  if (enable_cache) {
    GlobalAddress cache_addr;
    //The cache here is a skip list mapping from key(from, to) to a cached internal node. when searching the
    // only when the from and to are both equal the entry in the skip list, then there will be cache hit. This is actually
    // a tuple level cache, which may not has a large cache locality.
    auto entry = index_cache->search_from_cache(k, &cache_addr);
    if (entry) { // cache hit
      auto root = get_root_ptr(cxt, coro_id);
      // this will by pass the page search.
      // there will be a potentially bug for concurrent root update
      if (leaf_page_store(cache_addr, k, v, root, 0, cxt, coro_id, true)) {

        cache_hit[dsm->getMyThreadID()][0]++;
//          printf("Cache hit\n");
//        if (res == HotResult::SUCC) {
//          hot_buf.clear(k);
//        }

        return;
      }
      // cache stale, from root,
      index_cache->invalidate(entry);
//        invalid_counter[dsm->getMyThreadID()][1]++;
//        if(invalid_counter[dsm->getMyThreadID()][1] % 5000 == 0){
//            printf("Invalidate cache 1\n");
//        }
//        printf("Invalidate cache\n");
    }
    cache_miss[dsm->getMyThreadID()][0]++;
  }

  auto root = get_root_ptr(cxt, coro_id);
//  std::cout << "The root now is " << root << std::endl;
  SearchResult result;
  GlobalAddress p = root;
  // this is root is to help the tree to refresh the root node because the
  // new root broadcast is not usable if physical disaggregated.
  bool isroot = true;
    //The page_search will be executed mulitple times if the result is not is_leaf
next:
  if (!page_search(p, k, result, cxt, coro_id, false, isroot)) {
      if (isroot){
          std::cout << "SEARCH WARNING insert" << std::endl;
          p = get_root_ptr(cxt, coro_id);
          sleep(1);
          goto next;
      }else{
          p =  path_stack[coro_id][result.level+1];
          goto next;
      }

  }
  isroot = false;
//The page_search will be executed mulitple times if the result is not is_leaf
// Maybe it will goes to the sibling pointer or go to the children
  if (!result.is_leaf) {
    assert(result.level != 0);
    if (result.slibing != GlobalAddress::Null()) {
      p = result.slibing;
      goto next;
    }

    p = result.next_level;
//    printf("next level pointer is %p\n", p);
    if (result.level != 1) {
      goto next;
    }
  }

  if(!leaf_page_store(p, k, v, root, 0, cxt, coro_id, true)){
      p =  path_stack[coro_id][1];
      goto next;
  }

//  if (res == HotResult::SUCC) {
//    hot_buf.clear(k);
//  }
}

bool Tree::search(const Key &k, Value &v, CoroContext *cxt, int coro_id) {
  assert(dsm->is_register());

  auto root = get_root_ptr(cxt, coro_id);
  SearchResult result;

  GlobalAddress p = root;
    bool isroot = true;
  bool from_cache = false;
  const CacheEntry *entry = nullptr;
  if (enable_cache) {
    GlobalAddress cache_addr;
    entry = index_cache->search_from_cache(k, &cache_addr);
    if (entry) { // cache hit
      cache_hit[dsm->getMyThreadID()][0]++;
      from_cache = true;
      p = cache_addr;
      isroot = false;
    } else {
      cache_miss[dsm->getMyThreadID()][0]++;
    }
  }

next:
  if (!page_search(p, k, result, cxt, coro_id, from_cache, isroot)) {
    if (from_cache) { // cache stale
      index_cache->invalidate(entry);
      // Comment it during the test.
//        invalid_counter[dsm->getMyThreadID()][0]++;
//        if(invalid_counter[dsm->getMyThreadID()][0] % 5000 == 0){
//            printf("Invalidate cache 0\n");
//        }
        //The cache hit is the real cache hit counting the invalidation in
      cache_hit[dsm->getMyThreadID()][0]--;
      cache_miss[dsm->getMyThreadID()][0]++;
      from_cache = false;

      p = root;
      isroot = true;
    } else {
      std::cout << "SEARCH WARNING search" << std::endl;
      sleep(1);
    }
    goto next;
  }
  else{
      isroot = false;
  }

  if (result.is_leaf) {
    if (result.val != kValueNull) { // find
      v = result.val;

      return true;
    }
    if (result.slibing != GlobalAddress::Null()) { // turn right
      p = result.slibing;
      goto next;
    }
    return false; // not found
  } else {        // internal
    p = result.slibing != GlobalAddress::Null() ? result.slibing
                                                : result.next_level;
    goto next;
  }
}

// TODO: Need Fix
uint64_t Tree::range_query(const Key &from, const Key &to, Value *value_buffer,
                           CoroContext *cxt, int coro_id) {

  const int kParaFetch = 32;
  thread_local std::vector<InternalPage *> result;
  thread_local std::vector<GlobalAddress> leaves;

  result.clear();
  leaves.clear();
  index_cache->search_range_from_cache(from, to, result);

  if (result.empty()) {
    return 0;
  }

  uint64_t counter = 0;
  for (auto page : result) {
    auto cnt = page->hdr.last_index + 1;
    auto addr = page->hdr.leftmost_ptr;

    // [from, to]
    // [lowest, page->records[0].key);
    bool no_fetch = from > page->records[0].key || to < page->hdr.lowest;
    if (!no_fetch) {
      leaves.push_back(addr);
    }
    for (int i = 1; i < cnt; ++i) {
      no_fetch = from > page->records[i].key || to < page->records[i - 1].key;
      if (!no_fetch) {
        leaves.push_back(page->records[i - 1].ptr);
      }
    }

    no_fetch = from > page->hdr.highest || to < page->records[cnt - 1].key;
    if (!no_fetch) {
      leaves.push_back(page->records[cnt - 1].ptr);
    }
  }

  // printf("---- %d ----\n", leaves.size());
  // sleep(1);

  int cq_cnt = 0;
  char *range_buffer = (dsm->get_rbuf(coro_id)).get_range_buffer();
  for (size_t i = 0; i < leaves.size(); ++i) {
    if (i > 0 && i % kParaFetch == 0) {
      dsm->poll_rdma_cq(kParaFetch);
      cq_cnt -= kParaFetch;
      for (int k = 0; k < kParaFetch; ++k) {
        auto page = (LeafPage *)(range_buffer + k * kLeafPageSize);
        for (int i = 0; i < kLeafCardinality; ++i) {
          auto &r = page->records[i];
          if (r.value != kValueNull && r.f_version == r.r_version) {
            if (r.key >= from && r.key <= to) {
              value_buffer[counter++] = r.value;
            }
          }
        }
      }
    }
    dsm->read(range_buffer + kLeafPageSize * (i % kParaFetch), leaves[i],
              kLeafPageSize, true);
    cq_cnt++;
  }

  if (cq_cnt != 0) {
    dsm->poll_rdma_cq(cq_cnt);
    for (int k = 0; k < cq_cnt; ++k) {
      auto page = (LeafPage *)(range_buffer + k * kLeafPageSize);
      for (int i = 0; i < kLeafCardinality; ++i) {
        auto &r = page->records[i];
        if (r.value != kValueNull && r.f_version == r.r_version) {
          if (r.key >= from && r.key <= to) {
            value_buffer[counter++] = r.value;
          }
        }
      }
    }
  }

  return counter;
}

// Del needs to be rewritten
void Tree::del(const Key &k, CoroContext *cxt, int coro_id) {
  assert(dsm->is_register());
  before_operation(cxt, coro_id);

  auto root = get_root_ptr(cxt, coro_id);
  SearchResult result;

  GlobalAddress p = root;

next:
  if (!page_search(p, k, result, cxt, coro_id)) {
    std::cout << "SEARCH WARNING" << std::endl;
    goto next;
  }

  if (!result.is_leaf) {
    assert(result.level != 0);
    if (result.slibing != GlobalAddress::Null()) {
      p = result.slibing;
      goto next;
    }

    p = result.next_level;
    if (result.level != 1) {

      goto next;
    }
  }

  leaf_page_del(p, k, 0, cxt, coro_id);
}
//Node ID in GLobalAddress for a tree pointer should be the id in the Memory pool
// THis funciton will get the page by the page addr and search the pointer for the
// next level if it is not leaf page. If it is a leaf page, just put the value in the
// result
bool Tree::page_search(GlobalAddress page_addr, const Key &k,
                       SearchResult &result, CoroContext *cxt, int coro_id,
                       bool from_cache, bool isroot) {
  auto page_buffer = (dsm->get_rbuf(coro_id)).get_page_buffer();
  char local_buffer[kLeafPageSize] = {};
    auto header = (Header *)(local_buffer + (STRUCT_OFFSET(LeafPage, hdr)));
  auto &pattern_cnt = pattern[dsm->getMyThreadID()][page_addr.nodeID];

  int counter = 0;
re_read:
  if (++counter > 100) {
    printf("re read too many times\n");
    sleep(1);
  }
  //we need three RDMA round trips to guarantee the correctness here.
  // front version, page content, back veriosn.
  GlobalAddress front_version_addr = page_addr;
  front_version_addr.offset += STRUCT_OFFSET(LeafPage,front_version);
    GlobalAddress back_version_addr = page_addr ;
    back_version_addr.offset += STRUCT_OFFSET(LeafPage,rear_version);
     //TODO: The read page three times for invalidation.
    dsm->read_sync(page_buffer, front_version_addr, sizeof(uint8_t), cxt);
    uint8_t front_v = *(uint8_t*) page_buffer;
  dsm->read_sync(page_buffer, page_addr, kLeafPageSize, cxt);
    memcpy(local_buffer, page_buffer, kLeafPageSize);
    dsm->read_sync(page_buffer, back_version_addr, sizeof(uint8_t), cxt);
    uint8_t rear_v = *(uint8_t*) page_buffer;

    //TODO: The read page three times for invalidation.
//    dsm->read_sync(page_buffer, page_addr, kLeafPageSize, cxt);
//    uint8_t front_v = *(uint8_t*) (page_buffer + (STRUCT_OFFSET(LeafPage, front_version)));
//    dsm->read_sync(page_buffer, page_addr, kLeafPageSize, cxt);
//    memcpy(local_buffer, page_buffer, kLeafPageSize);
//    dsm->read_sync(page_buffer, page_addr, kLeafPageSize, cxt);
//    uint8_t rear_v = *(uint8_t*) (page_buffer + (STRUCT_OFFSET(LeafPage, front_version)));
  pattern_cnt++;
  memset(&result, 0, sizeof(result));
  result.is_leaf = header->leftmost_ptr == GlobalAddress::Null();
  result.level = header->level;
  if(!result.is_leaf)
      assert(result.level !=0);
  path_stack[coro_id][result.level] = page_addr;
  // std::cout << "level " << (int)result.level << " " << page_addr <<
  // std::endl;

  if (result.is_leaf) {
    auto page = (LeafPage *)local_buffer;
    if (!page->check_consistent() | (front_v != rear_v)) {
      goto re_read;
    }

    if (from_cache &&
        (k < page->hdr.lowest || k >= page->hdr.highest)) { // cache is stale
      return false;
    }

    assert(result.level == 0);
    if (k >= page->hdr.highest) { // should turn right
//        printf("should turn right ");
      result.slibing = page->hdr.sibling_ptr;
      return true;
    }
    if (k < page->hdr.lowest) {
      assert(false);
      return false;
    }
    leaf_page_search(page, k, result);
  } else {

      assert(result.level != 0);
    assert(!from_cache);
    auto page = (InternalPage *)local_buffer;
//      assert(page->records[page->hdr.last_index].ptr != GlobalAddress::Null());

    if (!page->check_consistent()) {
      goto re_read;
    }

    if (result.level == 1 && enable_cache) {
        // add the pointer of this internal page into cache. Why it make sense?
//        printf("Add content to cache\n");
      index_cache->add_to_cache(page);
      // if (enter_debug) {
      //   printf("add %lud [%lud %lud]\n", k, page->hdr.lowest,
      //          page->hdr.highest);
      // }
    }

    if (k >= page->hdr.highest) { // should turn right
//        printf("should turn right ");
    // TODO: if this is the root node then we need to refresh the new root.
        if (isroot){
            // invalidate the root.
            g_root_ptr = GlobalAddress::Null();
        }
      result.slibing = page->hdr.sibling_ptr;
      return true;
    }
    if (k < page->hdr.lowest) {
      printf("key %ld error in level %d\n", k, page->hdr.level);
      sleep(10);
      print_and_check_tree();
      assert(false);
      return false;
    }
    // this function will add the children pointer to the result.
    internal_page_search(page, k, result);
  }

  return true;
}
// internal page serach will return the global point for the next level
void Tree::internal_page_search(InternalPage *page, const Key &k,
                                SearchResult &result) {

  assert(k >= page->hdr.lowest);
  assert(k < page->hdr.highest);
// if the record front verison is not equal to the rear version, what to do.
    // If we pile up the index sequentially by mulitple threads the bugs will happen
    // when muli9tple thread trying to modify the same page, because the reread for
    // inconsistent record below is not well implemented.

    //TODO (potential bug) what will happen if the last record version is not consistent?

  auto cnt = page->hdr.last_index + 1;
  // page->debug();
  if (k < page->records[0].key) { // this only happen when the lowest is 0
//      printf("next level pointer is  leftmost %p \n", page->hdr.leftmost_ptr);
    result.next_level = page->hdr.leftmost_ptr;
//      result.upper_key = page->records[0].key;
      assert(result.next_level != GlobalAddress::Null());
//      assert(page->hdr.lowest == 0);//this actually should not happen
    return;
  }

  for (int i = 1; i < cnt; ++i) {
    if (k < page->records[i].key) {
//        printf("next level key is %lu \n", page->records[i - 1].key);
      result.next_level = page->records[i - 1].ptr;
        assert(result.next_level != GlobalAddress::Null());
        assert(page->records[i - 1].key <= k);
        result.upper_key = page->records[i - 1].key;
      return;
    }
  }
//    printf("next level pointer is  the last value %p \n", page->records[cnt - 1].ptr);

    result.next_level = page->records[cnt - 1].ptr;
    assert(result.next_level != GlobalAddress::Null());
    assert(page->records[cnt - 1].key <= k);
//    result.upper_key = page->records[cnt - 1].key;
}

void Tree::leaf_page_search(LeafPage *page, const Key &k,
                            SearchResult &result) {

  for (int i = 0; i < kLeafCardinality; ++i) {
    auto &r = page->records[i];
    // if the record front verison is not equal to the rear version, what to do.
    // If we pile up the index sequentially by mulitple threads the bugs will happen
    // when muli9tple thread trying to modify the same page, because the reread for
    // inconsistent record below is not well implemented.
    if (r.key == k && r.value != kValueNull && r.f_version == r.r_version) {
      result.val = r.value;
#ifdef PADDED_VALUE
        memcpy(result.value_padding, r.value_padding, VALUE_PADDING);
#endif
//      result.value_padding = r.value_padding;
      break;
    }
  }
}

void Tree::internal_page_store(GlobalAddress page_addr, const Key &k,
                               GlobalAddress v, GlobalAddress root, int level,
                               CoroContext *cxt, int coro_id) {
  uint64_t lock_index =
      CityHash64((char *)&page_addr, sizeof(page_addr)) % define::kNumOfLock;

  GlobalAddress lock_addr;
  lock_addr.nodeID = page_addr.nodeID;
  lock_addr.offset = lock_index * sizeof(uint64_t);

  auto &rbuf = dsm->get_rbuf(coro_id);
  uint64_t *cas_buffer = rbuf.get_cas_buffer();
  auto page_buffer = rbuf.get_page_buffer();

  auto tag = dsm->getThreadTag();
  assert(tag != 0);

  lock_and_read_page(page_buffer, page_addr, kInternalPageSize, cas_buffer,
                     lock_addr, tag, cxt, coro_id);

  auto page = (InternalPage *)page_buffer;

  assert(page->hdr.level == level);
  assert(page->check_consistent());
  if (k >= page->hdr.highest) {

    this->unlock_addr(lock_addr, tag, cas_buffer, cxt, coro_id, true);

    assert(page->hdr.sibling_ptr != GlobalAddress::Null());

    this->internal_page_store(page->hdr.sibling_ptr, k, v, root, level, cxt,
                              coro_id);

    return;
  }
  assert(k >= page->hdr.lowest);

  auto cnt = page->hdr.last_index + 1;
  assert(page->records[page->hdr.last_index].ptr != GlobalAddress::Null());
  bool is_update = false;
  uint16_t insert_index = 0;
  //TODO: Make it a binary search.
  for (int i = cnt - 1; i >= 0; --i) {
    if (page->records[i].key == k) { // find and update
      page->records[i].ptr = v;
      // assert(false);
      is_update = true;
      break;
    }
    if (page->records[i].key < k) {
      insert_index = i + 1;
      break;
    }
  }
  assert(cnt != kInternalCardinality);

  if (!is_update) { // insert and shift
    for (int i = cnt; i > insert_index; --i) {
      page->records[i].key = page->records[i - 1].key;
      page->records[i].ptr = page->records[i - 1].ptr;
    }
    page->records[insert_index].key = k;
    page->records[insert_index].ptr = v;

    page->hdr.last_index++;
  }
  assert(page->records[page->hdr.last_index].ptr != GlobalAddress::Null());
  assert(page->records[page->hdr.last_index].key != 0);

  cnt = page->hdr.last_index + 1;
  bool need_split = cnt == kInternalCardinality;
  Key split_key;
  GlobalAddress sibling_addr;
  // THe internal node is different from leaf nodes because it has the
  // leftmost_ptr. THe internal nodes has n key but n+1 global pointers.
  // the internal node split pick the middle key as split key and it
  // will not existed in either of the splited node
  if (need_split) { // need split
    sibling_addr = dsm->alloc(kInternalPageSize);
    auto sibling_buf = rbuf.get_sibling_buffer();

    auto sibling = new (sibling_buf) InternalPage(page->hdr.level);

    //    std::cout << "addr " <<  sibling_addr << " | level " <<
    //    (int)(page->hdr.level) << std::endl;
      int m = cnt / 2;
      split_key = page->records[m].key;
      assert(split_key > page->hdr.lowest);
      assert(split_key < page->hdr.highest);
      for (int i = m + 1; i < cnt; ++i) { // move
          sibling->records[i - m - 1].key = page->records[i].key;
          sibling->records[i - m - 1].ptr = page->records[i].ptr;
      }
      page->hdr.last_index -= (cnt - m); // this is correct.
      assert(page->hdr.last_index == m-1);
      sibling->hdr.last_index += (cnt - m - 1);
      assert(sibling->hdr.last_index == cnt - m - 1 - 1);
      sibling->hdr.leftmost_ptr = page->records[m].ptr;
      sibling->hdr.lowest = page->records[m].key;
      sibling->hdr.highest = page->hdr.highest;
      page->hdr.highest = page->records[m].key;

      // link
      sibling->hdr.sibling_ptr = page->hdr.sibling_ptr;
      page->hdr.sibling_ptr = sibling_addr;
    sibling->set_consistent();
    //the code below is just for debugging.
//    sibling_addr.mark = 2;

    dsm->write_sync(sibling_buf, sibling_addr, kInternalPageSize, cxt);
      assert(sibling->records[sibling->hdr.last_index].ptr != GlobalAddress::Null());
      assert(page->records[page->hdr.last_index].ptr != GlobalAddress::Null());

  }
//  assert(page->records[page->hdr.last_index].ptr != GlobalAddress::Null());


    page->set_consistent();
  write_page_and_unlock(page_buffer, page_addr, kInternalPageSize, cas_buffer,
                        lock_addr, tag, cxt, coro_id, need_split);

  if (!need_split)
    return;

  if (root == page_addr) { // update root

    if (update_new_root(page_addr, split_key, sibling_addr, level + 1, root,
                        cxt, coro_id)) {
      return;
    }
  }

  auto up_level = path_stack[coro_id][level + 1];

  if (up_level != GlobalAddress::Null()) {
    internal_page_store(up_level, split_key, sibling_addr, root, level + 1, cxt,
                        coro_id);
  } else {
      insert_internal(split_key, sibling_addr, cxt, coro_id, level + 1);
    assert(false);
  }
}

bool Tree::leaf_page_store(GlobalAddress page_addr, const Key &k,
                           const Value &v, GlobalAddress root, int level,
                           CoroContext *cxt, int coro_id, bool from_cache) {

  uint64_t lock_index =
      CityHash64((char *)&page_addr, sizeof(page_addr)) % define::kNumOfLock;

  GlobalAddress lock_addr;


    char padding[VALUE_PADDING];
#ifdef CONFIG_ENABLE_EMBEDDING_LOCK
  lock_addr = page_addr;
#else
  lock_addr.nodeID = page_addr.nodeID;
  lock_addr.offset = lock_index * sizeof(uint64_t);
#endif

  auto &rbuf = dsm->get_rbuf(coro_id);
  uint64_t *cas_buffer = rbuf.get_cas_buffer();
  auto page_buffer = rbuf.get_page_buffer();

  auto tag = dsm->getThreadTag();
  assert(tag != 0);

  lock_and_read_page(page_buffer, page_addr, kLeafPageSize, cas_buffer,

                     lock_addr, tag, cxt, coro_id);

  auto page = (LeafPage *)page_buffer;

  assert(page->hdr.level == level);
  assert(page->check_consistent());

  if (from_cache &&
      (k < page->hdr.lowest || k >= page->hdr.highest)) { // cache is stale
    this->unlock_addr(lock_addr, tag, cas_buffer, cxt, coro_id, true);

    // if (enter_debug) {
    //   printf("cache {%lu} %lu [%lu %lu]\n", page_addr.val, k,
    //   page->hdr.lowest,
    //          page->hdr.highest);
    // }

    return false;
  }

  // if (enter_debug) {
  //   printf("{%lu} %lu [%lu %lu]\n", page_addr.val, k, page->hdr.lowest,
  //          page->hdr.highest);
  // }

  if (k >= page->hdr.highest) {

    this->unlock_addr(lock_addr, tag, cas_buffer, cxt, coro_id, true);

    assert(page->hdr.sibling_ptr != GlobalAddress::Null());

    this->leaf_page_store(page->hdr.sibling_ptr, k, v, root, level, cxt,
                          coro_id);

    return true;
  }
  assert(k >= page->hdr.lowest);

  int cnt = 0;
  int empty_index = -1;
  char *update_addr = nullptr;
    // It is problematic to just check whether the value is empty, because it is possible
    // that the buffer is not initialized as 0
  for (int i = 0; i < kLeafCardinality; ++i) {

    auto &r = page->records[i];
    if (r.value != kValueNull) {
      cnt++;
      if (r.key == k) {
        r.value = v;
#ifdef PADDED_VALUE
        // ADD MORE weight for write.
        memcpy(r.value_padding, padding, VALUE_PADDING);
#endif
        r.f_version++;
        r.r_version = r.f_version;
        update_addr = (char *)&r;
        break;
      }
    } else if (empty_index == -1) {
      empty_index = i;
    }
  }

  assert(cnt != kLeafCardinality);

  if (update_addr == nullptr) { // insert new item
    if (empty_index == -1) {
      printf("%d cnt\n", cnt);
      assert(false);
    }

    auto &r = page->records[empty_index];
    r.key = k;
    r.value = v;
#ifdef PADDED_VALUE
    memcpy(r.value_padding, padding, VALUE_PADDING);
#endif
    r.f_version++;
    r.r_version = r.f_version;

    update_addr = (char *)&r;

    cnt++;
  }

  bool need_split = cnt == kLeafCardinality;
  if (!need_split) {
    assert(update_addr);
    write_page_and_unlock(
        update_addr, GADD(page_addr, (update_addr - (char *)page)),
        sizeof(LeafEntry), cas_buffer, lock_addr, tag, cxt, coro_id, false);

    return true;
  } else {
    std::sort(
        page->records, page->records + kLeafCardinality,
        [](const LeafEntry &a, const LeafEntry &b) { return a.key < b.key; });
  }


  Key split_key;
  GlobalAddress sibling_addr;
  if (need_split) { // need split
    sibling_addr = dsm->alloc(kLeafPageSize);
    auto sibling_buf = rbuf.get_sibling_buffer();

    auto sibling = new (sibling_buf) LeafPage(page->hdr.level);

    // std::cout << "addr " <<  sibling_addr << " | level " <<
    // (int)(page->hdr.level) << std::endl;

      int m = cnt / 2;
      split_key = page->records[m].key;
      assert(split_key > page->hdr.lowest);
      assert(split_key < page->hdr.highest);

      for (int i = m; i < cnt; ++i) { // move
          sibling->records[i - m].key = page->records[i].key;
          sibling->records[i - m].value = page->records[i].value;
          page->records[i].key = 0;
          page->records[i].value = kValueNull;
      }
      //We don't care about the last index in the leaf nodes actually,
      // because we iterate all the slots to find an entry.
      page->hdr.last_index -= (cnt - m);
//      assert(page_addr == root || page->hdr.last_index == m-1);
      sibling->hdr.last_index += (cnt - m);
//      assert(sibling->hdr.last_index == cnt -m -1);
      sibling->hdr.lowest = split_key;// the lowest for leaf node is the lowest that this node contain
      sibling->hdr.highest = page->hdr.highest;
      page->hdr.highest = split_key;

      // link
      sibling->hdr.sibling_ptr = page->hdr.sibling_ptr;
      page->hdr.sibling_ptr = sibling_addr;
    sibling->set_consistent();
    dsm->write_sync(sibling_buf, sibling_addr, kLeafPageSize, cxt);
  }

  page->set_consistent();
    // why need split make the write and locking async?
  write_page_and_unlock(page_buffer, page_addr, kLeafPageSize, cas_buffer,
                        lock_addr, tag, cxt, coro_id, need_split);

  if (!need_split)
    return true;
  // note: there will be a bug for the concurrent root update. because the root is not guaranteed to be the same
  // when split pop up to the root node. Causing two nodes.
  if (root == page_addr) { // update root
    if (update_new_root(page_addr, split_key, sibling_addr, level + 1, root,
                        cxt, coro_id)) {
      return true;
    }
  }

  auto up_level = path_stack[coro_id][level + 1];

  if (up_level != GlobalAddress::Null()) {

    internal_page_store(up_level, split_key, sibling_addr, root, level + 1, cxt,
                        coro_id);
  } else {
    assert(from_cache);
    //If the program comes here, then it could be dangerous
    insert_internal(split_key, sibling_addr, cxt, coro_id, level + 1);
  }

  return true;
}

// Need BIG FIX
void Tree::leaf_page_del(GlobalAddress page_addr, const Key &k, int level,
                         CoroContext *cxt, int coro_id) {
  uint64_t lock_index =
      CityHash64((char *)&page_addr, sizeof(page_addr)) % define::kNumOfLock;

  GlobalAddress lock_addr;
  lock_addr.nodeID = page_addr.nodeID;
  lock_addr.offset = lock_index * sizeof(uint64_t);

  uint64_t *cas_buffer = dsm->get_rbuf(coro_id).get_cas_buffer();

  auto tag = dsm->getThreadTag();
  try_lock_addr(lock_addr, tag, cas_buffer, cxt, coro_id);

  auto page_buffer = dsm->get_rbuf(coro_id).get_page_buffer();
  dsm->read_sync(page_buffer, page_addr, kLeafPageSize, cxt);
  auto page = (LeafPage *)page_buffer;

  assert(page->hdr.level == level);
  assert(page->check_consistent());
  if (k >= page->hdr.highest) {
    this->unlock_addr(lock_addr, tag, cas_buffer, cxt, coro_id, true);

    assert(page->hdr.sibling_ptr != GlobalAddress::Null());

    this->leaf_page_del(page->hdr.sibling_ptr, k, level, cxt, coro_id);
  }

  auto cnt = page->hdr.last_index + 1;

  int del_index = -1;
  for (int i = 0; i < cnt; ++i) {
    if (page->records[i].key == k) { // find and update
      del_index = i;
      break;
    }
  }

  if (del_index != -1) { // remove and shift
    for (int i = del_index + 1; i < cnt; ++i) {
      page->records[i - 1].key = page->records[i].key;
      page->records[i - 1].value = page->records[i].value;
    }

    page->hdr.last_index--;

    page->set_consistent();
    dsm->write_sync(page_buffer, page_addr, kLeafPageSize, cxt);
  }
  this->unlock_addr(lock_addr, tag, cas_buffer, cxt, coro_id, false);
}

void Tree::run_coroutine(CoroFunc func, int id, int coro_cnt) {

  using namespace std::placeholders;

  assert(coro_cnt <= define::kMaxCoro);
  for (int i = 0; i < coro_cnt; ++i) {
    auto gen = func(i, dsm, id);
    worker[i] = CoroCall(std::bind(&Tree::coro_worker, this, _1, gen, i));
  }

  master = CoroCall(std::bind(&Tree::coro_master, this, _1, coro_cnt));

  master();
}

void Tree::coro_worker(CoroYield &yield, RequstGen *gen, int coro_id) {
  CoroContext ctx;
  ctx.coro_id = coro_id;
  ctx.master = &master;
  ctx.yield = &yield;

  Timer coro_timer;
  auto thread_id = dsm->getMyThreadID();

  while (true) {

    auto r = gen->next();

    coro_timer.begin();
    if (r.is_search) {
      Value v;
      this->search(r.k, v, &ctx, coro_id);
    } else {
      this->insert(r.k, r.v, &ctx, coro_id);
    }
    auto us_10 = coro_timer.end() / 100;
    if (us_10 >= LATENCY_WINDOWS) {
      us_10 = LATENCY_WINDOWS - 1;
    }
    latency[thread_id][us_10]++;
  }
}

void Tree::coro_master(CoroYield &yield, int coro_cnt) {

  for (int i = 0; i < coro_cnt; ++i) {
    yield(worker[i]);
  }

  while (true) {

    uint64_t next_coro_id;

    if (dsm->poll_rdma_cq_once(next_coro_id)) {
      yield(worker[next_coro_id]);
    }

    if (!hot_wait_queue.empty()) {
      next_coro_id = hot_wait_queue.front();
      hot_wait_queue.pop();
      yield(worker[next_coro_id]);
    }

    if (!deadline_queue.empty()) {
      auto now = timer.get_time_ns();
      auto task = deadline_queue.top();
      if (now > task.deadline) {
        deadline_queue.pop();
        yield(worker[task.coro_id]);
      }
    }
  }
}

// Local Locks
inline bool Tree::acquire_local_lock(GlobalAddress lock_addr, CoroContext *cxt,
                                     int coro_id) {
  auto &node = local_locks[lock_addr.nodeID][lock_addr.offset / 8];
  bool is_local_locked = false;

  uint64_t lock_val = node.ticket_lock.fetch_add(1);

  uint32_t ticket = lock_val << 32 >> 32;//clear the former 32 bit
  uint32_t current = lock_val >> 32;// current is the former 32 bit in ticket lock

  // printf("%ud %ud\n", ticket, current);
  while (ticket != current) { // lock failed
    is_local_locked = true;

    if (cxt != nullptr) {
      hot_wait_queue.push(coro_id);
      (*cxt->yield)(*cxt->master);
    }

    current = node.ticket_lock.load(std::memory_order_relaxed) >> 32;
  }

  if (is_local_locked) {
    hierarchy_lock[dsm->getMyThreadID()][0]++;
  }

  node.hand_time++;

  return node.hand_over;
}

inline bool Tree::can_hand_over(GlobalAddress lock_addr) {

  auto &node = local_locks[lock_addr.nodeID][lock_addr.offset / 8];
  uint64_t lock_val = node.ticket_lock.load(std::memory_order_relaxed);
// only when unlocking, it need to check whether it can handover to the next, so that it do not need to UNLOCK the global lock.
// It is possible that the handover is set as false but this server is still holding the lock.
  uint32_t ticket = lock_val << 32 >> 32;//
  uint32_t current = lock_val >> 32;
// if the handover in node is true, then the other thread can get the lock without any RDMAcas
// if the handover in node is false, then the other thread will acquire the lock from by RDMA cas AGAIN
  if (ticket <= current + 1) { // no pending locks
    node.hand_over = false;// if no pending thread, then it will release the remote lock and next aquir need RDMA CAS again
  } else {
    node.hand_over = node.hand_time < define::kMaxHandOverTime; // check the limit
  }
  if (!node.hand_over) {
    node.hand_time = 0;// clear the handtime.
  } else {
    handover_count[dsm->getMyThreadID()][0]++;
  }

  return node.hand_over;
}

inline void Tree::releases_local_lock(GlobalAddress lock_addr) {
  auto &node = local_locks[lock_addr.nodeID][lock_addr.offset / 8];

  node.ticket_lock.fetch_add((1ull << 32));
}

void Tree::index_cache_statistics() {
  index_cache->statistics();
  index_cache->bench();
}

void Tree::clear_statistics() {
  for (int i = 0; i < MAX_APP_THREAD; ++i) {
    cache_hit[i][0] = 0;
    cache_miss[i][0] = 0;
  }
}