#ifndef __HUGEPAGEALLOC_H__
#define __HUGEPAGEALLOC_H__


#include <cstdint>

#include <sys/mman.h>
#include <memory.h>


char *getIP();
inline void *hugePageAlloc(size_t size) {

//    void *res = mmap(NULL, size, PROT_READ | PROT_WRITE,
//                     MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
    void *res = malloc(size);
    if (res == MAP_FAILED) {
        Debug::notifyError("%s mmap failed!\n", getIP());
    }

    return res;
}
static bool is_mmap_work = true;
//inline void *hugePageAlloc(size_t size) {
//    /**
//     * mmap will actually go ahead and reserve the pages from the kernel's internal hugetlbfs mount, whose status can be
//     * seen under /sys/kernel/mm/hugepages. The pages in question need to be available by the time mmap is invoked
//     * (see HugePages_Free in /proc/meminfo), or mmap will fail. (https://stackoverflow.com/questions/30470972/using-mmap-and-madvise-for-huge-pages)
//     */
//    void *res = nullptr;
//    int ret = 0;
//    if (is_mmap_work){
////            res = mmap(NULL, size, PROT_READ | PROT_WRITE,
////                       MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
//        void *ptr;
//        ret = posix_memalign(&res, 1 << 21, size);
//        if (ret != 0) {
//            printf("Posix alignment failed\n");
//        }
//        madvise(res, size, MADV_HUGEPAGE);
//
//    }
//
//    if (ret !=0 || res == MAP_FAILED || res == nullptr) {
////            assert(is_mmap_work == true);
//        printf("mmap failed!\n");
//        is_mmap_work = false;
//        printf("Allocate %zu bytes of data\n", size);
//        //Use aligned alloc to enable the atomic variables. aligned to cache line size at least.
//        res = aligned_alloc(128, size);
//        auto set_ret = memset(res, 0,size);
//        assert(set_ret != nullptr);
//        assert(res != nullptr);
//        return res;
//    }else{
//        printf("The returned pointer is %p size is %zu\n", res, size);
//        return res;
//    }
//
//}

#endif /* __HUGEPAGEALLOC_H__ */
