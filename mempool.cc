/**
 * @file mempool.cc
 * @author  Shimin Chen <shimin.chen@gmail.com>, Jihang Liu, Leying Chen
 * @version 1.0
 *
 * @section LICENSE
 *
 * TBD
 *
 * @section DESCRIPTION
 *
 * The mempool class implements a memory pool for experiments purpose.  It will
 * allocate a contiguous region of DRAM from OS.  Then it will serve memory
 * allocation requests from this memory pool.  Freed btree nodes will be
 * appended into a linked list for future reuse.
 *
 * The memory pool is divided into segments.  Each worker thread has its own 
 * segment of the memory pool to reduce contention.
 */

#include "mempool.h"

thread_local int worker_id = -1; /* in Thread Local Storage */

threadMemPools the_thread_mempools;
threadNVMPools the_thread_nvmpools;

#ifdef MEMPOOL
/* -------------------------------------------------------------- */
void threadMemPools::init(int num_workers, long long size, long long align)
{
    assert((num_workers > 0) && (size > 0) && (align > 0) & ((align & (align - 1)) == 0));

    // 1. allocate memory
    tm_num_workers = num_workers;
    tm_pools = new mempool[tm_num_workers];

    long long load_pool_size = (1024ll * 1024LL * 1024ll * 3ll / align) * align; // 3 GB for 100M loading
	long long size_per_pool = ((size - load_pool_size) / (long long) tm_num_workers / align) * align;
    printf("DRAM pool size for load (first) thread: %lld \n", load_pool_size + size_per_pool);
    printf("DRAM pool size for each other worker thread: %lld \n", size_per_pool);
    tm_size = size_per_pool * (long long) tm_num_workers + load_pool_size;
    printf("Total DRAM pool size: %lld \n", tm_size);
    tm_buf = (char *)memalign(align, tm_size);
    if (!tm_buf || !tm_pools)
    {
        perror("malloc");
        exit(1);
    }

    // 2. initialize memory pools
    char name[80];
    for (int i = 0; i < tm_num_workers; i++)
    {
        sprintf(name, "DRAM pool %d", i);
        if (i)
            tm_pools[i].init(tm_buf + load_pool_size + i * size_per_pool, size_per_pool, align, strdup(name));
        else
            tm_pools[0].init(tm_buf, load_pool_size + size_per_pool, align, strdup(name));
    }

    // 3. touch every page to make sure that they are allocated
    for (long long i = 0; i < tm_size; i += 4096)
    {
        tm_buf[i] = 1;
    }
}

void threadMemPools::print(void)
{
    if (tm_pools == NULL)
    {
        printf("Error: threadMemPools is not yet initialized!\n");
        return;
    }

    printf("threadMemPools\n");
    printf("--------------------\n");
    for (int i = 0; i < tm_num_workers; i++)
    {
        tm_pools[i].print_params();
        tm_pools[i].print_free_nodes();
        printf("--------------------\n");
    }
}

void threadMemPools::print_usage(void)
{
    printf("threadMemPools\n");
    printf("--------------------\n");
    for (int i = 0; i < tm_num_workers; i++)
    {
        tm_pools[i].print_usage();
    }
    printf("--------------------\n");
}

/* -------------------------------------------------------------- */
threadNVMPools::~threadNVMPools()
{
    if (tm_buf)
    {
#ifdef NVMPOOL_REAL
        pmem_unmap(tm_buf, tm_size);
#else // NVMPOOL_REAL not defined, use DRAM memory
        free(tm_buf);
#endif
        tm_buf = NULL;
    }
    if (tm_pools)
    {
        delete[] tm_pools;
        tm_pools = NULL;
    }
}

/**
 * get the sigbus signal
 */
static void handleSigbus(int sig)
{
    printf("SIGBUS %d is received\n", sig);
    _exit(0);
}

void threadNVMPools::init(int num_workers, const char *nvm_file, long long size)
{
    // map_addr must be 4KB aligned, size must be multiple of 4KB
    assert((num_workers > 0) && (size > 0) && (size % 4096 == 0));

    // set sigbus handler
    signal(SIGBUS, handleSigbus);

    // 1. allocate memory
    tm_num_workers = num_workers;
    tm_pools = new mempool[tm_num_workers];
    if (!tm_pools)
    {
        perror("malloc");
        exit(1);
    }

    tn_nvm_file = nvm_file;
    
    long long load_pool_size = (1024ll * 1024LL * 1024ll * 7ll / 4096LL) * 4096LL; // 7 GB for 100M loading
	long long size_per_pool = ((size - load_pool_size) / (long long) tm_num_workers / 4096LL) * 4096LL;
    printf("NVM pool size for load (first) thread: %lld \n", load_pool_size + size_per_pool);
    printf("NVM pool size for each other worker thread: %lld \n", size_per_pool);
    tm_size = size_per_pool * (long long) tm_num_workers + load_pool_size;
    printf("Total NVM pool size: %lld \n", tm_size);
#ifdef NVMPOOL_REAL
    printf("Using actual NVM\n");
    // pmdk allows PMEM_MMAP_HINT=map_addr to set the map address

    int is_pmem = false;
    size_t mapped_len = tm_size;

    tm_buf = (char *)pmem_map_file(tn_nvm_file, tm_size, PMEM_FILE_CREATE, 0666, &mapped_len, &is_pmem);
    if (tm_buf == NULL || !is_pmem)
    {
        perror("pmem_map_file");
        exit(1);
    }

    printf("NVM mapping address: %p, size: %ld\n", tm_buf, mapped_len);
    if (tm_size != mapped_len)
    {
        fprintf(stderr, "Error: cannot map %lld bytes\n", tm_size);
        pmem_unmap(tm_buf, mapped_len);
        exit(1);
    }

#else // NVMPOOL_REAL not defined, use DRAM memory
    printf("Using DRAM as NVM\n");
    tm_buf = (char *)memalign(4096, tm_size);
    if (!tm_buf)
    {
        perror("malloc");
        exit(1);
    }

#endif // NVMPOOL_REAL

    // 2. initialize NVM memory pools
    char name[80];
    for (int i = 0; i < tm_num_workers; i++)
    {
        sprintf(name, "NVM pool %d", i);
        if (i)
            tm_pools[i].init(tm_buf + load_pool_size + i * size_per_pool, size_per_pool, 4096LL, strdup(name));
        else
            tm_pools[0].init(tm_buf, load_pool_size + size_per_pool, 4096LL, strdup(name));
    }

    // 3. touch every page to make sure that they are allocated
    for (long long i = 0; i < tm_size; i += 4096)
    {
        tm_buf[i] = 1; // XXX: need a special signature
    }
}

void threadNVMPools::print(void)
{
    if (tm_pools == NULL)
    {
        printf("Error: threadNVMPools is not yet initialized!\n");
        return;
    }

    printf("threadNVMPools\n");
    printf("--------------------\n");
    for (int i = 0; i < tm_num_workers; i++)
    {
        tm_pools[i].print_params();
        tm_pools[i].print_free_nodes();
        printf("--------------------\n");
    }
}

void threadNVMPools::print_usage(void)
{
    printf("threadNVMPools\n");
    printf("--------------------\n");
    for (int i = 0; i < tm_num_workers; i++)
    {
        tm_pools[i].print_usage();
    }
    printf("--------------------\n");
}

#else
void *allocate(unsigned long size)
{
   return new (std::align_val_t(4096)) char[size];
} 

void free(void *p)
{
}
#endif