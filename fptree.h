#pragma once
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <sys/types.h>
#include <bits/hash_bytes.h>
#include <unistd.h>
#include <time.h>
#include <string.h>
#include <immintrin.h>
#include <tbb/spin_mutex.h>
#include <tbb/spin_rw_mutex.h>
#include <iostream>
#include <string>
#include <cstdint>
#include <cstring>
#include <cmath>
#include <algorithm>
#include <array>
#include <utility>
#include <tuple>
#include <atomic>
#include <queue>
#include <limits>
#include <chrono>
#include <vector>
#include <random>
#include <climits>
#include <functional>
#include <cassert>
#include <thread>
#include <boost/lockfree/queue.hpp>

#ifdef TEST_MODE
    #define MAX_INNER_SIZE 3
    #define MAX_LEAF_SIZE 4
    #define SIZE_ONE_BYTE_HASH 1
    #define SIZE_PMEM_POINTER 16
#else
    #define MAX_INNER_SIZE 128
    #define MAX_LEAF_SIZE 64
    #define SIZE_ONE_BYTE_HASH 1
    #define SIZE_PMEM_POINTER 16
#endif

#if MAX_LEAF_SIZE > 64
    #error "Number of kv pairs in LeafNode must be <= 64."
#endif

static const uint64_t offset = std::numeric_limits<uint64_t>::max() >> (64 - MAX_LEAF_SIZE);

enum Result { Insert, Update, Split, Abort, Delete, Remove, NotFound };

#ifdef PMEM
    #include <libpmemobj.h>

    #define PMEMOBJ_POOL_SIZE ((size_t)(1024 * 1024 * 11) * 1000)  /* 11 GB */

    POBJ_LAYOUT_BEGIN(FPtree);
    POBJ_LAYOUT_ROOT(FPtree, struct List);
    POBJ_LAYOUT_TOID(FPtree, struct LeafNode);
    POBJ_LAYOUT_END(FPtree);

    POBJ_LAYOUT_BEGIN(Array);
    POBJ_LAYOUT_TOID(Array, struct Log);
    POBJ_LAYOUT_END(Array);

    inline PMEMobjpool *pop;
#endif

static uint8_t getOneByteHash(uint64_t key);

struct KV
{
    uint64_t key;
    uint64_t value;

    KV() {}
    KV(uint64_t key, uint64_t value) { this->key = key; this->value = value; }
};

struct LeafNodeStat
{
    uint64_t kv_idx;    // bitmap index of key
    uint64_t count;     // number of kv in leaf 
    uint64_t min_key;   // min key excluding key
};

// This Bitset class implements bitmap of size <= 64
// The bitmap iterates from right to left - starting from least significant bit
// off contains 0 on some significant bits when bitmap size < 64
class Bitset
{
 public:
    uint64_t bits;

    Bitset()
    {
        bits = 0;
    }

    ~Bitset() {}

    Bitset(const Bitset& bts)
    {
        bits = bts.bits;
    }

    Bitset& operator=(const Bitset& bts)
    {
        bits = bts.bits;
        return *this;
    }

    inline void set(const size_t pos)
    {
        bits |= ((uint64_t)1 << pos);
    }

    inline void reset(const size_t pos)
    {
        bits &= ~((uint64_t)1 << pos);
    }

    inline void clear()
    {
        bits = 0;
    }

    inline bool test(const size_t pos) const
    {
        return bits & ((uint64_t)1 << pos);
    }

    inline void flip()
    {
        bits ^= offset;
    }

    inline bool is_full()
    {
        return bits == offset;
    }

    inline size_t count() 
    {
        return __builtin_popcountl(bits);
    }

    inline size_t first_set()
    {
        size_t idx = __builtin_ffsl(bits);
        return idx ? idx - 1 : MAX_LEAF_SIZE;
    }

    inline size_t first_zero() 
    {
        size_t idx = __builtin_ffsl(bits ^ offset);
        return idx? idx - 1 : MAX_LEAF_SIZE;
    }

    void print_bits()
    {
        for (uint64_t i = 0; i < 64; i++) { std::cout << ((bits >> i) & 1); }
        std::cout << std::endl;
    }
};


/*******************************************************
                  Define node struture 
********************************************************/


struct BaseNode
{
    bool isInnerNode;

    friend class FPtree;

 public:
    BaseNode();
} __attribute__((aligned(64)));



struct InnerNode : BaseNode
{
    uint64_t nKey;
    uint64_t keys[MAX_INNER_SIZE];
    BaseNode* p_children[MAX_INNER_SIZE + 1];

    friend class FPtree;

 public:
    InnerNode();
    InnerNode(uint64_t key, BaseNode* left, BaseNode* right);
    InnerNode(const InnerNode& inner);
    ~InnerNode();

    // for using mempool only where constructor is not called 
    void init(uint64_t key, BaseNode* left, BaseNode* right);

    // return index of child in p_children when searching key in this innernode
    uint64_t findChildIndex(uint64_t key);

    // remove key at index, default remove right child (or left child if false)
    void removeKey(uint64_t index, bool remove_right_child);

    // add key at index pos, default add child to the right
    void addKey(uint64_t index, uint64_t key, BaseNode* child, bool add_child_right);
} __attribute__((aligned(64)));


struct LeafNode : BaseNode
{
    __attribute__((aligned(64))) uint8_t fingerprints[MAX_LEAF_SIZE];
    Bitset bitmap;

    KV kv_pairs[MAX_LEAF_SIZE];

    #ifdef PMEM
        TOID(struct LeafNode) p_next;
    #else
        LeafNode* p_next;
    #endif

    std::atomic<uint64_t> lock;

    friend class FPtree;

 public:
    #ifndef PMEM
        LeafNode();
        LeafNode(const LeafNode& leaf);
        LeafNode& operator=(const LeafNode& leaf);
    #endif

    inline bool isFull() { return this->bitmap.is_full(); }

    void addKV(struct KV kv);

    // find index of kv that has kv.key = key, return MAX_LEAF_SIZE if key not found
    uint64_t findKVIndex(uint64_t key);

    // return min key in leaf
    uint64_t minKey();

    bool Lock()
    {
        uint64_t expected = 0;
        return std::atomic_compare_exchange_strong(&lock, &expected, 1);
    }
    void Unlock()
    {
        this->lock = 0;
    }

    void getStat(uint64_t key, LeafNodeStat& lstat);
} __attribute__((aligned(64)));


#ifdef PMEM
    struct argLeafNode
    {
        size_t size;
        bool isInnerNode;
        Bitset bitmap;
        __attribute__((aligned(64))) uint8_t fingerprints[MAX_LEAF_SIZE];
        KV kv_pairs[MAX_LEAF_SIZE];
        uint64_t lock;

        argLeafNode(LeafNode* leaf)
        {
            isInnerNode = false;
            size = sizeof(struct LeafNode);
            memcpy(fingerprints, leaf->fingerprints, sizeof(leaf->fingerprints));
            memcpy(kv_pairs, leaf->kv_pairs, sizeof(leaf->kv_pairs));
            bitmap = leaf->bitmap;
            lock = 1;
        }

        argLeafNode(struct KV kv)
        {
            isInnerNode = false;
            size = sizeof(struct LeafNode);
            kv_pairs[0] = kv;
            fingerprints[0] = getOneByteHash(kv.key);
            bitmap.set(0);
            lock = 0;
        }
    };

    static int constructLeafNode(PMEMobjpool *pop, void *ptr, void *arg);

    struct List
    {
        TOID(struct LeafNode) head;
    };

/*
    uLog
*/
    struct Log
    {
        TOID(struct LeafNode) PCurrentLeaf;
        TOID(struct LeafNode) PLeaf;
    };

    static const uint64_t sizeLogArray = 128;

    static boost::lockfree::queue<Log*> splitLogQueue = boost::lockfree::queue<Log*>(sizeLogArray);
    static boost::lockfree::queue<Log*> deleteLogQueue = boost::lockfree::queue<Log*>(sizeLogArray);
#endif


struct Stack //TODO: Get rid of Stack
{
 public:
    static const uint64_t kMaxNodes = 32;
    InnerNode* innerNodes[kMaxNodes];
    uint64_t num_nodes;

    Stack() : num_nodes(0) {}
    ~Stack() { num_nodes = 0; }

    inline void push(InnerNode* node)
    {
        assert(num_nodes < kMaxNodes && "Error: exceed max stack size");
        this->innerNodes[num_nodes++] = node;
    }

    InnerNode* pop() { return num_nodes == 0 ? nullptr : this->innerNodes[--num_nodes]; }

    inline bool isEmpty() { return num_nodes == 0; }

    inline InnerNode* top() { return num_nodes == 0 ? nullptr : this->innerNodes[num_nodes - 1]; }

    inline void clear() { num_nodes = 0; }
};

static thread_local Stack stack_innerNodes;
static thread_local InnerNode* inners[32];
static thread_local short ppos[32];

struct FPtree
{
    BaseNode *root;
    tbb::speculative_spin_rw_mutex speculative_lock;

    InnerNode* right_most_innnerNode; // for bulkload

 public:
    FPtree();
    ~FPtree();

    BaseNode* getRoot () { return this->root; }

    void printFPTree(std::string prefix, BaseNode *root);

    // return flse if kv.key not found, otherwise set kv.value to value associated with kv.key
    uint64_t find(uint64_t key);

    // return false if kv.key not found, otherwise update value associated with key
    bool update(struct KV kv);

    // return false if key already exists, otherwise insert kv
    bool insert(struct KV kv);

    // delete key from tree
    bool deleteKey(uint64_t key);

    // TODO: fix/optimize iterator based scan methods
    // initialize scan by finding the first kv with kv.key >= key
    void scanInitialize(uint64_t key);

    KV scanNext();

    bool scanComplete();

    uint64_t rangeScan(uint64_t key, uint64_t scan_size, char*& result);

    #ifdef PMEM
        bool bulkLoad(float load_factor);

        void recoverSplit(Log* uLog);

        void recoverDelete(Log* uLog);

        void recover();

        void showList();
    #endif

 private:
    // return leaf that may contain key, does not push inner nodes
    LeafNode* findLeaf(uint64_t key);

    // return leaf that may contain key, push all innernodes on traversal path into stack
    LeafNode* findLeafAndPushInnerNodes(uint64_t key);

    uint64_t findSplitKey(LeafNode* leaf);

    uint64_t splitLeaf(LeafNode* leaf);

    void updateParents(uint64_t splitKey, InnerNode* parent, BaseNode* leaf);

    void splitLeafAndUpdateInnerParents(LeafNode* reachedLeafNode, Result decision, struct KV kv, 
                                                                bool updateFunc, uint64_t prevPos);

    // merge parent with sibling, may incur further merges. Remove key from indexNode after
    void removeLeafAndMergeInnerNodes(short i, short indexNode_level);

    // try transfer a key from sender to receiver, sender and receiver should be immediate siblings
    // If receiver & sender are inner nodes, will assume the only child in receiver is at index 0
    // return false if cannot borrow key from sender
    bool tryBorrowKey(InnerNode* parent, uint64_t receiver_idx, uint64_t sender_idx);

    uint64_t minKey(BaseNode* node);

    LeafNode* minLeaf(BaseNode* node);

    LeafNode* maxLeaf(BaseNode* node);

    void sortKV();

    uint64_t size_volatile_kv;
    KV volatile_current_kv[MAX_LEAF_SIZE];

    LeafNode* current_leaf;
    uint64_t bitmap_idx;

    friend class Inspector;
};
