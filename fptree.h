#include <stdio.h>    
#include <stdlib.h> 
#include <iostream>
#include <string>
#include <cstdint>
#include <stdint.h>
#include <sys/types.h>
#include <bits/hash_bytes.h>
#include <bitset>
#include <cmath>
#include <algorithm>  
#include <array>
#include <unistd.h>
#include <utility>
#include <time.h>   
#include <tuple>
#include <cassert>
#include <atomic>
#include <queue> 
#include <string.h> 
#include <limits>
#include <chrono>
#include <vector>
#include <random>
#include <climits>
#include <functional>
#include <tbb/spin_mutex.h>
#include <tbb/spin_rw_mutex.h>
#include <immintrin.h>
#include <thread>

#include "bitset.h"

#pragma once

#define TEST_MODE 0

#define INSPECT_MODE 0

#define PMEM 

// static const uint64_t kMaxEntries = 256;
#if TEST_MODE == 0
    #define MAX_INNER_SIZE 1024
    #define MAX_LEAF_SIZE 48
    #define SIZE_ONE_BYTE_HASH 1
#else
    #define MAX_INNER_SIZE 3
    #define MAX_LEAF_SIZE 4
    #define SIZE_ONE_BYTE_HASH 1
#endif

const static uint64_t offset = std::numeric_limits<uint64_t>::max() >> (64 - MAX_LEAF_SIZE);

enum Result { Insert, Split, Abort };  

#ifdef PMEM
    #include <libpmemobj.h>

    #define PMEMOBJ_POOL_SIZE ((size_t)(1024 * 1024 * 5) * 1000)  /* 1 * 1000 MiB */

    POBJ_LAYOUT_BEGIN(List);
    POBJ_LAYOUT_ROOT(List, struct List);
    POBJ_LAYOUT_TOID(List, struct LeafNode);
    POBJ_LAYOUT_END(List);

    inline PMEMobjpool *pop;
#endif

static uint8_t getOneByteHash(uint64_t key);


#ifdef PMEM
    uint64_t findFirstZero(TOID(struct LeafNode) *dst);

    static void showList();
#endif


struct KV 
{
    uint64_t key;
    uint64_t value;

    KV() {}
    KV(uint64_t key, uint64_t value) { this->key = key; this->value = value; }
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
    InnerNode(const InnerNode& inner);
    ~InnerNode();

    // return index of child in p_children when searching key in this innernode
    uint64_t findChildIndex(uint64_t key);

    // remove key at index, default remove right child (or left child if false) 
    void removeKey(uint64_t index, bool remove_right_child);

    // add key at index pos, default add child to the right
    void addKey(uint64_t index, uint64_t key, BaseNode* child, bool add_child_right);

    void updateKey(uint64_t key);

} __attribute__((aligned(64)));



struct LeafNode : BaseNode
{
    __attribute__((aligned(64))) uint8_t fingerprints[MAX_LEAF_SIZE];
    std::bitset<MAX_LEAF_SIZE> bitmap;
    // Bitset bitmap;
    
    KV kv_pairs[MAX_LEAF_SIZE];

    #ifdef PMEM
        TOID(struct LeafNode) p_next;
    #else
        LeafNode* p_next;
    #endif
    
    std::atomic<uint64_t> lock;
    //uint64_t lock;

    friend class FPtree;

public:
    #ifdef PMEM
    #else
        LeafNode();
        LeafNode(const LeafNode& leaf);
        LeafNode& operator=(const LeafNode& leaf);
    #endif

    // return position of first unset bit in bitmap, return bitmap size if all bits are set
    uint64_t findFirstZero();

    bool isFull() { return this->bitmap.all(); }

    void addKV(struct KV kv);

    // find kv with kv.key = key and remove it, return kv.value 
    uint64_t removeKV(uint64_t key);

    // remove kv at pos, return kv.value  
    uint64_t removeKVByIdx(uint64_t pos);

    // find index of kv that has kv.key = key, return MAX_LEAF_SIZE if key not found
    uint64_t findKVIndex(uint64_t key);

    // find number of valid kv pairs in leaf
    uint64_t countKV(){ return this->bitmap.count(); }

    // find and optionally remove the min/max kv in leaf
    KV minKV(bool remove);
    KV maxKV(bool remove);


    void _lock() 
    { 
        // uint64_t time = 1;
        // while (!__sync_bool_compare_and_swap(&this->lock, 0, 1)) 
        //     { 
        //         // std::this_thread::sleep_for(std::chrono::milliseconds(time));
        //         // time *= 2;
        //     } 
        uint64_t expected = 0;
        while (!std::atomic_compare_exchange_strong(&lock, &expected, 1))
            expected = 0;
    }
    void _unlock() 
    { 
        uint64_t expected = 1;
        while (!std::atomic_compare_exchange_strong(&lock, &expected, 0))
        {
            std::cout << "Error!\n";
            expected = 1;
        }
        // this->lock = 0; 
    }
} __attribute__((aligned(64)));


#ifdef PMEM
    struct argLeafNode {
        size_t size;
        bool isInnerNode;
        std::bitset<MAX_LEAF_SIZE> bitmap;
        __attribute__((aligned(64))) uint8_t fingerprints[MAX_LEAF_SIZE];
        KV kv_pairs[MAX_LEAF_SIZE];
        uint64_t lock;

        argLeafNode(LeafNode* leaf){
            isInnerNode = false;
            size = sizeof(struct LeafNode);
            memcpy(fingerprints, leaf->fingerprints, sizeof(leaf->fingerprints));
            memcpy(kv_pairs, leaf->kv_pairs, sizeof(leaf->kv_pairs));
            bitmap = leaf->bitmap;
            lock = 1;
        }

        argLeafNode(struct KV kv){
            isInnerNode = false;
            size = sizeof(struct LeafNode);
            kv_pairs[0] = kv;
            fingerprints[0] = getOneByteHash(kv.key);
            bitmap.set(0);
            lock = 0;
        }
    };


    static int constructLeafNode(PMEMobjpool *pop, void *ptr, void *arg);
#endif

#ifdef PMEM
    struct List 
    {
        TOID(struct LeafNode) head;
    };
#endif


struct Stack 
{
public:
    static const uint64_t kMaxNodes = 100;
    InnerNode* innerNodes[kMaxNodes];
    uint64_t num_nodes;

    Stack() : num_nodes(0) {}
    ~Stack() { num_nodes = 0; }

    inline void push(InnerNode* node) 
    {
        assert(num_nodes < kMaxNodes && "Error: exceed max stack size");
        this->innerNodes[num_nodes++] = node;
    }

    inline InnerNode* pop() { return num_nodes == 0 ? nullptr : this->innerNodes[--num_nodes]; }

    inline bool isEmpty() { return num_nodes == 0; }

    inline InnerNode* top() { return num_nodes == 0 ? nullptr : this->innerNodes[num_nodes - 1]; }

    inline void clear() { num_nodes = 0; }
};

static thread_local Stack stack_innerNodes;
static thread_local uint64_t CHILD_IDX;  // the idx of leafnode w.r.t its immediate parent innernode
static thread_local InnerNode* INDEX_NODE; // pointer to inner node that contains key

struct FPtree 
{
    BaseNode *root;
    tbb::speculative_spin_rw_mutex speculative_lock;
    tbb::speculative_spin_rw_mutex speculative_lock_split;

public:

    FPtree();
    ~FPtree();

    BaseNode* getRoot () { return this->root; }

    void displayTree(BaseNode *root);

    void printFPTree(std::string prefix, BaseNode *root);

    // return flse if kv.key not found, otherwise set kv.value to value associated with kv.key
    uint64_t find(uint64_t key);

    // return false if kv.key not found, otherwise update value associated with key
    bool update(struct KV kv);

    // return false if key already exists, otherwise insert kv
    bool insert(struct KV kv);

    // delete key from tree 
    bool deleteKey(uint64_t key);

    // initialize scan by finding the first kv with kv.key >= key
    void ScanInitialize(uint64_t key);

    KV ScanNext();

    bool ScanComplete();

    void printTSXInfo();

    //bool bulkLoad();

private:

    // return leaf that may contain key, does not push inner nodes
    LeafNode* findLeaf(uint64_t key);

    // return leaf that may contain key, push all innernodes on traversal path into stack
    LeafNode* findLeafAndPushInnerNodes(uint64_t key);

    uint64_t findSplitKey(LeafNode* leaf);

    uint64_t splitLeaf(LeafNode* leaf);

    void updateParents(uint64_t splitKey, InnerNode* parent, BaseNode* leaf);

    void splitLeafAndUpdateInnerParents(LeafNode* reachedLeafNode, InnerNode* parentNode, 
                                            Result decision, struct KV kv, bool updateFunc, uint64_t prevPos);

    // merge parent with sibling, may incur further merges. Remove key from indexNode after
    void removeKeyAndMergeInnerNodes(InnerNode* indexNode, InnerNode* parent, uint64_t child_idx, uint64_t key);

    // try transfer a key from sender to receiver, sender and receiver should be immediate siblings 
    // If receiver & sender are inner nodes, will assume the only child in receiver is at index 0
    // return false if cannot borrow key from sender
    bool tryBorrowKey(InnerNode* parent, uint64_t receiver_idx, uint64_t sender_idx);

    uint64_t minKey(BaseNode* node);

    LeafNode* minLeaf(BaseNode* node);

    LeafNode* maxLeaf(BaseNode* node);

    KV volatile_current_kv[MAX_LEAF_SIZE];

    uint64_t size_volatile_kv;
    void sortKV();
    
    LeafNode* current_leaf;
    uint64_t bitmap_idx;

    friend class Inspector;
};
