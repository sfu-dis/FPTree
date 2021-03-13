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
#include <tuple>
#include <utility>
#include <time.h>  
#include <atomic>

#include <cassert>

#pragma once

// static const uint64_t kMaxEntries = 256;
#define MAX_INNER_SIZE 3
#define MAX_LEAF_SIZE 4

static size_t getOneByteHash(uint64_t key);

class BaseNode
{
protected:
    bool isInnerNode;

    friend class FPtree;

public:
    BaseNode();
};



class InnerNode : public BaseNode
{
    uint64_t nKey;
    std::array<uint64_t, MAX_INNER_SIZE> keys;
    std::array<BaseNode*, MAX_INNER_SIZE + 1> p_children;

    friend class FPtree;

public:
    InnerNode();
    InnerNode(const InnerNode& inner);

    // uint64_t: index of child that would lie along the path of searching key
    // bool: whether current node actually contains key in keys
    std::pair<uint64_t, bool> findChildIndex(uint64_t key);

    // remove key at index, default remove right child (or left child if false) 
    void removeKey(uint64_t index, bool remove_right_child);

    // add key at index pos, default add child to the right
    void addKey(uint64_t index, uint64_t key, BaseNode* child, bool add_child_right);
};



struct KV {
    uint64_t key;
    uint64_t value;

    KV() {}
    KV(uint64_t key, uint64_t value) { this->key = key; this->value = value; }
};

class LeafNode : public BaseNode
{
    std::bitset<MAX_LEAF_SIZE> bitmap;
    LeafNode* p_next;
    std::array<size_t, MAX_LEAF_SIZE> fingerprints;
    std::array<KV, MAX_LEAF_SIZE> kv_pairs;
    std::atomic<bool> lock;

    friend class FPtree;

public:
    LeafNode();
    LeafNode(const LeafNode& leaf);

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

    void sortKV();
};



class FPtree 
{
    BaseNode *root;

public:
    FPtree();
    ~FPtree();

    BaseNode* getRoot () { return this->root; }

    void displayTree(BaseNode* root);

    void printFPTree(std::string prefix, BaseNode* root);

    // return 0 if key not found, otherwise return value associated with key
    uint64_t find(uint64_t key);

    // return false if kv.key not found, otherwise update value associated with key
    bool update(struct KV kv);

    // return false if key already exists, otherwise insert kv
    bool insert(struct KV kv);

    // delete key from tree and return associated value 
    uint64_t deleteKey(uint64_t key);

    // initialize scan by finding the first kv with kv.key >= key
    void ScanInitialize(uint64_t key);

    KV ScanNext();

    bool ScanComplete();

private:
    // find leaf that could potentially contain the key, the returned leaf is not garanteed to contain the key
    LeafNode* findLeaf(uint64_t key);

    //find leaf node that could potentially contain the key and its immediate parent
    std::pair<InnerNode*, LeafNode*> findLeafWithParent(uint64_t key);

    // First InnerNode*: innernode that contains key in keys, nullptr if no such innernode
    // Second InnerNode*: immediate parent of leaf node
    // uint64_t: p_children index of parent that could potentially contain key
    std::tuple<InnerNode*, InnerNode*, uint64_t> findInnerAndLeafWithParent(uint64_t key);

    // find immediate parent of child and the child index, child != root
    std::pair<InnerNode*, uint64_t> findInnerNodeParent(InnerNode* child);

    uint64_t findSplitKey(LeafNode* leaf);

    uint64_t splitLeaf(LeafNode* leaf);

    void updateParents(uint64_t splitKey, InnerNode* parent, BaseNode* leaf);

    // if parent's children are leaf nodes, assume left child has one child at index 0, right child empty
    // if parent's children are inner nodes, assume the child with no key has one child at index 0
    void mergeNodes(InnerNode* parent, uint64_t left_child_idx, uint64_t right_child_idx);

    // try transfer a key from sender to receiver, sender and receiver should be immediate siblings 
    // If receiver & sender are inner nodes, will assume the only child in receiver is at index 0
    // return false if cannot borrow key from sender
    bool tryBorrowKey(InnerNode* parent, uint64_t receiver_idx, uint64_t sender_idx);

    uint64_t minKey(BaseNode* node);


    LeafNode* current_leaf;
    uint64_t bitmap_idx;
};
