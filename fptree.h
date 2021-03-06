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
#include <utility>
#include <time.h>   

using namespace std;

#pragma once

// static const uint64_t kMaxEntries = 256;
#define MAX_INNER_SIZE 3
#define MAX_LEAF_SIZE 4

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
    
    friend class FPtree;

public:
    LeafNode();
    LeafNode(std::bitset<MAX_LEAF_SIZE> bitmap, LeafNode* p_next, 
        std::array<size_t, MAX_LEAF_SIZE> fingerprints, std::array<KV, MAX_LEAF_SIZE> kv_pairs);
    // LeafNode(const LeafNode* leaf);

    int64_t findFirstZero();
    bool isFull() { return this->findFirstZero() == MAX_LEAF_SIZE; }
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

    // find leaf that could contain the key, the returned leaf may not contain the key
    LeafNode* findLeaf(uint64_t key);

    std::pair<InnerNode*, LeafNode*> findLeafWithParent(uint64_t key);

    InnerNode* findInnerNodeParent(InnerNode* child);

    uint64_t find(uint64_t key);

    bool updateValue(struct KV kv);

    uint64_t findSplitKey(LeafNode* leaf);

    bool insert(struct KV kv);

    uint64_t splitLeaf(LeafNode* leaf);

    void updateParents(uint64_t splitKey, InnerNode* parent, BaseNode* leaf);

};
