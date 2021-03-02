#include <iostream>
#include <string>
#include <cstdint>
#include <stdint.h>

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
    uint64_t keys[MAX_INNER_SIZE];
    BaseNode* p_children[MAX_INNER_SIZE + 1];

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
    bool bitmap[MAX_LEAF_SIZE];
    LeafNode* p_next;
    size_t fingerprints[MAX_LEAF_SIZE];
    KV kv_pairs[MAX_LEAF_SIZE];
    
    friend class FPtree;

public:
    LeafNode();
    LeafNode(uint64_t nKey, uint64_t *keys, bool* bitmap, LeafNode* p_next, 
             size_t* fingerprints, KV* kv_pairs);

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

    BaseNode* findLeaf(uint64_t key);
    BaseNode* findParent(uint64_t key);
    InnerNode* findParentNode(BaseNode* root, BaseNode* child);
    uint64_t find(uint64_t key);

    uint64_t findSplitKey(LeafNode* leaf);
    void insert(struct KV kv);
    uint64_t splitLeaf(LeafNode* leaf);
    void updateParents(uint64_t splitKey, InnerNode* parent, BaseNode* leaf);

};
