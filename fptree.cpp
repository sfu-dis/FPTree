#include <iostream>
#include <string>
#include <sys/types.h>
#include <cstdint>
#include <stdint.h>
#include <bits/hash_bytes.h>
#include <cmath>
#include <algorithm>
#include <stdio.h>    
#include <stdlib.h>     
#include <time.h>       
#include "fptree.h"

using namespace std;


BaseNode::BaseNode() 
{
    this->isInnerNode = false;
}


InnerNode::InnerNode()
{
    this->isInnerNode = true;
    this->nKey = 0;
}

LeafNode::LeafNode() 
{
    this->isInnerNode = false;
    for (size_t i = 0; i < MAX_LEAF_SIZE; i++)
        this->bitmap[i] = 0;
    this->p_next = NULL;
}


int64_t LeafNode::findFirstZero()
{
    size_t i;
    for (i = 0; i < MAX_LEAF_SIZE; i++)
        if (this->bitmap[i] == 0)
            return i;
    
    return i;
}


FPtree::FPtree() 
{
    root = NULL;
}

FPtree::~FPtree() 
{
    // TODO: restore all dynamic memory
}


void FPtree::displayTree(BaseNode* root)
{
    if (root != NULL) 
    {
        if (root->isInnerNode == true)
            for (size_t i = 0; i < reinterpret_cast<InnerNode*> (root)->nKey; i++)
                cout << reinterpret_cast<InnerNode*> (root)->keys[i] << "  ";
        else
            for (size_t i = 0; i < MAX_LEAF_SIZE; i++)
                cout << (reinterpret_cast<LeafNode*> (root)->bitmap[i] == 1 ? 
                        reinterpret_cast<LeafNode*> (root)->kv_pairs[i].key : 0)  << "  ";
        cout << "\n" << endl;
    }
    else
        return;

    if (root->isInnerNode == true)
        for (size_t i = 0; i < reinterpret_cast<InnerNode*> (root)->nKey+1; i++)
            displayTree(reinterpret_cast<InnerNode*> (root)->p_children[i]);
}


size_t getOneByteHash(uint64_t key)
{
    uint64_t* pHashKey = new uint64_t(key);
    size_t len = sizeof(uint64_t);
    size_t hashKey = _Hash_bytes(pHashKey, len, 1);

    hashKey = (hashKey >> (8 * 0)) & 0xff;

    return hashKey;
}


BaseNode* FPtree::findLeaf(uint64_t key) 
{
    if (root == NULL) 
    {
        cout << "Empty Tree" << endl;
        return NULL;
    }
    else if (root->isInnerNode == false)
    {
        return root;
    }
    else 
    {
        InnerNode* cursor = reinterpret_cast<InnerNode*> (root);
        while (cursor->isInnerNode == true) 
        {
            for (uint64_t i = 0; i < cursor->nKey; i++)
            {
                if (key < cursor->keys[i])
                {
                    cursor = reinterpret_cast<InnerNode*> (cursor->p_children[i]);
                    break;
                }
                if (i == cursor->nKey - 1)
                {
                    cursor = reinterpret_cast<InnerNode*> (cursor->p_children[i+1]);
                    break;
                }
            }
        }
        return reinterpret_cast<BaseNode*> (cursor);
    }
}


BaseNode* FPtree::findParent(uint64_t key)
{
    InnerNode* parentNode;
    if (root == NULL || root->isInnerNode == false) 
    {
        cout << "NULL" << endl;
        return NULL;
    }
    else 
    {
        cout << "In" << endl;
        InnerNode* cursor = reinterpret_cast<InnerNode*> (root);
        while (cursor->isInnerNode == true) 
        {
            parentNode = cursor;
            for (uint64_t i = 0; i < cursor->nKey; i++)
            {
                if (key < cursor->keys[i])
                {
                    cursor = reinterpret_cast<InnerNode*> (cursor->p_children[i]);
                    break;
                }
                if (i == cursor->nKey - 1)
                {
                    cursor = reinterpret_cast<InnerNode*> (cursor->p_children[i+1]);
                    break;
                }
            }
        }
        return reinterpret_cast<BaseNode*> (parentNode);
    }
}


InnerNode* FPtree::findParentNode(BaseNode* root, BaseNode* child)
{
    BaseNode* parent;
    if (reinterpret_cast<InnerNode*> (root)->isInnerNode == false) 
        return NULL;

    for (size_t i = 0; i < reinterpret_cast<InnerNode*> (root)->nKey+1; i++)
    {
        if (reinterpret_cast<InnerNode*> (root)->p_children[i] == child)
        {
            return reinterpret_cast<InnerNode*> (root);
        }
        else
        {
            parent = findParentNode(reinterpret_cast<InnerNode*> (root)->p_children[i], child);
            if (parent != NULL)
                return reinterpret_cast<InnerNode*> (parent);
        }
    }

    return reinterpret_cast<InnerNode*> (parent);
}   




uint64_t FPtree::find(uint64_t key)
{
    LeafNode* pLeafNode = reinterpret_cast<LeafNode*> (findLeaf(key));

    for (uint64_t i = 0; i < MAX_LEAF_SIZE; i++) 
    {
        uint64_t currKey = pLeafNode->kv_pairs[i].key;
        if (pLeafNode->bitmap[i] == 1 &&
            pLeafNode->fingerprints[i] == getOneByteHash(key) &&
            currKey == key)
        {
            return pLeafNode->kv_pairs[i].value;
        }
    }

    return 0;
}




void FPtree::insert(struct KV kv) 
{

    cout << "\ninsert: " << kv.key << endl;
    if (root == NULL) 
    {
        root = new LeafNode();
        reinterpret_cast<LeafNode*> (root)->bitmap[0] = 1;
        reinterpret_cast<LeafNode*> (root)->fingerprints[0] = getOneByteHash(kv.key);
        reinterpret_cast<LeafNode*> (root)->kv_pairs[0] = kv;

        return;
    }


    LeafNode* reachedLeafNode = reinterpret_cast<LeafNode*> (findLeaf(kv.key));
    InnerNode* parentNode = findParentNode(root, reachedLeafNode);
    cout << "reachedLeafNode: " << reachedLeafNode->kv_pairs[0].key << endl;

    bool decision = reachedLeafNode->isFull();

    cout << "decision: " << decision << endl;
    
    uint64_t splitKey;
    LeafNode* insertNode = reachedLeafNode;
    if (decision == true)
    {
        splitKey = splitLeaf(reachedLeafNode);

        if (kv.key > splitKey)
            insertNode = reachedLeafNode->p_next;
    }
    
    uint64_t slot = insertNode->findFirstZero();
    insertNode->kv_pairs[slot] = kv;
    insertNode->fingerprints[slot] = getOneByteHash(kv.key);
    insertNode->bitmap[slot] = 1;

    if (decision == true && root->isInnerNode == false)
    {
        root = new InnerNode();
        root->isInnerNode = true;
        reinterpret_cast<InnerNode*> (root)->nKey = 1;
        reinterpret_cast<InnerNode*> (root)->keys[0] = splitKey;
        reinterpret_cast<InnerNode*> (root)->p_children[0] = reachedLeafNode;
        reinterpret_cast<InnerNode*> (root)->p_children[1] = reachedLeafNode->p_next;
        return;
    }


    if (decision == true)
    {
        // cout << "updateParents" << endl;
        // for (int i = 0; i < MAX_LEAF_SIZE; i++)
        // {
        //     cout << "keys: ";
        //     cout << reachedLeafNode->p_next->kv_pairs[i].key << endl;
        // }
    
        updateParents(splitKey, parentNode, reachedLeafNode->p_next);
    }

    return;
}


void FPtree::updateParents(uint64_t splitKey, InnerNode* parent, BaseNode* leaf)
{
    if (parent->nKey < MAX_INNER_SIZE)
    {
        size_t i = 0;
        while (splitKey > parent->keys[i] && i < parent->nKey)
            i++;
        
        for (size_t j = parent->nKey; j > i; j--)
            parent->keys[j] = parent->keys[j-1];
        for (size_t j = parent->nKey+1; j > i+1; j--)
            parent->p_children[j] = parent->p_children[j-1];

        parent->nKey++;
        parent->keys[i] = splitKey;
        parent->p_children[i+1] = leaf;
    }
    else 
    {
        InnerNode* newInnerNode = new InnerNode();
        uint64_t tempKeys[MAX_INNER_SIZE+1];
        BaseNode* tempChildren[MAX_INNER_SIZE+2];
        for (size_t i = 0; i < MAX_INNER_SIZE; i++)
            tempKeys[i] = parent->keys[i];
        for (size_t i = 0; i < MAX_INNER_SIZE+1; i++)
            tempChildren[i] = parent->p_children[i];

        for (size_t i = 0; i < parent->nKey; i++)
        {
            parent->keys[i] = tempKeys[i];
            cout << "parent key: " << parent->keys[i] << endl;
        }


        for (size_t i = 0; i < parent->nKey+1; i++)
        {
            parent->p_children[i] = tempChildren[i];
            cout << "parent child: " << reinterpret_cast<LeafNode*> (parent->p_children[i])->kv_pairs[0].key << endl;
        }
        
        size_t i = 0, j;
        while (splitKey > tempKeys[i] && i < MAX_INNER_SIZE) 
            i++;
        for (size_t j = MAX_INNER_SIZE; j > i; j--)
            tempKeys[j] = tempKeys[j-1];
        for (size_t j = MAX_INNER_SIZE+1; j > i+1; j--)
            tempChildren[j] = tempChildren[j-1];
        tempKeys[i] = splitKey;
        tempChildren[i+1] = leaf;



        for (size_t i = 0; i < MAX_INNER_SIZE+1; i++)
            cout << "tempKeys key: " << tempKeys[i] << endl;


        for (size_t i = 0; i < MAX_INNER_SIZE+2; i++)
            cout << "tempChildren child: " << reinterpret_cast<LeafNode*> (tempChildren[i])->kv_pairs[0].key << endl;
        

    
        parent->nKey = MAX_INNER_SIZE % 2 == 0 ? floor(MAX_INNER_SIZE / 2) : floor(MAX_INNER_SIZE / 2) + 1;
        newInnerNode->nKey = MAX_INNER_SIZE - parent->nKey;

        cout << "parent->nKey: " << parent->nKey << endl;
        cout << "newInnerNode->nKey: " << newInnerNode->nKey << endl;

        for (size_t i = 0, j = parent->nKey+1; i < newInnerNode->nKey; i++, j++)
            newInnerNode->keys[i] = tempKeys[j];
        for (size_t i = 0, j = parent->nKey+1; i < newInnerNode->nKey+1; i++, j++)
            newInnerNode->p_children[i] = tempChildren[j];


        for (size_t i = 0; i < parent->nKey; i++)
        {
            parent->keys[i] = tempKeys[i];
            cout << "parent key: " << parent->keys[i] << endl;
        }

        for (size_t i = 0; i < parent->nKey+1; i++)
        {
            parent->p_children[i] = tempChildren[i];
            cout << "parent child: " << reinterpret_cast<LeafNode*> (parent->p_children[i])->kv_pairs[0].key << endl;
        }
        
        if (parent == root)
        {
            // root = new InnerNode();
            // reinterpret_cast<InnerNode*> (root)->nKey = 1;
            // reinterpret_cast<InnerNode*> (root)->keys[0] = parent->keys[parent->nKey];
            // reinterpret_cast<InnerNode*> (root)->p_children[0] = parent;
            // reinterpret_cast<InnerNode*> (root)->p_children[1] = newInnerNode;
            InnerNode* newRoot = new InnerNode();
            newRoot->nKey = 1;
            newRoot->keys[0] = tempKeys[parent->nKey];
            newRoot->p_children[0] = parent;
            newRoot->p_children[1] = newInnerNode;
            root = newRoot;
        }
        else
        {
            updateParents(tempKeys[parent->nKey], findParentNode(root, parent), newInnerNode);
        }
    }


    return;
}



uint64_t FPtree::splitLeaf(LeafNode* leaf)
{
    LeafNode* newLeafNode = new LeafNode();

    // copy the content to Leaf into NewLeaf
    for (size_t i = 0; i < MAX_LEAF_SIZE; i++)
    {
        newLeafNode->bitmap[i] = leaf->bitmap[i];
        newLeafNode->fingerprints[i] = leaf->fingerprints[i];
        newLeafNode->kv_pairs[i] = leaf->kv_pairs[i];
        newLeafNode->p_next = leaf->p_next;
    }

    uint64_t splitKey = findSplitKey(leaf);

    for (size_t i = 0; i < MAX_LEAF_SIZE; i++)
    {
        if (newLeafNode->kv_pairs[i].key >= splitKey)
            newLeafNode->bitmap[i] = 1;
        else
            newLeafNode->bitmap[i] = 0;
    }

    for (size_t i = 0; i < MAX_LEAF_SIZE; i++)
    {
        if (newLeafNode->bitmap[i] == 0)
            leaf->bitmap[i] = 1;
        else
            leaf->bitmap[i] = 0;
    }

    leaf->p_next = newLeafNode;

    return splitKey;
}


uint64_t FPtree::findSplitKey(LeafNode* leaf)
{
    uint64_t tempKey[MAX_LEAF_SIZE];
    for (size_t i = 0; i < MAX_LEAF_SIZE; i++)
        tempKey[i] = leaf->kv_pairs[i].key;

    std::sort(tempKey, tempKey + MAX_LEAF_SIZE);

    uint64_t mid = floor(MAX_LEAF_SIZE / 2);
    uint64_t splitKey = tempKey[mid];

    return splitKey;
}



int main() 
{
    FPtree fptree;

    size_t insertLength = 25;
    size_t randList[insertLength];
    srand(time(NULL));
    for (size_t i = 0; i < insertLength; i++)
        randList[i] = i+1;
    
    int size = sizeof(randList) / sizeof(randList[0]);

    std::random_shuffle(randList, randList+size);

    for (size_t i = 0; i < insertLength; i++)
        cout << randList[i] << endl;

    for (size_t i = 0; i < insertLength; i++)
        fptree.insert(KV(randList[i], randList[i]));

    // fptree.insert(KV(16, 10));
    // fptree.insert(KV(15, 10));
    // fptree.insert(KV(14, 10));
    // fptree.insert(KV(13, 10));
    // fptree.insert(KV(12, 10));
    // fptree.insert(KV(11, 10));
    // fptree.insert(KV(10, 10));
    // fptree.insert(KV(9, 6));
    // fptree.insert(KV(8, 6));
    // fptree.insert(KV(7, 6));
    // fptree.insert(KV(6, 6));
    // fptree.insert(KV(5, 6));
    // fptree.insert(KV(4, 6));
    // fptree.insert(KV(3, 6));
    // fptree.insert(KV(2, 6));
    // fptree.insert(KV(1, 6));


    fptree.displayTree(fptree.getRoot());

    // cout << "find: " << fptree.find(9) << endl;
    // cout << "find: " << fptree.find(2) << endl;
    // cout << "find: " << fptree.find(3) << endl;
    // cout << "find: " << fptree.find(4) << endl;
}















// if (root->isInnerNode == false && root->nKey < MAX_nKeys)
// {
//     size_t slot = reinterpret_cast<LeafNode*> (root)->findFirstZero();
//     root->keys[slot] = kv.key;
//     reinterpret_cast<LeafNode*> (root)->bitmap[slot] = 1;
//     reinterpret_cast<LeafNode*> (root)->fingerprints[slot] = getOneByteHash(kv.key);
//     reinterpret_cast<LeafNode*> (root)->kv_pairs[slot] = kv;
//     root->nKey++;

//     if (root->nKey == MAX_nKeys)
//     {
//         uint64_t splitKey = splitLeaf(reinterpret_cast<LeafNode*> (root));
//         LeafNode* tempRootLeafPointer = reinterpret_cast<LeafNode*> (root);
        
//         root = new InnerNode();
//         reinterpret_cast<InnerNode*> (root)->isInnerNode = true;
//         reinterpret_cast<InnerNode*> (root)->nKey = 1;
//         reinterpret_cast<InnerNode*> (root)->keys[0] = splitKey;
//         reinterpret_cast<InnerNode*> (root)->p_children[0] = tempRootLeafPointer;
//         reinterpret_cast<InnerNode*> (root)->p_children[1] = tempRootLeafPointer->p_next;
//     }
// }



// if (root->nKey == MAX_nKeys)
// {
//     size_t splitPosition = floor(root->nKey / 2);
//     uint64_t middleKey = root->keys[splitPosition];
    
//     LeafNode* rootLeafNode = new LeafNode();
//     rootLeafNode->nKey = splitPosition;
//     for (size_t i = 0; i < splitPosition; i++)
//     {
//         rootLeafNode->keys[i] = root->keys[i];
//         rootLeafNode->bitmap[i] = reinterpret_cast<LeafNode*> (root)->bitmap[i];
//         rootLeafNode->fingerprints[i] = reinterpret_cast<LeafNode*> (root)->fingerprints[i];
//         rootLeafNode->kv_pairs[i] = reinterpret_cast<LeafNode*> (root)->kv_pairs[i];
//     }

//     LeafNode* newLeafNode = new LeafNode();
//     newLeafNode->nKey = splitPosition % 2 == 0 ? splitPosition : (splitPosition + 1);
//     for (size_t i = splitPosition; i < root->nKey; i++)
//     {
//         newLeafNode->keys[i - splitPosition] = root->keys[i];
//         newLeafNode->bitmap[i - splitPosition] = reinterpret_cast<LeafNode*> (root)->bitmap[i];
//         newLeafNode->fingerprints[i - splitPosition] = reinterpret_cast<LeafNode*> (root)->fingerprints[i];
//         newLeafNode->kv_pairs[i - splitPosition] = reinterpret_cast<LeafNode*> (root)->kv_pairs[i];
//     }

//     free(root);

//     root = new InnerNode();
//     reinterpret_cast<InnerNode*> (root)->isInnerNode = true;
//     reinterpret_cast<InnerNode*> (root)->nKey = 1;
//     reinterpret_cast<InnerNode*> (root)->keys[0] = middleKey;
//     reinterpret_cast<InnerNode*> (root)->p_children[0] = rootLeafNode;
//     reinterpret_cast<InnerNode*> (root)->p_children[1] = newLeafNode;

//     rootLeafNode->p_next = newLeafNode;
// }

// return 1;


// for (size_t i = 0; i < newLeafNode->nKey; i++)
// {
//     cout << "splitLeaf newLeafNode bitmap: " << newLeafNode->bitmap[i] << endl;
// }


// for (int i = 0; i < MAX_nKeys; i++)
// {
//     cout << "tempRootLeafPointer bitmap: " << tempRootLeafPointer->bitmap[i] << endl;
//     cout << "newLeaf bitmap: " << tempRootLeafPointer->p_next->bitmap[i] << endl;
// }



// cout << "key: " << key << endl;
// cout << "currKey: " << currKey << endl;
// cout << "pLeafNode->bitmap[i]: " << pLeafNode->bitmap[i] << endl;