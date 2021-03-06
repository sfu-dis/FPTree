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
    bitmap.reset();
    this->p_next = NULL;
}

LeafNode::LeafNode(std::bitset<MAX_LEAF_SIZE> bitmap, LeafNode* p_next, 
        std::array<size_t, MAX_LEAF_SIZE> fingerprints, std::array<KV, MAX_LEAF_SIZE> kv_pairs)
: bitmap(bitmap), p_next(p_next), fingerprints(fingerprints), kv_pairs(kv_pairs)
{
    this->isInnerNode = false;
}

// LeafNode::LeafNode(const LeafNode* leaf)
// {

// }

int64_t LeafNode::findFirstZero()
{
    std::bitset<MAX_LEAF_SIZE> b = bitmap;
    return b.flip()._Find_first();
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
    if (root == NULL)
    return;

    if (root->isInnerNode)
    {
        InnerNode* node = reinterpret_cast<InnerNode*> (root);
        for (size_t i = 0; i < node->nKey; i++)
            cout << node->keys[i] << "  ";
        cout << "\n" << endl;

        for (size_t i = 0; i < node->nKey + 1; i++)
            displayTree(node->p_children[i]);
    }
    else
    {
        LeafNode* node = reinterpret_cast<LeafNode*> (root);
        cout << "L ";
        for (size_t i = 0; i < MAX_LEAF_SIZE; i++)
                cout << (node->bitmap.test(i) == 1 ? std::to_string(node->kv_pairs[i].key) : "")  << "  ";
        cout << "\n" << endl;
    }
}

void FPtree::printFPTree(std::string prefix, BaseNode* root)
{
	if (root->isInnerNode) {
		InnerNode* node = reinterpret_cast<InnerNode*> (root);
		printFPTree("    " + prefix, node->p_children[0]);
        for (size_t i = 0; i < node->nKey; i++)
        {
        	cout << prefix << node->keys[i] << endl;
            printFPTree("    " + prefix, node->p_children[i+1]);

        }
        cout << "\n" << endl; 
	}
	else
	{
		LeafNode* node = reinterpret_cast<LeafNode*> (root);
        for (size_t i = 0; i < MAX_LEAF_SIZE; i++)
        {
        	if (node->bitmap.test(i) == 1)
        	{
        		cout << prefix << node->kv_pairs[i].key << "," << node->kv_pairs[i].value << endl;
        	}
        }
	}
}

size_t getOneByteHash(uint64_t key)
{
    size_t len = sizeof(uint64_t);
    size_t hashKey = _Hash_bytes(&key, len, 1);
    hashKey = (hashKey >> (8 * 0)) & 0xff;
    return hashKey;
}


LeafNode* FPtree::findLeaf(uint64_t key) 
{
    return findLeafWithParent(key).second;
}

std::pair<InnerNode*, LeafNode*> FPtree::findLeafWithParent(uint64_t key)
{
	InnerNode* parentNode;
	if (root == NULL) 
    {
        cout << "Empty Tree." << endl;
        return std::make_pair(nullptr, nullptr);
    }
    else if (root->isInnerNode == false) 
    {
        cout << "Tree with root." << endl;
        return std::make_pair(nullptr, reinterpret_cast<LeafNode*> (root));
    }
    else 
    {
        InnerNode* cursor = reinterpret_cast<InnerNode*> (root);
        while (cursor->isInnerNode == true) 
        {
            parentNode = cursor;

            int mid, l = 0, r = cursor->nKey - 1;
            while(l <= r)
            {
                mid = l + (r - l)/2;
                if(cursor->keys[mid] == key)
                {
                    mid ++;
                    break;
                }
                else if(cursor->keys[mid] > key)
                    r = mid-1;
                else 
                    l = mid+1;
            }
            if (key >= cursor->keys[mid])
                mid ++;
            cursor = reinterpret_cast<InnerNode*> (cursor->p_children[mid]);
        }
        return std::make_pair(parentNode, reinterpret_cast<LeafNode*> (cursor));
    }
}


InnerNode* FPtree::findInnerNodeParent(InnerNode* child)
{
    if (root == NULL || root->isInnerNode == false)
        return nullptr;

    InnerNode* parent;
    InnerNode* cursor = reinterpret_cast<InnerNode*> (root);
    uint64_t first_key = child->keys[0];

    while (cursor->isInnerNode == true) 
    {
        parent = cursor;

        int mid, l = 0, r = cursor->nKey - 1;
        while(l <= r)
        {
            mid = l + (r - l)/2;
            if(cursor->keys[mid] == first_key)
            {
                mid ++;
                break;
            }
            else if(cursor->keys[mid] > first_key)
                r = mid-1;
            else 
                l = mid+1;
        }
        if (first_key >= cursor->keys[mid])
            mid ++;
        cursor = reinterpret_cast<InnerNode*> (cursor->p_children[mid]);
        if (cursor->keys[0] == first_key)
            return parent;
    }
    return nullptr;
}   


uint64_t FPtree::find(uint64_t key)
{
    LeafNode* pLeafNode = findLeaf(key);
    size_t key_hash = getOneByteHash(key);
    for (uint64_t i = 0; i < MAX_LEAF_SIZE; i++) 
    {
        KV currKV = pLeafNode->kv_pairs[i];
        if (pLeafNode->bitmap.test(i) == 1 &&
            pLeafNode->fingerprints[i] == key_hash &&
            currKV.key == key)
        {
            return currKV.value;
        }
    }

    return 0;
}

bool FPtree::updateValue(struct KV kv)
{
    LeafNode* leaf = FPtree::findLeaf(kv.key);
    if (leaf == nullptr)
        return false;

    size_t key_hash = getOneByteHash(kv.key);
    for (uint64_t i = 0; i < MAX_LEAF_SIZE; i++) 
    {
        KV currKV = leaf->kv_pairs[i];
        if (leaf->bitmap.test(i) == 1 &&
            leaf->fingerprints[i] == key_hash &&
            currKV.key == kv.key)
        {
            leaf->kv_pairs[i].value = kv.value;
            return true;
        }
    }
    return false;
}

bool FPtree::insert(struct KV kv) 
{

    cout << "\ninsert: " << kv.key << endl;
    if (root == NULL) 
    {
        root = new LeafNode();
        reinterpret_cast<LeafNode*> (root)->bitmap.set(0);
        reinterpret_cast<LeafNode*> (root)->fingerprints[0] = getOneByteHash(kv.key);
        reinterpret_cast<LeafNode*> (root)->kv_pairs[0] = kv;
        reinterpret_cast<LeafNode*> (root)->p_next = nullptr;
        return true;
    }

    std::pair<InnerNode*, LeafNode*> p = findLeafWithParent(kv.key);
    InnerNode* parentNode = p.first;
    LeafNode* reachedLeafNode = p.second;
    cout << "reachedLeafNode: " << reachedLeafNode->kv_pairs[0].key << endl;

    // return false if key already exists
    size_t key_hash = getOneByteHash(kv.key);
    for (uint64_t i = 0; i < MAX_LEAF_SIZE; i++) 
    {
        KV currKV = reachedLeafNode->kv_pairs[i];
        if (reachedLeafNode->bitmap.test(i) == 1 &&
            reachedLeafNode->fingerprints[i] == key_hash &&
            currKV.key == kv.key)
        {
            return false;
        }
    }

    bool decision = reachedLeafNode->isFull();

    cout << "decision: " << decision << endl;
    
    uint64_t splitKey;
    LeafNode* insertNode = reachedLeafNode;
    if (decision == true)
    {
        splitKey = splitLeaf(reachedLeafNode);

        if (kv.key >= splitKey)
            insertNode = reachedLeafNode->p_next;
    }
    
    uint64_t slot = insertNode->findFirstZero();
    insertNode->kv_pairs[slot] = kv;
    insertNode->fingerprints[slot] = getOneByteHash(kv.key);
    insertNode->bitmap.set(slot);

    if (decision == true)
    {
        if (root->isInnerNode == false)
        {
            root = new InnerNode();
            reinterpret_cast<InnerNode*> (root)->nKey = 1;
            reinterpret_cast<InnerNode*> (root)->keys[0] = splitKey;
            reinterpret_cast<InnerNode*> (root)->p_children[0] = reachedLeafNode;
            reinterpret_cast<InnerNode*> (root)->p_children[1] = reachedLeafNode->p_next;
            return true;
        }
        updateParents(splitKey, parentNode, reachedLeafNode->p_next);
    }

    return true;
}


void FPtree::updateParents(uint64_t splitKey, InnerNode* parent, BaseNode* child)
{
    if (parent->nKey < MAX_INNER_SIZE)
    {
        for (size_t i = parent->nKey-1; i >= 0; i--)
        {
            if (parent->keys[i] > splitKey)
            {
                parent->keys[i+1] = parent->keys[i];
                parent->p_children[i+2] = parent->p_children[i+1];
            }
            else
            {
                parent->keys[i+1] = splitKey;
                parent->p_children[i+2] = child;
                break;
            }
        }
        parent->nKey++;
    }
    else 
    {
        InnerNode* newInnerNode = new InnerNode();
        uint64_t mid = floor((MAX_INNER_SIZE + 1) / 2);
        std::array<uint64_t, MAX_INNER_SIZE + 1> temp_keys;
        std::array<BaseNode*, MAX_INNER_SIZE + 2> temp_children;
        size_t key_idx = 0;
        bool added_splitKey = false;
        for (size_t i = 0; i < MAX_INNER_SIZE; i++){
            if (!added_splitKey && splitKey < parent->keys[i])
            {
                temp_keys[key_idx] = splitKey;
                temp_children[++key_idx] = child;
                added_splitKey = true;
            }
            temp_keys[key_idx] = parent->keys[i];
            temp_children[++key_idx] = parent->p_children[i+1];
        }
        if (!added_splitKey)
        {
            temp_keys[key_idx] = splitKey;
            temp_children[++key_idx] = child;
        }

        for (size_t i = 0; i < mid; i++)
        {
            parent->keys[i] = temp_keys[i];
            parent->p_children[i+1] = temp_children[i+1];
        }
        parent->nKey = mid;
        splitKey = temp_keys[mid];

        key_idx = 0;
        newInnerNode->p_children[key_idx] = temp_children[mid+1];
        for (size_t i = mid + 1; i <= MAX_INNER_SIZE; i++)
        {
            newInnerNode->keys[key_idx] = temp_keys[i];
            newInnerNode->p_children[++key_idx] = temp_children[i+1];
        }
        newInnerNode->nKey = key_idx;
        
        if (parent == root)
        {
            InnerNode* newRoot = new InnerNode();
            newRoot->nKey = 1;
            newRoot->keys[0] = splitKey;
            newRoot->p_children[0] = parent;
            newRoot->p_children[1] = newInnerNode;
            root = newRoot;
            return;
        }
        updateParents(splitKey, findInnerNodeParent(parent), newInnerNode);
    }
}



uint64_t FPtree::splitLeaf(LeafNode* leaf)
{
    LeafNode* newLeafNode = new LeafNode();

    // copy the content to Leaf into NewLeaf
    newLeafNode->bitmap = leaf->bitmap;
    newLeafNode->fingerprints = leaf->fingerprints;
    newLeafNode->kv_pairs = leaf->kv_pairs;
    newLeafNode->p_next = leaf->p_next;

    uint64_t splitKey = findSplitKey(leaf);

    for (size_t i = 0; i < MAX_LEAF_SIZE; i++)
    {
        if (newLeafNode->kv_pairs[i].key < splitKey)
            newLeafNode->bitmap.set(i, 0);
    }

    leaf->bitmap = newLeafNode->bitmap;
    leaf->bitmap.flip();

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
    // srand(1);
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

    fptree.printFPTree("├──", fptree.getRoot());

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