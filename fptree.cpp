#include "fptree.h"

BaseNode::BaseNode() 
{
    this->isInnerNode = false;
}


InnerNode::InnerNode()
{
    this->isInnerNode = true;
    this->nKey = 0;
}

InnerNode::InnerNode(const InnerNode& inner)
{
    this->isInnerNode = true;
    this->nKey = inner.nKey;
    this->keys = inner.keys;
    this->p_children = inner.p_children;
}

std::pair<uint64_t, bool> InnerNode::findChildIndex(uint64_t key)
{
    int mid, l = 0, r = this->nKey - 1;
    bool contain_key = false;
    while(l <= r)
    {
        mid = l + (r - l)/2;
        if(this->keys[mid] == key)
        {
            mid ++;
            contain_key = true;
            break;
        }
        else if(this->keys[mid] > key)
            r = mid-1;
        else 
            l = mid+1;
    }
    if (mid < this->nKey && key >= this->keys[mid])
        mid ++;
    assert(mid >= 0);
    return std::make_pair((uint64_t)mid, contain_key);
}

void InnerNode::removeKey(uint64_t index, bool remove_right_child = true)
{
    assert(this->nKey > index && "Remove key index out of range!");
    this->nKey--;
    uint64_t i = index, j = i;
    if (remove_right_child)
        j++;
    for (i; i < this->nKey; i++)
    {
        this->keys[i] = this->keys[i+1];
        this->p_children[j] = this->p_children[j+1];
        j++;
    }
    if (!remove_right_child)
        this->p_children[this->nKey] = this->p_children[this->nKey+1];
}

void InnerNode::addKey(uint64_t index, uint64_t key, BaseNode* child, bool add_child_right = true)
{
    assert(this->nKey >= index && "Insert key index out of range!");
    uint64_t i = this->nKey, j = i;
    this->nKey++;
    for (i; i > index; i--)
    {
        this->keys[i] = this->keys[i-1];
        this->p_children[i+1] = this->p_children[i];
    }
    this->keys[index] = key;
    if (!add_child_right)
    {
        this->p_children[index+1] = this->p_children[index];
        this->p_children[index] = child;
    }
    else
        this->p_children[index+1] = child;
}


LeafNode::LeafNode() 
{
    this->isInnerNode = false;
    this->bitmap.reset();
    this->p_next = nullptr;
    this->lock = ATOMIC_VAR_INIT(false);
}

LeafNode::LeafNode(const LeafNode& leaf)
{
    this->isInnerNode = false;
    this->bitmap = leaf.bitmap;
    this->fingerprints = leaf.fingerprints;
    this->kv_pairs = leaf.kv_pairs;
    this->p_next = leaf.p_next;
    this->lock.store(leaf.lock, std::memory_order_relaxed);
}


uint64_t LeafNode::findFirstZero()
{
    std::bitset<MAX_LEAF_SIZE> b = bitmap;
    return b.flip()._Find_first();
}

void LeafNode::addKV(struct KV kv)
{
    uint64_t idx = this->findFirstZero();
    this->fingerprints[idx] = getOneByteHash(kv.key);
    this->kv_pairs[idx] = kv;
    this->bitmap.set(idx);
}

uint64_t LeafNode::removeKV(uint64_t key)
{
    uint64_t idx = findKVIndex(key);
    assert(idx != MAX_LEAF_SIZE);
    return this->removeKVByIdx(idx);
}

uint64_t LeafNode::removeKVByIdx(uint64_t pos)
{
    assert(this->bitmap.test(pos) == true);
    this->bitmap.set(pos, 0);
    return this->kv_pairs[pos].value;
}

uint64_t LeafNode::findKVIndex(uint64_t key)
{
    size_t key_hash = getOneByteHash(key);
    for (uint64_t i = 0; i < MAX_LEAF_SIZE; i++) 
    {
        if (this->bitmap.test(i) == 1 &&
            this->fingerprints[i] == key_hash &&
            this->kv_pairs[i].key == key)
        {
            return i;
        }
    }
    return MAX_LEAF_SIZE;
}

KV LeafNode::minKV(bool remove = false)
{
    uint64_t min_key = -1, min_key_idx = 0;
    for (uint64_t i = 0; i < MAX_LEAF_SIZE; i++) 
        if (this->bitmap.test(i) == 1 && this->kv_pairs[i].key <= min_key)
        {
            min_key = this->kv_pairs[i].key;
            min_key_idx = i;
        }
    if (remove)
        bitmap.set(min_key_idx, 0);
    return this->kv_pairs[min_key_idx];
}

KV LeafNode::maxKV(bool remove = false)
{
    uint64_t max_key = 0, max_key_idx = 0;
    for (uint64_t i = 0; i < MAX_LEAF_SIZE; i++) 
        if (this->bitmap.test(i) == 1 && this->kv_pairs[i].key >= max_key)
        {
            max_key = this->kv_pairs[i].key;
            max_key_idx = i;
        }
    if (remove)
        bitmap.set(max_key_idx, 0);
    return this->kv_pairs[max_key_idx];
}

void LeafNode::sortKV()
{
    uint64_t slot_idx = findFirstZero();    // idx of first empty slot
    for (uint64_t kv_idx = slot_idx + 1; kv_idx < MAX_LEAF_SIZE;)
    {
        if (this->bitmap.test(kv_idx))
        {
            this->kv_pairs[slot_idx] = this->kv_pairs[kv_idx];
            this->bitmap.set(slot_idx);
            this->bitmap.set(kv_idx, 0);
            slot_idx = findFirstZero();
            kv_idx = slot_idx + 1;
        }
        else
            kv_idx++;
    }
    std::sort(this->kv_pairs.begin(), this->kv_pairs.begin() + slot_idx, 
        [] (const KV& kv1, const KV& kv2){
            return kv1.key < kv2.key;
        });
    for (uint64_t i = 0; i < slot_idx; i++)
        this->fingerprints[i] = getOneByteHash(this->kv_pairs[i].key);
}

FPtree::FPtree() 
{
    root = nullptr;
    current_leaf = nullptr;
}

FPtree::~FPtree() 
{
    // TODO: restore all dynamic memory
}


void FPtree::displayTree(BaseNode* root)
{
    if (root == nullptr)
    return;

    if (root->isInnerNode)
    {
        InnerNode* node = reinterpret_cast<InnerNode*> (root);
        for (size_t i = 0; i < node->nKey; i++)
            std::cout << node->keys[i] << "  ";
        std::cout << "\n" << std::endl;

        for (size_t i = 0; i < node->nKey + 1; i++)
            displayTree(node->p_children[i]);
    }
    else
    {
        LeafNode* node = reinterpret_cast<LeafNode*> (root);
        std::cout << "L ";
        for (size_t i = 0; i < MAX_LEAF_SIZE; i++)
                std::cout << (node->bitmap.test(i) == 1 ? std::to_string(node->kv_pairs[i].key) : "")  << "  ";
        std::cout << "\n" << std::endl;
    }
}

void FPtree::printFPTree(std::string prefix, BaseNode* root)
{
	if (root->isInnerNode) {
		InnerNode* node = reinterpret_cast<InnerNode*> (root);
		printFPTree("    " + prefix, node->p_children[node->nKey]);
        for (int64_t i = node->nKey-1; i >= 0; i--)
        {
        	std::cout << prefix << node->keys[i] << std::endl;
            printFPTree("    " + prefix, node->p_children[i]);
        } 
	}
	else
	{
		LeafNode* node = reinterpret_cast<LeafNode*> (root);
        for (int64_t i = MAX_LEAF_SIZE-1; i >= 0; i--)
        {
        	if (node->bitmap.test(i) == 1)
        	{
        		std::cout << prefix << node->kv_pairs[i].key << "," << node->kv_pairs[i].value << std::endl;
        	}
        }
	}
}

size_t getOneByteHash(uint64_t key)
{
    size_t len = sizeof(uint64_t);
    size_t hashKey = std::_Hash_bytes(&key, len, 1);
    hashKey = (hashKey >> (8 * 0)) & 0xff;
    return hashKey;
}


LeafNode* FPtree::findLeaf(uint64_t key) 
{
    return findLeafWithParent(key).second;
}

std::pair<InnerNode*, LeafNode*> FPtree::findLeafWithParent(uint64_t key)
{
    if (!root->isInnerNode)
    {
        // cout << "Tree with root." << endl;
        return std::make_pair(nullptr, reinterpret_cast<LeafNode*> (root));
    }
    std::tuple<InnerNode*, InnerNode*, uint64_t> nodes = findInnerAndLeafWithParent(key);
    InnerNode* parent = std::get<1>(nodes);
    return std::make_pair(parent, reinterpret_cast<LeafNode*> (parent->p_children[std::get<2>(nodes)]));
}

std::tuple<InnerNode*, InnerNode*, uint64_t> FPtree::findInnerAndLeafWithParent(uint64_t key)
{
    InnerNode* parentNode, *indexNode = nullptr;
    std::pair<uint64_t, bool> p;

    InnerNode* cursor = reinterpret_cast<InnerNode*> (root);
    while (cursor->isInnerNode == true) 
    {
        parentNode = cursor;
        p = cursor->findChildIndex(key);
        if (p.second)
            indexNode = cursor;
        cursor = reinterpret_cast<InnerNode*> (cursor->p_children[p.first]);
    }
    return std::make_tuple(indexNode, parentNode, p.first);
}


std::pair<InnerNode*, uint64_t> FPtree::findInnerNodeParent(InnerNode* child)
{
    InnerNode* parent;
    InnerNode* cursor = reinterpret_cast<InnerNode*> (root);
    uint64_t first_key = child->keys[0], child_idx;

    while (cursor->isInnerNode == true) 
    {
        parent = cursor;
        child_idx = cursor->findChildIndex(first_key).first;
        cursor = reinterpret_cast<InnerNode*> (cursor->p_children[child_idx]);
        if (cursor == child)
            return std::make_pair(parent, child_idx);
    }
    assert(false && "Function called with child == root!");
    return std::make_pair(nullptr, 0);
}   


uint64_t FPtree::find(uint64_t key)
{
    if (root != nullptr)
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
    }
    return 0;
}


bool FPtree::update(struct KV kv)
{
    assert(root != nullptr);
    LeafNode* leaf = FPtree::findLeaf(kv.key);
    size_t pos = leaf->findKVIndex(kv.key);
    // if cannot find key
    if (pos == MAX_LEAF_SIZE)
        return false;
    leaf->kv_pairs[pos].value = kv.value;
    return true;
}


bool FPtree::insert(struct KV kv) 
{

    // cout << "\ninsert: " << kv.key << endl;
    if (root == nullptr) 
    {
        LeafNode* leaf = new LeafNode();
        leaf->addKV(kv);
        root = leaf;
        return true;
    }

    std::pair<InnerNode*, LeafNode*> p = findLeafWithParent(kv.key);
    InnerNode* parentNode = p.first;
    LeafNode* reachedLeafNode = p.second;
    // cout << "reachedLeafNode: " << reachedLeafNode->kv_pairs[0].key << endl;

    // return false if key already exists
    if (reachedLeafNode->findKVIndex(kv.key) != MAX_LEAF_SIZE)
        return false;

    bool decision = reachedLeafNode->isFull();
    // cout << "decision: " << decision << endl;
    
    uint64_t splitKey;
    LeafNode* insertNode = reachedLeafNode;
    if (decision == true)
    {
        splitKey = splitLeaf(reachedLeafNode);

        if (kv.key >= splitKey)
            insertNode = reachedLeafNode->p_next;
    }
    
    insertNode->addKV(kv); 

    if (decision == true)
    {
        if (root->isInnerNode == false)
        {
            root = new InnerNode();
            reinterpret_cast<InnerNode*> (root)->addKey(0, splitKey, reachedLeafNode, false);
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
        std::pair<uint64_t, bool> insert_pos = parent->findChildIndex(splitKey);
        parent->addKey(insert_pos.first, splitKey, child);
    }
    else 
    {
        InnerNode* newInnerNode = new InnerNode();
        uint64_t mid = floor((MAX_INNER_SIZE + 1) / 2);
        std::array<uint64_t, MAX_INNER_SIZE + 1> temp_keys;
        std::array<BaseNode*, MAX_INNER_SIZE + 2> temp_children;
        size_t key_idx = 0, insert_idx = MAX_INNER_SIZE;
        for (size_t i = 0; i < MAX_INNER_SIZE; i++){
            if (splitKey < parent->keys[i] && insert_idx == MAX_INNER_SIZE)
                insert_idx = key_idx++;
            temp_keys[key_idx] = parent->keys[i];
            temp_children[++key_idx] = parent->p_children[i+1];
        }
        temp_keys[insert_idx] = splitKey;
        temp_children[insert_idx + 1] = child;

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
        updateParents(splitKey, findInnerNodeParent(parent).first, newInnerNode);
    }
}



uint64_t FPtree::splitLeaf(LeafNode* leaf)
{
    LeafNode* newLeafNode = new LeafNode(*leaf);
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
    std::array<KV, MAX_LEAF_SIZE> tempArr = leaf->kv_pairs;

    std::sort(tempArr.begin(), tempArr.end(), [] (const KV& kv1, const KV& kv2){
            return kv1.key < kv2.key;
        });

    uint64_t mid = floor(MAX_LEAF_SIZE / 2);
    uint64_t splitKey = tempArr[mid].key;

    return splitKey;
}

uint64_t FPtree::deleteKey(uint64_t key)
{
    assert(root != nullptr && "Exception: Delete from empty tree!");

    if (!root->isInnerNode)     // tree with root only
    {
        uint64_t idx = reinterpret_cast<LeafNode*> (root)->findKVIndex(key);
        assert(idx != MAX_LEAF_SIZE && "Exception: KV to delete is not present in leaf!");
        return reinterpret_cast<LeafNode*> (root)->removeKVByIdx(idx);
    }

    std::tuple<InnerNode*, InnerNode*, uint64_t> tpl = findInnerAndLeafWithParent(key);
    InnerNode* indexNode = std::get<0>(tpl);
    InnerNode* parent = std::get<1>(tpl);
    uint64_t child_idx = std::get<2>(tpl);
    LeafNode* leaf = reinterpret_cast<LeafNode*>(parent->p_children[child_idx]);

    uint64_t idx = leaf->findKVIndex(key);
    assert(idx != MAX_LEAF_SIZE && "Exception: KV to delete is not present in leaf!");

    uint64_t value;

    if (indexNode == nullptr)  // key does not appear in any innernode
    {
        value = leaf->removeKVByIdx(idx);
        if (leaf->countKV() == 0 && !tryBorrowKey(parent, child_idx, child_idx+1)) // no kv left and cannot borrow from right sibling
        {
            leaf->addKV(reinterpret_cast<LeafNode*> (parent->p_children[child_idx+1])->minKV());
            mergeNodes(parent, child_idx, child_idx+1);
        }
    }
    else if (indexNode == parent)   // key also appear in parent node (leaf is right child )
    {
        value = leaf->removeKVByIdx(idx);
        
        if (leaf->countKV() == 0 && !tryBorrowKey(parent, child_idx, child_idx-1))
        {
            mergeNodes(parent, child_idx-1, child_idx);
        }
        else
            parent->keys[child_idx-1] = minKey(parent->p_children[child_idx]);
    }
    else    // key appears in inner node (above parent), it is the minimum key of right subtree. (leaf is left most child )
    {
        std::pair<uint64_t, bool> p = indexNode->findChildIndex(key);
        assert(p.second == true && "Key not found in index node!");
        value = leaf->removeKVByIdx(idx);
        if (leaf->countKV() == 0 && !tryBorrowKey(parent, child_idx, child_idx+1)) // no kv left and cannot borrow from right sibling
        {
            KV kv = reinterpret_cast<LeafNode*> (parent->p_children[child_idx+1])->minKV();
            leaf->addKV(kv);
            indexNode->keys[p.first - 1] = kv.key;
            mergeNodes(parent, child_idx, child_idx+1);
        }
        else
            indexNode->keys[p.first - 1] = minKey(indexNode->p_children[p.first]);
    }
    return value;
}

void FPtree::mergeNodes(InnerNode* parent, uint64_t left_child_idx, uint64_t right_child_idx)
{
    InnerNode* inner_child = nullptr;
    LeafNode* leaf_child = nullptr;
    if (parent->p_children[0]->isInnerNode)    // merge inner nodes
    {
        InnerNode* left = reinterpret_cast<InnerNode*> (parent->p_children[left_child_idx]);
        InnerNode* right = reinterpret_cast<InnerNode*> (parent->p_children[right_child_idx]);
        if (left->nKey == 0)
        {
            right->addKey(0, parent->keys[left_child_idx], left->p_children[0], false);
            delete left; left = nullptr;
            parent->removeKey(left_child_idx, false);
            if (left_child_idx != 0)
                parent->keys[left_child_idx-1] = minKey(parent->p_children[left_child_idx]);
            inner_child = right;
        }
        else
        {
            left->addKey(left->nKey, parent->keys[left_child_idx], right->p_children[0]);
            delete right; right = nullptr;
            parent->removeKey(left_child_idx);
            inner_child = left;
        }
    }
    else    // merge leaves
    {
        LeafNode* left = reinterpret_cast<LeafNode*> (parent->p_children[left_child_idx]);
        LeafNode* right = reinterpret_cast<LeafNode*> (parent->p_children[right_child_idx]);
        left->p_next = right->p_next;
        delete right; right = nullptr;
        if (left_child_idx != 0)
            parent->keys[left_child_idx-1] = left->minKV().key;
        parent->removeKey(left_child_idx);
        leaf_child = left;
    }
    if (parent->nKey == 0)  // parent has 0 key, need to  borrow or merge
    {
        if (parent == root) // entire tree stores 1 kv, convert the only leafnode into root
        {
            delete root; root = nullptr;
            root = inner_child;
            if (leaf_child != nullptr)
                root = leaf_child;
            return;
        }
        std::pair<InnerNode*, uint64_t> p = findInnerNodeParent(parent);

        if (!(p.second != 0 && tryBorrowKey(p.first, p.second, p.second-1)) && 
            !(p.second != p.first->nKey && tryBorrowKey(p.first, p.second, p.second+1)))
        {
            if (p.second != 0)
                p.second --;
            mergeNodes(p.first, p.second, p.second+1);
        }
    }
}

bool FPtree::tryBorrowKey(InnerNode* parent, uint64_t receiver_idx, uint64_t sender_idx)
{
    assert(receiver_idx == sender_idx + 1 || receiver_idx + 1 == sender_idx && "Sender and receiver are not immediate siblings!");
    if (parent->p_children[0]->isInnerNode)    // inner nodes
    {
        InnerNode* sender = reinterpret_cast<InnerNode*> (parent->p_children[sender_idx]);
        if (sender->nKey <= 1)      // sibling has only 1 key, cannot borrow
            return false;
        InnerNode* receiver = reinterpret_cast<InnerNode*> (parent->p_children[receiver_idx]);
        if (receiver_idx < sender_idx) // borrow from right sibling
        {
            receiver->addKey(0, parent->keys[receiver_idx], sender->p_children[0]);
            parent->keys[receiver_idx] = sender->keys[0];
            if (receiver_idx != 0)
                parent->keys[receiver_idx-1] = minKey(receiver);
            sender->removeKey(0, false);
        }
        else // borrow from left sibling
        {
            receiver->addKey(0, parent->keys[sender_idx], sender->p_children[sender->nKey], false);
            parent->keys[sender_idx] = sender->keys[sender->nKey-1];
            sender->removeKey(sender->nKey-1);
        }
    }
    else    // leaf nodes
    {
        LeafNode* sender = reinterpret_cast<LeafNode*> (parent->p_children[sender_idx]);
        if (sender->countKV() <= 1)      // sibling has only 1 key, cannot borrow
            return false;
        LeafNode* receiver = reinterpret_cast<LeafNode*> (parent->p_children[receiver_idx]);

        if (receiver_idx < sender_idx) // borrow from right sibling
        {
            KV borrowed_kv = sender->minKV(true);
            receiver->addKV(borrowed_kv);
            if (receiver_idx != 0)
                parent->keys[receiver_idx-1] = borrowed_kv.key;
            parent->keys[receiver_idx] = sender->minKV().key;
        }
        else // borrow from left sibling
        {
            KV borrowed_kv = sender->maxKV(true);
            receiver->addKV(borrowed_kv);
            parent->keys[sender_idx] = borrowed_kv.key;
        }
    }
    return true;
}

uint64_t FPtree::minKey(BaseNode* node)
{
    while (node->isInnerNode)
        node = reinterpret_cast<InnerNode*> (node)->p_children[0];
    return reinterpret_cast<LeafNode*> (node)->minKV().key;
}

void FPtree::ScanInitialize(uint64_t key)
{
    if (!root)
        return;
    this->current_leaf = root->isInnerNode? findLeaf(key) : reinterpret_cast<LeafNode*> (root);
    while (this->current_leaf != nullptr)
    {
        this->current_leaf->sortKV();
        uint64_t leaf_size = this->current_leaf->findFirstZero();
        for (uint64_t i = 0; i < leaf_size; i++)
        {
            if (this->current_leaf->kv_pairs[i].key >= key)
            {
                this->bitmap_idx = i;
                return;
            }
        }
        this->current_leaf = this->current_leaf->p_next;
    }
}

KV FPtree::ScanNext()
{
    KV kv;
    assert(this->current_leaf != nullptr && "Current scan node was deleted!");
    kv = this->current_leaf->kv_pairs[this->bitmap_idx ++];
    if (this->bitmap_idx == MAX_LEAF_SIZE || this->current_leaf->bitmap.test(this->bitmap_idx) == 0)  // scan next leaf
    {
        this->current_leaf = this->current_leaf->p_next;
        if (this->current_leaf != nullptr)
        {
            this->current_leaf->sortKV();
            this->bitmap_idx = 0;
        }
    }
    return kv;
}

bool FPtree::ScanComplete()
{
    return this->current_leaf == nullptr;
}


int main() 
{
    FPtree fptree;

    size_t insertLength = 25;
    size_t randList[insertLength];
    // srand(time(NULL));
    srand(1);
    // for (size_t i = 0; i < insertLength; i++)
    //     randList[i] = i+1;
    
    // int size = sizeof(randList) / sizeof(randList[0]);

    // std::random_shuffle(randList, randList+size);

    // for (size_t i = 0; i < insertLength; i++)
    //     cout << randList[i] << endl;

    // for (size_t i = 0; i < insertLength; i++)
    //     fptree.insert(KV(randList[i], randList[i]));

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

    // fptree.displayTree(fptree.getRoot());

    // fptree.printFPTree("├──", fptree.getRoot());

    // cout << "find: " << fptree.find(9) << endl;
    // cout << "find: " << fptree.find(2) << endl;
    // cout << "find: " << fptree.find(3) << endl;
    // cout << "find: " << fptree.find(4) << endl;



    uint64_t key;
    while (true){
        std::cout << "\nEnter the key to insert or delete: "; 
        std::cin >> key;
        std::cout << std::endl;
        if (key == 0)
            break;
        else if (fptree.find(key) != 0)
            fptree.deleteKey(key);
        else
            fptree.insert(KV(key, key));
        fptree.printFPTree("├──", fptree.getRoot());
    }

    std::cout << "\nEnter the key to initialize scan: "; 
    std::cin >> key;
    std::cout << std::endl;
    fptree.ScanInitialize(key);
    while(!fptree.ScanComplete())
    {
        KV kv = fptree.ScanNext();
        std::cout << kv.key << "," << kv.value << " ";
    }
    std::cout << std::endl;
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