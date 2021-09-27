// Copyright (c) Simon Fraser University. All rights reserved.
// Licensed under the MIT license.
//
// Authors:
// Duo Lu <luduol@sfu.ca>
// George He <georgeh@sfu.ca>
// Tianzheng Wang <tzwang@sfu.ca>

#include "fptree.h"

#ifdef PMEM
    inline bool file_pool_exists(const std::string& name) 
    {
        return ( access( name.c_str(), F_OK ) != -1 );
    }
#endif

BaseNode::BaseNode() 
{
    this->isInnerNode = false;
}

InnerNode::InnerNode()
{
    this->isInnerNode = true;
    this->nKey = 0;
}

InnerNode::InnerNode(uint64_t key, BaseNode* left, BaseNode* right)
{
    this->isInnerNode = true;
    this->keys[0] = key;
    this->p_children[0] = left;
    this->p_children[1] = right;
    this->nKey = 1;
}

void InnerNode::init(uint64_t key, BaseNode* left, BaseNode* right)
{
    this->isInnerNode = true;
    this->keys[0] = key;
    this->p_children[0] = left;
    this->p_children[1] = right;
    this->nKey = 1;
}

InnerNode::InnerNode(const InnerNode& inner)
{
    memcpy(this, &inner, sizeof(struct InnerNode));
}

InnerNode::~InnerNode()
{
    for (size_t i = 0; i < this->nKey; i++) { delete this->p_children[i]; }
}

#ifndef PMEM
    LeafNode::LeafNode() 
    {
        this->isInnerNode = false;
        this->bitmap.clear();
        this->p_next = nullptr;
        this->lock.store(0, std::memory_order_acquire);
    }

    LeafNode::LeafNode(const LeafNode& leaf)
    {
        memcpy(this, &leaf, sizeof(struct LeafNode));
    }

    LeafNode& LeafNode::operator=(const LeafNode& leaf)
    {
        memcpy(this, &leaf, sizeof(struct LeafNode));
        return *this;
    }
#endif

void InnerNode::removeKey(uint64_t index, bool remove_right_child = true)
{
    assert(this->nKey > index && "Remove key index out of range!");
    this->nKey--;
    std::memmove(this->keys + index, this->keys + index + 1, (this->nKey-index)*sizeof(uint64_t));
    if (remove_right_child)
        index ++;
    std::memmove(this->p_children + index, this->p_children + index + 1, (this->nKey - index + 1)*sizeof(BaseNode*));
}

void InnerNode::addKey(uint64_t index, uint64_t key, BaseNode* child, bool add_child_right = true)
{
    assert(this->nKey >= index && "Insert key index out of range!");
    std::memmove(this->keys+index+1, this->keys+index, (this->nKey-index)*sizeof(uint64_t)); // move keys
    this->keys[index] = key;
    if (add_child_right)
        index ++;
    std::memmove(this->p_children+index+1, this->p_children+index, (this->nKey-index+1)*sizeof(BaseNode*));
    this->p_children[index] = child;
    this->nKey++;
}

inline uint64_t InnerNode::findChildIndex(uint64_t key)
{
    auto lower = std::lower_bound(this->keys, this->keys + this->nKey, key);
    uint64_t idx = lower - this->keys;
    if (idx < this->nKey && *lower == key)
        idx++;
    return idx;
}

inline void LeafNode::addKV(struct KV kv)
{
    uint64_t idx = this->bitmap.first_zero();
    assert(idx < MAX_LEAF_SIZE && "Insert kv out of bound!");
    this->fingerprints[idx] = getOneByteHash(kv.key);
    this->kv_pairs[idx] = kv;
    this->bitmap.set(idx);
}

inline uint64_t LeafNode::findKVIndex(uint64_t key)
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

uint64_t LeafNode::minKey()
{
    uint64_t min_key = -1, i = 0;
    for (; i < MAX_LEAF_SIZE; i++) 
    {
        if (this->bitmap.test(i) && this->kv_pairs[i].key < min_key)
            min_key = this->kv_pairs[i].key;
    }
    assert(min_key != -1 && "minKey called for empty leaf!");
    return min_key;
}

void LeafNode::getStat(uint64_t key, LeafNodeStat& lstat)
{
    lstat.count = 0;
    lstat.min_key = -1;
    lstat.kv_idx = MAX_LEAF_SIZE;

    uint64_t cur_key = -1;
    for (size_t counter = 0; counter < MAX_LEAF_SIZE; counter ++)
    {
        if (this->bitmap.test(counter))  // if find a valid entry
        {
            lstat.count ++;
            cur_key = this->kv_pairs[counter].key;
            if (cur_key == key)   // if the entry is key
                lstat.kv_idx = counter;
            else if (cur_key < lstat.min_key)
                lstat.min_key = cur_key;
        }  
    }
}

inline LeafNode* FPtree::maxLeaf(BaseNode* node)
{
    while(node->isInnerNode)
    {
        node = reinterpret_cast<InnerNode*> (node)->p_children[reinterpret_cast<InnerNode*> (node)->nKey];
    }
    return reinterpret_cast<LeafNode*> (node);
}

#ifdef PMEM
    static TOID(struct Log) allocLogArray()
    {
        TOID(struct Log) array = POBJ_ROOT(pop, struct Log);

        POBJ_ALLOC(pop, &array, struct Log, sizeof(struct Log) * sizeLogArray,
                    NULL, NULL);

        if (TOID_IS_NULL(array)) { fprintf(stderr, "POBJ_ALLOC\n"); return OID_NULL; }

        for (uint64_t i = 0; i < sizeLogArray; i++) 
        {
            if (POBJ_ALLOC(pop, &D_RW(array)[i],
                struct Log, sizeof(struct Log),
                NULL, NULL)) 
            {
                fprintf(stderr, "pmemobj_alloc\n");
            }
        }	
        return array.oid;
    }

    static TOID(struct Log) root_LogArray;

    void FPtree::recover()
    {
        root_LogArray = POBJ_ROOT(pop, struct Log);
        for (uint64_t i = 1; i < sizeLogArray / 2; i++)
        {
            recoverSplit(&D_RW(root_LogArray)[i]);
        }
        for (uint64_t i = sizeLogArray / 2; i < sizeLogArray; i++)
        {
            recoverDelete(&D_RW(root_LogArray)[i]);
        }
    }
#endif

FPtree::FPtree() 
{
    root = nullptr;
    #ifdef PMEM
        const char *path = "./test_pool";

        if (file_pool_exists(path) == 0) 
        {
            if ((pop = pmemobj_create(path, POBJ_LAYOUT_NAME(FPtree), PMEMOBJ_POOL_SIZE, 0666)) == NULL) 
                perror("failed to create pool\n");
            root_LogArray = allocLogArray();
        } 
        else 
        {
            if ((pop = pmemobj_open(path, POBJ_LAYOUT_NAME(FPtree))) == NULL)
                perror("failed to open pool\n");
            else 
            {
                recover();
                bulkLoad(1);
            }
        }
        root_LogArray = POBJ_ROOT(pop, struct Log);  // Avoid push root object to Queue, i = 1
        for (uint64_t i = 1; i < sizeLogArray / 2; i++)   // push persistent array to splitLogQueue
        {   
            D_RW(root_LogArray)[i].PCurrentLeaf = OID_NULL;
            D_RW(root_LogArray)[i].PLeaf = OID_NULL;
            splitLogQueue.push(&D_RW(root_LogArray)[i]);
        }
        for (uint64_t i = sizeLogArray / 2; i < sizeLogArray; i++) // second half of array use as delete log
        {
            D_RW(root_LogArray)[i].PCurrentLeaf = OID_NULL;
            D_RW(root_LogArray)[i].PLeaf = OID_NULL;
            deleteLogQueue.push(&D_RW(root_LogArray)[i]);
        }
    #else
        bitmap_idx = MAX_LEAF_SIZE;
    #endif
}


FPtree::~FPtree() 
{
    #ifdef PMEM
        pmemobj_close(pop);
    #else
        if (root != nullptr)
            delete root;
    #endif  
}


inline static uint8_t getOneByteHash(uint64_t key)
{
    uint8_t oneByteHashKey = std::_Hash_bytes(&key, sizeof(key), 1) & 0xff;
    return oneByteHashKey;
}


#ifdef PMEM
    static void showList()
    {
        TOID(struct List) ListHead = POBJ_ROOT(pop, struct List);
        TOID(struct LeafNode) leafNode = D_RO(ListHead)->head;

        while (!TOID_IS_NULL(leafNode)) 
        {
            for (size_t i = 0; i < MAX_LEAF_SIZE; i++)
            {
                if (D_RO(leafNode)->bitmap.test(i))
                    std::cout << "(" << D_RO(leafNode)->kv_pairs[i].key << " | " << 
                                        D_RO(leafNode)->kv_pairs[i].value << ")" << ", ";
            }
            std::cout << std::endl;
            leafNode = D_RO(leafNode)->p_next;
        }
    }

    static int constructLeafNode(PMEMobjpool *pop, void *ptr, void *arg)
    {
        struct LeafNode *node = (struct LeafNode *)ptr;
        struct argLeafNode *a = (struct argLeafNode *)arg;

        node->isInnerNode = a->isInnerNode;
        node->bitmap = a->bitmap;
        memcpy(node->fingerprints, a->fingerprints, sizeof(a->fingerprints));
        memcpy(node->kv_pairs, a->kv_pairs, sizeof(a->kv_pairs));
        node->p_next = TOID_NULL(struct LeafNode);
        node->lock = a->lock;

        pmemobj_persist(pop, node, a->size);

        return 0;
    }
#endif  



void FPtree::printFPTree(std::string prefix, BaseNode* root)
{
    if (root)
    {
        if (root->isInnerNode) 
        {
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
                    std::cout << prefix << node->kv_pairs[i].key << "," << node->kv_pairs[i].value << std::endl;
            }
        }
    }
}

inline LeafNode* FPtree::findLeaf(uint64_t key) 
{
    if (!root)
        return nullptr;
    if (!root->isInnerNode) 
        return reinterpret_cast<LeafNode*> (root);
    InnerNode* cursor = reinterpret_cast<InnerNode*> (root);
    while (cursor->isInnerNode) 
    {
        cursor = reinterpret_cast<InnerNode*> (cursor->p_children[cursor->findChildIndex(key)]);
    }
    return reinterpret_cast<LeafNode*> (cursor);
}

inline LeafNode* FPtree::findLeafAndPushInnerNodes(uint64_t key)
{
	if (!root)
		return nullptr;
    stack_innerNodes.clear();
    if (!root->isInnerNode) 
    {
    	stack_innerNodes.push(nullptr);
        return reinterpret_cast<LeafNode*> (root);
    }
    InnerNode* cursor = reinterpret_cast<InnerNode*> (root);
    while (cursor->isInnerNode)
    {
        stack_innerNodes.push(cursor);
        cursor = reinterpret_cast<InnerNode*> (cursor->p_children[cursor->findChildIndex(key)]);
    }
    return reinterpret_cast<LeafNode*> (cursor);
}


uint64_t FPtree::find(uint64_t key)
{
    LeafNode* pLeafNode;
    volatile uint64_t idx;
    tbb::speculative_spin_rw_mutex::scoped_lock lock_find;
    while (true)
    {
        lock_find.acquire(speculative_lock, false);
        if ((pLeafNode = findLeaf(key)) == nullptr) { lock_find.release(); break; }
        if (pLeafNode->lock) { lock_find.release(); continue; }
        idx = pLeafNode->findKVIndex(key);
        lock_find.release();
        return (idx != MAX_LEAF_SIZE ? pLeafNode->kv_pairs[idx].value : 0 );
    }
    return 0;
}


void FPtree::splitLeafAndUpdateInnerParents(LeafNode* reachedLeafNode, Result decision, struct KV kv, 
                                            bool updateFunc = false, uint64_t prevPos = MAX_LEAF_SIZE)
{
    uint64_t splitKey;

    #ifdef PMEM
        TOID(struct LeafNode) insertNode = pmemobj_oid(reachedLeafNode);
    #else
        LeafNode* insertNode = reachedLeafNode;
    #endif

    if (decision == Result::Split)
    {
        splitKey = splitLeaf(reachedLeafNode);       // split and link two leaves
        if (kv.key >= splitKey)                      // select one leaf to insert
            insertNode = reachedLeafNode->p_next;
    }

    #ifdef PMEM
        uint64_t slot = D_RW(insertNode)->bitmap.first_zero();
        assert(slot < MAX_LEAF_SIZE && "Slot idx out of bound");
        D_RW(insertNode)->kv_pairs[slot] = kv; 
        D_RW(insertNode)->fingerprints[slot] = getOneByteHash(kv.key);
        pmemobj_persist(pop, &D_RO(insertNode)->kv_pairs[slot], sizeof(struct KV));
        pmemobj_persist(pop, &D_RO(insertNode)->fingerprints[slot], SIZE_ONE_BYTE_HASH);

        if (!updateFunc)
        {
            D_RW(insertNode)->bitmap.set(slot);
        }
        else 
        {
            Bitset tmpBitmap = D_RW(insertNode)->bitmap;
            tmpBitmap.reset(prevPos); tmpBitmap.set(slot);
            D_RW(insertNode)->bitmap = tmpBitmap;
        }
        pmemobj_persist(pop, &D_RO(insertNode)->bitmap, sizeof(D_RO(insertNode)->bitmap));
    #else
        if (updateFunc)
            insertNode->kv_pairs[prevPos].value = kv.value;
        else
            insertNode->addKV(kv);
    #endif

    if (decision == Result::Split)
    {   
        LeafNode* newLeafNode;
    #ifdef PMEM
        newLeafNode = (struct LeafNode *) pmemobj_direct((reachedLeafNode->p_next).oid);
    #else
        newLeafNode = reachedLeafNode->p_next;
    #endif
        tbb::speculative_spin_rw_mutex::scoped_lock lock_split;
        uint64_t mid = MAX_INNER_SIZE / 2, new_splitKey, insert_pos;
        InnerNode* cur, *parent, *newInnerNode;
        BaseNode* child;
        short i = 0, idx;
        /*---------------- Second Critical Section -----------------*/
        lock_split.acquire(speculative_lock);
        if (!root->isInnerNode) // splitting when tree has only root 
        {
            cur = new InnerNode();
            cur->init(splitKey, reachedLeafNode, newLeafNode);
            root = cur;
        }
        else // need to retraverse & update parent
        {
            cur = reinterpret_cast<InnerNode*> (root);
            while(cur->isInnerNode)
            {
                inners[i] = cur;
                idx = std::lower_bound(cur->keys, cur->keys + cur->nKey, kv.key) - cur->keys;
                if (idx < cur->nKey && cur->keys[idx] == kv.key) // TODO: this should always be false
                    idx ++;
                ppos[i++] = idx;
                cur = reinterpret_cast<InnerNode*> (cur->p_children[idx]);
            }
            parent = inners[--i];
            child = newLeafNode;
            while (true)
            {
                insert_pos = ppos[i--];
                if (parent->nKey < MAX_INNER_SIZE)
                {
                    parent->addKey(insert_pos, splitKey, child);
                    break;
                }
                else 
                {
                    newInnerNode = new InnerNode();
                    parent->nKey = mid;
                    if (insert_pos != mid)
                    {
                        new_splitKey = parent->keys[mid];
                        std::copy(parent->keys + mid + 1, parent->keys + MAX_INNER_SIZE, newInnerNode->keys);
                        std::copy(parent->p_children + mid + 1, parent->p_children + MAX_INNER_SIZE + 1, newInnerNode->p_children);
                        newInnerNode->nKey = MAX_INNER_SIZE - mid - 1;
                        if (insert_pos < mid)
                            parent->addKey(insert_pos, splitKey, child);
                        else
                            newInnerNode->addKey(insert_pos - mid - 1, splitKey, child);
                    }
                    else 
                    {
                        new_splitKey = splitKey;
                        std::copy(parent->keys + mid, parent->keys + MAX_INNER_SIZE, newInnerNode->keys);
                        std::copy(parent->p_children + mid, parent->p_children + MAX_INNER_SIZE + 1, newInnerNode->p_children);
                        newInnerNode->p_children[0] = child;
                        newInnerNode->nKey = MAX_INNER_SIZE - mid;
                    }
                    splitKey = new_splitKey;
                    if (parent == root)
                    {
                        cur = new InnerNode(splitKey, parent, newInnerNode);
                        root = cur;
                        break;
                    }
                    parent = inners[i];
                    child = newInnerNode;
                }
            }
        }
        newLeafNode->Unlock();
        lock_split.release();
        /*---------------- End of Second Critical Section -----------------*/
    }
}


void FPtree::updateParents(uint64_t splitKey, InnerNode* parent, BaseNode* child) 
{
    uint64_t mid = floor(MAX_INNER_SIZE / 2);
    uint64_t new_splitKey, insert_pos;
    while (true)
    {
        if (parent->nKey < MAX_INNER_SIZE)
        {
            insert_pos = parent->findChildIndex(splitKey);
            parent->addKey(insert_pos, splitKey, child);
            return;
        }
        else 
        {
            InnerNode* newInnerNode = new InnerNode();
            insert_pos = std::lower_bound(parent->keys, parent->keys + MAX_INNER_SIZE, splitKey) - parent->keys;

            if (insert_pos < mid) {  // insert into parent node
                new_splitKey = parent->keys[mid];
                parent->nKey = mid;
                std::memmove(newInnerNode->keys, parent->keys + mid + 1, (MAX_INNER_SIZE - mid - 1)*sizeof(uint64_t));
                std::memmove(newInnerNode->p_children, parent->p_children + mid + 1, (MAX_INNER_SIZE - mid)*sizeof(BaseNode*));
                newInnerNode->nKey = MAX_INNER_SIZE - mid - 1;
                parent->addKey(insert_pos, splitKey, child);
            }
            else if (insert_pos > mid) {  // insert into new innernode
                new_splitKey = parent->keys[mid];
                parent->nKey = mid;
                std::memmove(newInnerNode->keys, parent->keys + mid + 1, (MAX_INNER_SIZE - mid - 1)*sizeof(uint64_t));
                std::memmove(newInnerNode->p_children, parent->p_children + mid + 1, (MAX_INNER_SIZE - mid)*sizeof(BaseNode*));
                newInnerNode->nKey = MAX_INNER_SIZE - mid - 1;
                newInnerNode->addKey(insert_pos - mid - 1, splitKey, child);
            }
            else {  // only insert child to new innernode, splitkey does not change
                new_splitKey = splitKey;
                parent->nKey = mid;
                std::memmove(newInnerNode->keys, parent->keys + mid, (MAX_INNER_SIZE - mid)*sizeof(uint64_t));
                std::memmove(newInnerNode->p_children, parent->p_children + mid, (MAX_INNER_SIZE - mid + 1)*sizeof(BaseNode*));
                newInnerNode->p_children[0] = child;
                newInnerNode->nKey = MAX_INNER_SIZE - mid;
            }

            splitKey = new_splitKey;

            if (parent == root)
            {
                root = new InnerNode(splitKey, parent, newInnerNode);
                return;
            }
            parent = stack_innerNodes.pop();
            child = newInnerNode;
        }
    }
}


bool FPtree::update(struct KV kv)
{
    tbb::speculative_spin_rw_mutex::scoped_lock lock_update;
    LeafNode* reachedLeafNode;
    volatile uint64_t prevPos;
    volatile Result decision = Result::Abort;
    while (decision == Result::Abort)
    {
        // std::this_thread::sleep_for(std::chrono::nanoseconds(1));
        lock_update.acquire(speculative_lock, false);
        if ((reachedLeafNode = findLeaf(kv.key)) == nullptr) { lock_update.release(); return false; }
        if (!reachedLeafNode->Lock()) { lock_update.release(); continue; }
        prevPos = reachedLeafNode->findKVIndex(kv.key);
        if (prevPos == MAX_LEAF_SIZE) // key not found
        {
            reachedLeafNode->Unlock();
            lock_update.release();
            return false;
        }
        decision = reachedLeafNode->isFull() ? Result::Split : Result::Update;
        lock_update.release();
    }

    splitLeafAndUpdateInnerParents(reachedLeafNode, decision, kv, true, prevPos);

    reachedLeafNode->Unlock();
    
    return true;
}


bool FPtree::insert(struct KV kv) 
{
    tbb::speculative_spin_rw_mutex::scoped_lock lock_insert;
    if (!root)  // if tree is empty
    {
        lock_insert.acquire(speculative_lock, true);
        if (!root)
        {
            #ifdef PMEM
                struct argLeafNode args(kv);
                TOID(struct List) ListHead = POBJ_ROOT(pop, struct List);
                TOID(struct LeafNode) *dst = &D_RW(ListHead)->head;
                POBJ_ALLOC(pop, dst, struct LeafNode, args.size, constructLeafNode, &args);
                D_RW(ListHead)->head = *dst; 
                pmemobj_persist(pop, &D_RO(ListHead)->head, sizeof(D_RO(ListHead)->head));
                root = (struct BaseNode *) pmemobj_direct((*dst).oid);
            #else
                root = new LeafNode();
                reinterpret_cast<LeafNode*>(root)->lock = 1;
                reinterpret_cast<LeafNode*> (root)->addKV(kv);
                reinterpret_cast<LeafNode*>(root)->lock = 0;
            #endif
            lock_insert.release();
            return true;
        }
        lock_insert.release();
    }

    Result decision = Result::Abort;
    InnerNode* cursor;
    LeafNode* reachedLeafNode;
    uint64_t nKey;
    int idx;
    /*---------------- First Critical Section -----------------*/
    {
    TBB_BEGIN:
        lock_insert.acquire(speculative_lock, false);
        reachedLeafNode = findLeaf(kv.key);
        if (!reachedLeafNode->Lock()) 
        { 
            lock_insert.release(); 
            goto TBB_BEGIN;
        }
        idx = reachedLeafNode->findKVIndex(kv.key);
        if (idx != MAX_LEAF_SIZE)
            reachedLeafNode->Unlock();
        else
            decision = reachedLeafNode->isFull() ? Result::Split : Result::Insert;
        lock_insert.release();
    }
    /*---------------- End of First Critical Section -----------------*/

    if (decision == Result::Abort)  // kv already exists
        return false;

    splitLeafAndUpdateInnerParents(reachedLeafNode, decision, kv);

    reachedLeafNode->Unlock();
    
    return true;
}



uint64_t FPtree::splitLeaf(LeafNode* leaf)
{
    uint64_t splitKey = findSplitKey(leaf);
    #ifdef PMEM
        TOID(struct LeafNode) *dst = &(leaf->p_next);
        TOID(struct LeafNode) nextLeafNode = leaf->p_next;

        // Get uLog from splitLogQueue
        Log* log;
        if (!splitLogQueue.pop(log)) { assert("Split log queue pop error!"); }

        //set uLog.PCurrentLeaf to persistent address of Leaf
        log->PCurrentLeaf = pmemobj_oid(leaf);
        pmemobj_persist(pop, &(log->PCurrentLeaf), SIZE_PMEM_POINTER);

        // Copy the content of Leaf into NewLeaf
        struct argLeafNode args(leaf);

        log->PLeaf = *dst;

        POBJ_ALLOC(pop, dst, struct LeafNode, args.size, constructLeafNode, &args);

        for (size_t i = 0; i < MAX_LEAF_SIZE; i++)
        {
            if (D_RO(*dst)->kv_pairs[i].key < splitKey)
                D_RW(*dst)->bitmap.reset(i);
        }
        // Persist(NewLeaf.Bitmap)
        pmemobj_persist(pop, &D_RO(*dst)->bitmap, sizeof(D_RO(*dst)->bitmap));

        // Leaf.Bitmap = inverse(NewLeaf.Bitmap)
        leaf->bitmap = D_RO(*dst)->bitmap;
        if constexpr (MAX_LEAF_SIZE != 1)  leaf->bitmap.flip();

        // Persist(Leaf.Bitmap)
        pmemobj_persist(pop, &leaf->bitmap, sizeof(leaf->bitmap));

        // Persist(Leaf.Next)
        D_RW(*dst)->p_next = nextLeafNode;
        pmemobj_persist(pop, &D_RO(*dst)->p_next, sizeof(D_RO(*dst)->p_next));

        // reset uLog
        log->PCurrentLeaf = OID_NULL;
        log->PLeaf = OID_NULL;
        pmemobj_persist(pop, &(log->PCurrentLeaf), SIZE_PMEM_POINTER);
        pmemobj_persist(pop, &(log->PLeaf), SIZE_PMEM_POINTER);
        splitLogQueue.push(log);
    #else
        LeafNode* newLeafNode = new LeafNode(*leaf);

        for (size_t i = 0; i < MAX_LEAF_SIZE; i++)
        {
            if (newLeafNode->kv_pairs[i].key < splitKey)
                newLeafNode->bitmap.reset(i);
        }

        leaf->bitmap = newLeafNode->bitmap;
        leaf->bitmap.flip();
        leaf->p_next = newLeafNode;
    #endif
 
    return splitKey;
}


uint64_t FPtree::findSplitKey(LeafNode* leaf)
{
    KV tempArr[MAX_LEAF_SIZE];
    memcpy(tempArr, leaf->kv_pairs, sizeof(leaf->kv_pairs));
    // TODO: find median in one pass instead of sorting
    std::sort(std::begin(tempArr), std::end(tempArr), [] (const KV& kv1, const KV& kv2){
            return kv1.key < kv2.key;
        });

    uint64_t mid = floor(MAX_LEAF_SIZE / 2);
    uint64_t splitKey = tempArr[mid].key;

    return splitKey;
}


#ifdef PMEM
    void FPtree::recoverSplit(Log* uLog)
    {
        if (TOID_IS_NULL(uLog->PCurrentLeaf))
        {
            return;
        }
        
        if (TOID_IS_NULL(uLog->PLeaf))
        {
            uLog->PCurrentLeaf = OID_NULL;
            uLog->PLeaf = OID_NULL;
            return;
        }
        else
        {
            LeafNode* leaf = (struct LeafNode *) pmemobj_direct((uLog->PCurrentLeaf).oid);
            uint64_t splitKey = findSplitKey(leaf);
            if (leaf->isFull())  // Crashed before inverse the current leaf 
            {
                for (size_t i = 0; i < MAX_LEAF_SIZE; i++)
                {
                    if (D_RO(uLog->PLeaf)->kv_pairs[i].key < splitKey)
                        D_RW(uLog->PLeaf)->bitmap.reset(i);
                }
                // Persist(NewLeaf.Bitmap)
                pmemobj_persist(pop, &D_RO(uLog->PLeaf)->bitmap, sizeof(D_RO(uLog->PLeaf)->bitmap));

                // Leaf.Bitmap = inverse(NewLeaf.Bitmap)
                D_RW(uLog->PCurrentLeaf)->bitmap = D_RO(uLog->PLeaf)->bitmap;
                if constexpr (MAX_LEAF_SIZE != 1)  D_RW(uLog->PCurrentLeaf)->bitmap.flip();

                // Persist(Leaf.Bitmap)
                pmemobj_persist(pop, &D_RO(uLog->PCurrentLeaf)->bitmap, sizeof(D_RO(uLog->PCurrentLeaf)->bitmap));

                // Persist(Leaf.Next)
                D_RW(uLog->PCurrentLeaf)->p_next = uLog->PLeaf;
                pmemobj_persist(pop, &D_RO(uLog->PLeaf)->p_next, sizeof(D_RO(uLog->PLeaf)->p_next));

                // reset uLog
                uLog->PCurrentLeaf = OID_NULL;
                uLog->PLeaf = OID_NULL;
                return;
            }
            else    // Crashed after inverse the current leaf 
            {
                // Leaf.Bitmap = inverse(NewLeaf.Bitmap)
                if constexpr (MAX_LEAF_SIZE != 1)  D_RW(uLog->PCurrentLeaf)->bitmap.flip();
    
                // Persist(Leaf.Bitmap)
                pmemobj_persist(pop, &D_RO(uLog->PCurrentLeaf)->bitmap, sizeof(D_RO(uLog->PCurrentLeaf)->bitmap));

                // Persist(Leaf.Next)
                D_RW(uLog->PCurrentLeaf)->p_next = uLog->PLeaf;
                pmemobj_persist(pop, &D_RO(uLog->PLeaf)->p_next, sizeof(D_RO(uLog->PLeaf)->p_next));

                // reset uLog
                uLog->PCurrentLeaf = OID_NULL;
                uLog->PLeaf = OID_NULL;
                return;
            }
        }
    }
#endif

void FPtree::removeLeafAndMergeInnerNodes(short i, short indexNode_level)
{
    InnerNode* temp, *left, *right, *parent = inners[i];
    uint64_t left_idx, new_key = 0, child_idx = ppos[i];

    if (child_idx == 0)
    {
        new_key = parent->keys[0];
        parent->removeKey(child_idx, false);
        if (indexNode_level >= 0 && inners[indexNode_level] != parent)
            inners[indexNode_level]->keys[ppos[indexNode_level] - 1] = new_key;
    }
    else
        parent->removeKey(child_idx - 1, true);

    while (!parent->nKey) // parent has no key, merge with sibling
    {
        if (parent == root) // entire tree stores 1 kv, convert the only leafnode into root
        {
            temp = reinterpret_cast<InnerNode*> (root);
            root = parent->p_children[0];
            delete temp;
            break;         
        }
        parent = inners[--i];
        child_idx = ppos[i];
        left_idx = child_idx;
        if (!(child_idx != 0 && tryBorrowKey(parent, child_idx, child_idx-1)) && 
            !(child_idx != parent->nKey && tryBorrowKey(parent, child_idx, child_idx+1))) // if cannot borrow from any siblings
        {
            if (left_idx != 0)
                left_idx --;
            left = reinterpret_cast<InnerNode*> (parent->p_children[left_idx]);
            right = reinterpret_cast<InnerNode*> (parent->p_children[left_idx + 1]);

            if (left->nKey == 0)
            {
                right->addKey(0, parent->keys[left_idx], left->p_children[0], false);
                delete left;
                parent->removeKey(left_idx, false);
            }
            else
            {
                left->addKey(left->nKey, parent->keys[left_idx], right->p_children[0]);
                delete right;
                parent->removeKey(left_idx);
            }
        }
        else
            break;
    }
}

bool FPtree::deleteKey(uint64_t key)
{
    LeafNode* leaf, *sibling;
    InnerNode *parent, *cur; 
    tbb::speculative_spin_rw_mutex::scoped_lock lock_delete;
    Result decision = Result::Abort;
    LeafNodeStat lstat;
    short i, idx, indexNode_level, sib_level;
    while (decision == Result::Abort) 
    {
        i = 0; indexNode_level = -1, sib_level = -1;
        sibling = nullptr; 
        /*---------------- Critical Section -----------------*/
        lock_delete.acquire(speculative_lock, true);

        if (!root) { lock_delete.release(); return false;} // empty tree
        cur = reinterpret_cast<InnerNode*> (root);
        while (cur->isInnerNode)
        {
            inners[i] = cur;
            idx = std::lower_bound(cur->keys, cur->keys + cur->nKey, key) - cur->keys;
            if (idx < cur->nKey && cur->keys[idx] == key) // just found index node
            {
                indexNode_level = i;
                idx ++;
            }
            if (idx != 0)
                sib_level = i;
            ppos[i++] = idx;
            cur = reinterpret_cast<InnerNode*> (cur->p_children[idx]);
        }
        parent = inners[--i];
        leaf = reinterpret_cast<LeafNode*> (cur);

        if (!leaf->Lock()) { lock_delete.release(); continue; }
        leaf->getStat(key, lstat);
        if (lstat.kv_idx == MAX_LEAF_SIZE) // key not found
        {
            decision = Result::NotFound;
            leaf->Unlock();
        }
        else if (lstat.count > 1)   // leaf contains key and other keys
        {
            if (indexNode_level >= 0) // key appears in an inner node, need to replace
                inners[indexNode_level]->keys[ppos[indexNode_level] - 1] = lstat.min_key;
            decision = Result::Remove;
        }
        else // leaf contains key only
        {
            if (parent) // try lock left sibling if exist, then remove leaf from parent and update inner nodes
            {
                if (sib_level >= 0)   // left sibling exists
                {
                    cur = reinterpret_cast<InnerNode*> (inners[sib_level]->p_children[ppos[sib_level] - 1]);
                    while (cur->isInnerNode)
                        cur = reinterpret_cast<InnerNode*> (cur->p_children[cur->nKey]);
                    sibling = reinterpret_cast<LeafNode*> (cur);
                    if (!sibling->Lock())
                    {
                        lock_delete.release(); leaf->Unlock(); continue;
                    }
                }
                removeLeafAndMergeInnerNodes(i, indexNode_level);
            }
            decision = Result::Delete;
        }
        lock_delete.release();
        /*---------------- Critical Section -----------------*/
    }
    if (decision == Result::Remove)
    {
        leaf->bitmap.reset(lstat.kv_idx);
        #ifdef PMEM
            TOID(struct LeafNode) lf = pmemobj_oid(leaf);
            pmemobj_persist(pop, &D_RO(lf)->bitmap, sizeof(D_RO(lf)->bitmap));
        #endif
        leaf->Unlock();
    }
    else if (decision == Result::Delete)
    {
        #ifdef PMEM
            TOID(struct LeafNode) lf = pmemobj_oid(leaf);
            
            // Get uLog from deleteLogQueue
            Log* log;
            if (!deleteLogQueue.pop(log)) { assert("Delete log queue pop error!"); }

            //set uLog.PCurrentLeaf to persistent address of Leaf
            log->PCurrentLeaf = lf;
            pmemobj_persist(pop, &(log->PCurrentLeaf), SIZE_PMEM_POINTER);

            if (sibling) // set and persist sibling's p_next, then unlock sibling node
            {
                TOID(struct LeafNode) sib = pmemobj_oid(sibling);
                
                log->PLeaf = sib;
                pmemobj_persist(pop, &(log->PLeaf), SIZE_PMEM_POINTER);

                D_RW(sib)->p_next = D_RO(lf)->p_next;
                pmemobj_persist(pop, &D_RO(sib)->p_next, sizeof(D_RO(sib)->p_next));
                sibling->Unlock();
            }
            else if (parent) // the node to delete is left most node, set and persist list head instead
            {
                TOID(struct List) ListHead = POBJ_ROOT(pop, struct List);
                D_RW(ListHead)->head = D_RO(lf)->p_next; 
                pmemobj_persist(pop, &D_RO(ListHead)->head, sizeof(D_RO(ListHead)->head));
            }
            else
            {
                TOID(struct List) ListHead = POBJ_ROOT(pop, struct List);
                D_RW(ListHead)->head = OID_NULL; 
                pmemobj_persist(pop, &D_RO(ListHead)->head, sizeof(D_RO(ListHead)->head));
                root = nullptr;
            }
            POBJ_FREE(&lf);

            // reset uLog
            log->PCurrentLeaf = OID_NULL;
            log->PLeaf = OID_NULL;
            pmemobj_persist(pop, &(log->PCurrentLeaf), SIZE_PMEM_POINTER);
            pmemobj_persist(pop, &(log->PLeaf), SIZE_PMEM_POINTER);
            deleteLogQueue.push(log);
        #else
            if (sibling)
            {
                sibling->p_next = leaf->p_next;
                sibling->Unlock();
            }
            else if (!parent)
                root = nullptr;
            delete leaf;
        #endif
    }
    return decision != Result::NotFound;
}

#ifdef PMEM
    void FPtree::recoverDelete(Log* uLog)
    {
        TOID(struct List) ListHead = POBJ_ROOT(pop, struct List);
        TOID(struct LeafNode) PHead = D_RW(ListHead)->head;

        if( (!TOID_IS_NULL(uLog->PCurrentLeaf)) && (!TOID_IS_NULL(PHead)) )
        {
            D_RW(uLog->PLeaf)->p_next = D_RO(uLog->PCurrentLeaf)->p_next;
            pmemobj_persist(pop, &D_RO(uLog->PLeaf)->p_next, SIZE_PMEM_POINTER);
            D_RW(uLog->PLeaf)->Unlock();
            POBJ_FREE(&(uLog->PCurrentLeaf));
        }
        else
        {
            if ( (!TOID_IS_NULL(uLog->PCurrentLeaf)) && 
                ((struct LeafNode *) pmemobj_direct((uLog->PCurrentLeaf).oid) == 
                (struct LeafNode *) pmemobj_direct(PHead.oid)) 
            )
            {
                PHead = D_RO(uLog->PCurrentLeaf)->p_next; 
                pmemobj_persist(pop, &PHead, SIZE_PMEM_POINTER);
                POBJ_FREE(&(uLog->PCurrentLeaf));
            }
            else 
            {
                if ( (!TOID_IS_NULL(uLog->PCurrentLeaf)) && 
                    ((struct LeafNode *) pmemobj_direct((D_RO(uLog->PCurrentLeaf)->p_next).oid) == 
                    (struct LeafNode *) pmemobj_direct(PHead.oid)) 
                )
                    POBJ_FREE(&(uLog->PCurrentLeaf));
                else { /* reset uLog */ }
            }
        }

        // reset uLog
        uLog->PCurrentLeaf = OID_NULL;
        uLog->PLeaf = OID_NULL;
        return;
    }
#endif

bool FPtree::tryBorrowKey(InnerNode* parent, uint64_t receiver_idx, uint64_t sender_idx)
{
    InnerNode* sender = reinterpret_cast<InnerNode*> (parent->p_children[sender_idx]);
    if (sender->nKey <= 1)      // sibling has only 1 key, cannot borrow
        return false;
    InnerNode* receiver = reinterpret_cast<InnerNode*> (parent->p_children[receiver_idx]);
    if (receiver_idx < sender_idx)  // borrow from right sibling
    {
        receiver->addKey(0, parent->keys[receiver_idx], sender->p_children[0]);
        parent->keys[receiver_idx] = sender->keys[0];
        sender->removeKey(0, false);
    }
    else  // borrow from left sibling
    {
        receiver->addKey(0, receiver->keys[0], sender->p_children[sender->nKey], false);
        parent->keys[sender_idx] = sender->keys[sender->nKey-1];
        sender->removeKey(sender->nKey-1);
    }
    return true;
}

inline uint64_t FPtree::minKey(BaseNode* node)
{
    while (node->isInnerNode)
        node = reinterpret_cast<InnerNode*> (node)->p_children[0];
    return reinterpret_cast<LeafNode*> (node)->minKey();
}

LeafNode* FPtree::minLeaf(BaseNode* node)
{
    while(node->isInnerNode)
        node = reinterpret_cast<InnerNode*> (node)->p_children[0];
    return reinterpret_cast<LeafNode*> (node);
}


void FPtree::sortKV()
{
    uint64_t j = 0;
    for (uint64_t i = 0; i < MAX_LEAF_SIZE; i++)
        if (this->current_leaf->bitmap.test(i))
            this->volatile_current_kv[j++] = this->current_leaf->kv_pairs[i];
    
    this->size_volatile_kv = j;

    std::sort(std::begin(this->volatile_current_kv), std::begin(this->volatile_current_kv) + this->size_volatile_kv, 
    [] (const KV& kv1, const KV& kv2){
        return kv1.key < kv2.key;
    });
}


void FPtree::scanInitialize(uint64_t key)
{
    if (!root)
        return;

    this->current_leaf = root->isInnerNode? findLeaf(key) : reinterpret_cast<LeafNode*> (root);
    while (this->current_leaf != nullptr)
    {
        this->sortKV();
        for (uint64_t i = 0; i < this->size_volatile_kv; i++)
        {
            if (this->volatile_current_kv[i].key >= key)
            {
                this->bitmap_idx = i;
                return;
            }
        }
        #ifdef PMEM
            this->current_leaf = (struct LeafNode *) pmemobj_direct((this->current_leaf->p_next).oid);
        #else
            this->current_leaf = this->current_leaf->p_next;
        #endif
    }
}


KV FPtree::scanNext()
{
    assert(this->current_leaf != nullptr && "Current scan node was deleted!");
    struct KV kv = this->volatile_current_kv[this->bitmap_idx++];
    if (this->bitmap_idx == this->size_volatile_kv)
    {
        #ifdef PMEM
            this->current_leaf = (struct LeafNode *) pmemobj_direct((this->current_leaf->p_next).oid);
        #else
            this->current_leaf = this->current_leaf->p_next;
        #endif
        if (this->current_leaf != nullptr)
        {
            this->sortKV();
            this->bitmap_idx = 0;
        }
    }
    return kv;
}


bool FPtree::scanComplete()
{
    return this->current_leaf == nullptr;
}


uint64_t FPtree::rangeScan(uint64_t key, uint64_t scan_size, char* result)
{
    LeafNode* leaf, * next_leaf;
    std::vector<KV> records;
    records.reserve(scan_size);
    uint64_t i;
    tbb::speculative_spin_rw_mutex::scoped_lock lock_scan;
    while (true) 
    {
        lock_scan.acquire(speculative_lock, false);
        if ((leaf = findLeaf(key)) == nullptr) { lock_scan.release(); return 0; }
        if (!leaf->Lock()) { lock_scan.release(); continue; }
        for (i = 0; i < MAX_LEAF_SIZE; i++)
            if (leaf->bitmap.test(i) && leaf->kv_pairs[i].key >= key)
                records.push_back(leaf->kv_pairs[i]);
        while (records.size() < scan_size)
        {
            #ifdef PMEM
                if (TOID_IS_NULL(leaf->p_next))
                    break;
                next_leaf = (struct LeafNode *) pmemobj_direct((leaf->p_next).oid);
            #else
                if ((next_leaf = leaf->p_next) == nullptr)
                    break;
            #endif
            while (!next_leaf->Lock())
                std::this_thread::sleep_for(std::chrono::nanoseconds(1));
            leaf->Unlock();
            leaf = next_leaf;
            for (i = 0; i < MAX_LEAF_SIZE; i++)
                if (leaf->bitmap.test(i))
                    records.push_back(leaf->kv_pairs[i]);
        }
        lock_scan.release();
        break;
    }
    if (leaf && leaf->lock == 1)
        leaf->Unlock();
    std::sort(records.begin(), records.end(), [] (const KV& kv1, const KV& kv2) {
            return kv1.key < kv2.key;
    });
    // result = new char[sizeof(KV) * records.size()];
    i = records.size() > scan_size? scan_size : records.size();
    memcpy(result, records.data(), sizeof(KV) * i);
    return i;
}



#ifdef PMEM
    bool FPtree::bulkLoad(float load_factor = 1)
    {
        TOID(struct List) ListHead = POBJ_ROOT(pop, struct List);
        TOID(struct LeafNode) cursor = D_RW(ListHead)->head;

        if (TOID_IS_NULL(cursor)) { this->root = nullptr; return true; }

        if (TOID_IS_NULL(D_RO(cursor)->p_next)) 
            { root = (struct BaseNode *) pmemobj_direct(cursor.oid); return true;}

        std::vector<uint64_t> min_keys;
        std::vector<LeafNode*> child_nodes;
        uint64_t total_leaves = 0;
        LeafNode* temp_leafnode;
        while(!TOID_IS_NULL(cursor))   // record min keys and leaf nodes 
        {
            temp_leafnode = (struct LeafNode *) pmemobj_direct(cursor.oid);
            child_nodes.push_back(temp_leafnode);
            min_keys.push_back(temp_leafnode->minKey());
            cursor = D_RW(cursor)->p_next;
        }
        total_leaves = min_keys.size();
        min_keys.erase(min_keys.begin());

        InnerNode* new_root = new InnerNode();
        uint64_t idx = 0;
        uint64_t root_size = total_leaves <= MAX_INNER_SIZE ? 
                             total_leaves : MAX_INNER_SIZE + 1;
        for (; idx < root_size; idx++)     // recovery the root node
        {   
            if (idx < root_size - 1)
                new_root->keys[idx] = min_keys[idx];
            new_root->p_children[idx] = child_nodes[idx];
        }
        new_root->nKey = root_size - 1;
        this->root = reinterpret_cast<BaseNode*> (new_root);

        if (total_leaves > MAX_INNER_SIZE)
        {
            idx--;
            right_most_innnerNode = reinterpret_cast<InnerNode*>(this->root); 
            for (; idx < min_keys.size(); idx++)   // Index entries for leaf pages always entered into right-most index page
            {
                findLeafAndPushInnerNodes(min_keys[idx]);
                right_most_innnerNode = stack_innerNodes.pop();
                updateParents(min_keys[idx], right_most_innnerNode, child_nodes[idx+1]);
            }
        }
        return true;
    }
#endif


/*
    Use case
    uint64_t tick = rdtsc();
    Put program between 
    std::cout << rdtsc() - tick << std::endl;
*/
uint64_t rdtsc(){
    unsigned int lo,hi;
    __asm__ __volatile__ ("rdtsc" : "=a" (lo), "=d" (hi));
    return ((uint64_t)hi << 32) | lo;
}



#if BUILD_INSPECTOR == 0
    int main(int argc, char *argv[]) 
    {
        srand( (unsigned) time(NULL) * getpid());
        FPtree fptree;

        #ifdef PMEM
            const char* command = argv[1];
            if (command != NULL && strcmp(command, "show") == 0)
            {  
                showList();
                return 0;
            }
        #endif

        int64_t key;
        uint64_t value;
        while (true)
        {
            fptree.printFPTree("├──", fptree.getRoot());
            std::cout << "\nEnter the key to insert, delete or update (-1): "; 
            std::cin >> key;
            std::cout << std::endl;
            KV kv = KV(key, key);
            if (key == 0)
                break;
            else if (key == -1)
            {
                std::cout << "\nEnter the key to update: ";
                std::cin >> key;
                std::cout << "\nEnter the value to update: ";
                std::cin >> value;
                fptree.update(KV(key, value));
            }
            else if (fptree.find(kv.key))
                fptree.deleteKey(kv.key);
            else
            {
                // fptree.insert(kv);
                if (!fptree.getRoot())
                    for (size_t i = 0; i < 100; i++)
                        fptree.insert(KV(rand() % 100 + 2, 1));
                else
                    fptree.insert(kv);
            }
            // fptree.printFPTree("├──", fptree.getRoot());
            #ifdef PMEM
                std::cout << std::endl;
                std::cout << "show list: " << std::endl;
                showList();
            #endif
        }

        std::cout << "\nEnter the key to initialize scan: "; 
        std::cin >> key;
        std::cout << std::endl;
        fptree.scanInitialize(key);
        while(!fptree.scanComplete())
        {
            KV kv = fptree.scanNext();
            std::cout << kv.key << "," << kv.value << " ";
        }
        std::cout << std::endl;
    }
#endif
