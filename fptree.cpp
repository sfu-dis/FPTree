#include "fptree.h"


#ifdef PMEM
    inline bool file_pool_exists(const std::string& name) {
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

InnerNode::InnerNode(const InnerNode& inner)
{
    this->isInnerNode = true;
    this->nKey = inner.nKey;
    memcpy(this->keys, inner.keys, sizeof(inner.keys));
    memcpy(this->p_children, inner.p_children, sizeof(inner.p_children));
}

InnerNode::~InnerNode()
{
    for (size_t i = 0; i < this->nKey; i++)
        delete this->p_children[i];
}

#ifdef PMEM
#else
    LeafNode::LeafNode() 
    {
        this->isInnerNode = false;
        this->bitmap.reset();
        this->p_next = nullptr;
        this->lock = 0;
    }

    LeafNode::LeafNode(const LeafNode& leaf)
    {
        this->isInnerNode = false;
        this->bitmap = leaf.bitmap;
        memcpy(this->fingerprints, leaf.fingerprints, sizeof(leaf.fingerprints));
        memcpy(this->kv_pairs, leaf.kv_pairs, sizeof(leaf.kv_pairs));
        this->p_next = leaf.p_next;
        this->lock = leaf.lock;
    }

    LeafNode& LeafNode::operator=(const LeafNode& leaf)
    {
        this->isInnerNode = false;
        this->bitmap = leaf.bitmap;
        memcpy(this->fingerprints, leaf.fingerprints, sizeof(leaf.fingerprints));
        memcpy(this->kv_pairs, leaf.kv_pairs, sizeof(leaf.kv_pairs));
        this->p_next = leaf.p_next;
        this->lock = leaf.lock;
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

void InnerNode::updateKey(uint64_t key)
{
    auto lower = std::lower_bound(this->keys, this->keys + this->nKey, key);
    uint64_t idx = lower - this->keys;
    if (idx < this->nKey && *lower == key)
    {
        BaseNode* node = this->p_children[idx+1];
        while (node->isInnerNode)
            node = reinterpret_cast<InnerNode*> (node)->p_children[0];
        this->keys[idx]= reinterpret_cast<LeafNode*> (node)->minKV(false).key;
    }
}

inline uint64_t LeafNode::findFirstZero()
{
    std::bitset<MAX_LEAF_SIZE> b = bitmap;
    return b.flip()._Find_first();
}

inline void LeafNode::addKV(struct KV kv)
{
    uint64_t idx = this->findFirstZero();
    assert(idx != MAX_LEAF_SIZE && "Insert kv out of bound!");
    this->fingerprints[idx] = getOneByteHash(kv.key);
    this->kv_pairs[idx] = kv;
    this->bitmap.set(idx);
}

inline uint64_t LeafNode::removeKV(uint64_t key)
{
    uint64_t idx = findKVIndex(key);
    assert(idx != MAX_LEAF_SIZE && "Remove kv out of bound!");
    return this->removeKVByIdx(idx);
}

inline uint64_t LeafNode::removeKVByIdx(uint64_t pos)
{
    assert(this->bitmap.test(pos) == true);
    this->bitmap.set(pos, 0);
    return this->kv_pairs[pos].value;
}

inline uint64_t LeafNode::findKVIndex(uint64_t key)
{
    size_t key_hash = getOneByteHash(key);

    #ifdef PMEM
        __attribute__((aligned(64))) uint8_t tmp_fingerprints[MAX_LEAF_SIZE];
        memcpy(tmp_fingerprints, this->fingerprints, sizeof(this->fingerprints));
    #endif

    __m512i key_64B = _mm512_set1_epi8((char)key_hash);

       // b. load meta into another 16B register
    #ifdef PMEM
        __m512i fgpt_64B= _mm512_load_si512((__m512i*)tmp_fingerprints);
    #else
        __m512i fgpt_64B= _mm512_load_si512((__m512i*)this->fingerprints);
    #endif

       // c. compare them
    uint64_t mask = uint64_t(_mm512_cmpeq_epi8_mask(key_64B, fgpt_64B));

    mask &= offset;

    size_t counter = 0;
    while (mask != 0) {
        if (mask & 1 && this->bitmap.test(counter) && key == this->kv_pairs[counter].key)
            return counter;
        mask >>= 1;
        counter ++;
    }
    return MAX_LEAF_SIZE;
}

KV LeafNode::minKV(bool remove = false)
{
    uint64_t min_key = -1, min_key_idx = 0;
    for (uint64_t i = 0; i < MAX_LEAF_SIZE; i++) 
        if (this->bitmap.test(i) && this->kv_pairs[i].key <= min_key)
        {
            min_key = this->kv_pairs[i].key;
            min_key_idx = i;
        }
    assert(min_key_idx >= 0 && min_key_idx < MAX_LEAF_SIZE && "minKV out of bound");
    if (remove)
        bitmap.set(min_key_idx, 0);
    return this->kv_pairs[min_key_idx];
}

KV LeafNode::maxKV(bool remove = false)
{
    uint64_t max_key = 0, max_key_idx = 0;
    for (uint64_t i = 0; i < MAX_LEAF_SIZE; i++) 
        if (this->bitmap.test(i) && this->kv_pairs[i].key >= max_key)
        {
            max_key = this->kv_pairs[i].key;
            max_key_idx = i;
        }
    assert(min_key_idx >= 0 && min_key_idx < MAX_LEAF_SIZE && "maxKV out of bound");
    if (remove)
        bitmap.set(max_key_idx, 0);
    return this->kv_pairs[max_key_idx];
}

inline LeafNode* FPtree::maxLeaf(BaseNode* node)
{
    while(node->isInnerNode)
        node = reinterpret_cast<InnerNode*> (node)->p_children[reinterpret_cast<InnerNode*> (node)->nKey];
    return reinterpret_cast<LeafNode*> (node);
}


FPtree::FPtree() 
{
    root = nullptr;
    #ifdef PMEM
        const char *path = "./test_pool";

        if (file_pool_exists(path) == 0) 
        {
            if ((pop = pmemobj_create(path, POBJ_LAYOUT_NAME(FPtree), PMEMOBJ_POOL_SIZE, 0666)) == NULL) 
            {
                perror("failed to create pool\n");
            }
        } 
        else 
        {
            if ((pop = pmemobj_open(path, POBJ_LAYOUT_NAME(FPtree))) == NULL)
            {
                perror("failed to open pool\n");
            }
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
    size_t len = sizeof(uint64_t);
    uint8_t oneByteHashKey = std::_Hash_bytes(&key, len, 1) & 0xff;
    return oneByteHashKey;
}


#ifdef PMEM
    inline uint64_t findFirstZero(TOID(struct LeafNode) *dst)
    {
        std::bitset<MAX_LEAF_SIZE> b = D_RW(*dst)->bitmap;
        return b.flip()._Find_first();
    }

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
                if (node->bitmap.test(i) == 1)
                    std::cout << prefix << node->kv_pairs[i].key << "," << node->kv_pairs[i].value << std::endl;
        }
    }
}


inline uint64_t InnerNode::findChildIndex(uint64_t key)
{
    auto begin = std::begin(this->keys);
    auto lower = std::lower_bound(begin, begin + this->nKey, key);
    uint64_t idx = lower - begin;
    if (idx < this->nKey && *lower == key) {
        INDEX_NODE = this;
        return idx + 1;
    }
    return idx;
}


inline LeafNode* FPtree::findLeaf(uint64_t key) 
{
	if (!root->isInnerNode) 
        return reinterpret_cast<LeafNode*> (root);
    uint64_t child_idx;
    InnerNode* cursor = reinterpret_cast<InnerNode*> (root);
    while (cursor->isInnerNode) 
    {
        child_idx = cursor->findChildIndex(key);
        cursor = reinterpret_cast<InnerNode*> (cursor->p_children[child_idx]);
    }
    return reinterpret_cast<LeafNode*> (cursor);
}

inline LeafNode* FPtree::findLeafAndPushInnerNodes(uint64_t key)
{
    stack_innerNodes.clear();
    INDEX_NODE = nullptr;
    uint64_t child_idx = MAX_INNER_SIZE + 1;
    if (!root->isInnerNode) {
    	stack_innerNodes.push(nullptr);
        CHILD_IDX = child_idx;
        return reinterpret_cast<LeafNode*> (root);
    }
    InnerNode* cursor = reinterpret_cast<InnerNode*> (root);
    while (cursor->isInnerNode)
    {
        stack_innerNodes.push(cursor);
        child_idx = cursor->findChildIndex(key);
        cursor = reinterpret_cast<InnerNode*> (cursor->p_children[child_idx]);
    }
    CHILD_IDX = child_idx;
    return reinterpret_cast<LeafNode*> (cursor);
}


static uint64_t abort_counter = 0;
static uint64_t conflict_counter = 0;
static uint64_t capacity_counter = 0;
static uint64_t debug_counter = 0;
static uint64_t failed_counter = 0;
static uint64_t explicit_counter = 0;
static uint64_t nester_counter = 0;
static uint64_t zero_counter = 0;
static uint64_t total_abort_counter = 0;
static uint64_t speculative_lock_counter = 0;
static uint64_t insert_abort_counter = 0;
static uint64_t insert_counter = 0;

void FPtree::printTSXInfo() 
{
    std::cout << "Abort:" << abort_counter << std::endl;
    std::cout << "conflict_counter: " << conflict_counter << std::endl;
    std::cout << "capacity_counter: " << capacity_counter << std::endl;
    std::cout << "debug_counter: " << debug_counter << std::endl;
    std::cout << "failed_counter: " << failed_counter << std::endl;
    std::cout << "explicit_counter :" << explicit_counter << std::endl;
    std::cout << "nester_counter: " << nester_counter << std::endl;
    std::cout << "zero_counter: " << zero_counter << std::endl;
    std::cout << "total_abort_counter: " << total_abort_counter << std::endl;
    std::cout << "speculative_lock_counter: "<< speculative_lock_counter << std::endl;
    std::cout << "insert_abort_counter:" << insert_abort_counter << std::endl;
    std::cout << "insert_counter:" << insert_counter << std::endl;
}


template <typename T>
static inline T volatile_read(T volatile &x) {
  return *&x;
}

uint64_t FPtree::find(uint64_t key)
{
    LeafNode* pLeafNode;
    volatile uint64_t idx;
    volatile int retriesLeft = 5;
    volatile unsigned status;
    if (root)
    {
        while (true)
        {
            if ((status = _xbegin ()) == _XBEGIN_STARTED)
            {
                pLeafNode = findLeaf(key);
                if (pLeafNode->lock) { _xabort(1); continue; }
                idx = pLeafNode->findKVIndex(key);
                _xend();
                return (idx != MAX_LEAF_SIZE ? pLeafNode->kv_pairs[idx].value : 0 );
            }
            else 
            {
                retriesLeft--;
                if (retriesLeft < 0) 
                {
                    total_abort_counter++;
                    tbb::speculative_spin_rw_mutex::scoped_lock lock_find;
                    lock_find.acquire(speculative_lock, false);
                    pLeafNode = findLeaf(key);
                    if (pLeafNode->lock) { lock_find.release(); continue; }
                    idx = pLeafNode->findKVIndex(key);
                    lock_find.release();
                    return (idx != MAX_LEAF_SIZE ? pLeafNode->kv_pairs[idx].value : 0 );
                }
            }
        }
    }
    return 0;
}


void FPtree::splitLeafAndUpdateInnerParents(LeafNode* reachedLeafNode, InnerNode* parentNode, 
                                            Result decision, struct KV kv, bool updateFunc = false, uint64_t prevPos = MAX_LEAF_SIZE)
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

    if constexpr (MAX_LEAF_SIZE == 1) 
    {
        #ifdef PMEM
            D_RW(insertNode)->bitmap.set(0, 0);
        #else
            insertNode->bitmap.set(0, 0);
        #endif
        splitKey = std::max(kv.key, splitKey);
    }

    #ifdef PMEM
        uint64_t slot = findFirstZero(&insertNode);
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
            std::bitset<MAX_LEAF_SIZE> tmpBitmap = D_RW(insertNode)->bitmap;
            tmpBitmap.set(prevPos, 0); tmpBitmap.set(slot);
            D_RW(insertNode)->bitmap = tmpBitmap;
        }
        pmemobj_persist(pop, &D_RO(insertNode)->bitmap, sizeof(D_RO(insertNode)->bitmap));
    #else
        insertNode->addKV(kv); 
        if (prevPos != MAX_LEAF_SIZE)
            insertNode->removeKVByIdx(prevPos);
    #endif


    if (decision == Result::Split)
    {   
        tbb::speculative_spin_rw_mutex::scoped_lock lock_split;
        lock_split.acquire(speculative_lock);
        
        LeafNode* newLeafNode;
        #ifdef PMEM
            newLeafNode = (struct LeafNode *) pmemobj_direct((reachedLeafNode->p_next).oid);
        #else
            newLeafNode = reachedLeafNode->p_next;
        #endif

        if (root->isInnerNode == false)
        {
            root = new InnerNode();
            reinterpret_cast<InnerNode*> (root)->nKey = 1;
            reinterpret_cast<InnerNode*> (root)->keys[0] = splitKey;
            reinterpret_cast<InnerNode*> (root)->p_children[0] = reachedLeafNode;
            reinterpret_cast<InnerNode*> (root)->p_children[1] = newLeafNode;
            lock_split.release();
            return;
        }
        if constexpr (MAX_INNER_SIZE != 1) 
        {
            updateParents(splitKey, parentNode, newLeafNode);
        }
        else // when inner node size equal to 1 
        {
            InnerNode* newInnerNode = new InnerNode();
            newInnerNode->nKey = 1;
            newInnerNode->keys[0] = splitKey;
            newInnerNode->p_children[0] = reachedLeafNode;
            newInnerNode->p_children[1] = newLeafNode;
            if (parentNode->keys[0] > splitKey)
                parentNode->p_children[0] = newInnerNode;
            else
                parentNode->p_children[1] = newInnerNode;
        }
        lock_split.release();
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

            if (insert_pos < mid) { // insert into parent node
                new_splitKey = parent->keys[mid];
                parent->nKey = mid;
                std::memmove(newInnerNode->keys, parent->keys + mid + 1, (MAX_INNER_SIZE - mid - 1)*sizeof(uint64_t));
                std::memmove(newInnerNode->p_children, parent->p_children + mid + 1, (MAX_INNER_SIZE - mid)*sizeof(BaseNode*));
                newInnerNode->nKey = MAX_INNER_SIZE - mid - 1;
                parent->addKey(insert_pos, splitKey, child);
            }
            else if (insert_pos > mid) { // insert into new innernode
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
                root = new InnerNode();
                reinterpret_cast<InnerNode*> (root)->addKey(0, splitKey, parent, false);
                reinterpret_cast<InnerNode*> (root)->p_children[1] = newInnerNode;
                return;
            }
            parent = stack_innerNodes.pop();
            child = newInnerNode;
        }
    }
}


bool FPtree::update(struct KV kv)
{
    // LeafNode* reachedLeafNode = findLeafAndPushInnerNodes(kv.key);
    // InnerNode* parentNode = stack_innerNodes.pop();

    // uint64_t prevPos = reachedLeafNode->findKVIndex(kv.key);
    // if (prevPos == MAX_LEAF_SIZE) {  // key not found
    //  stack_innerNodes.clear();
    //  return false;
    // }

    // bool decision = reachedLeafNode->isFull();

    // splitLeafAndUpdateInnerParents(reachedLeafNode, parentNode, decision, kv, true, prevPos);
    // stack_innerNodes.clear();
    // return true;
}


bool FPtree::insert(struct KV kv) 
{
    tbb::speculative_spin_rw_mutex::scoped_lock lock_insert;
    if (!root) // if tree is empty
    {
        lock_insert.acquire(speculative_lock);
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
            reinterpret_cast<LeafNode*>(root)->_lock();
            reinterpret_cast<LeafNode*> (root)->addKV(kv);
            reinterpret_cast<LeafNode*>(root)->_unlock();
        #endif
        lock_insert.release();
        return true;
    }

    LeafNode* reachedLeafNode;
    volatile int retriesLeft = 5;
    volatile unsigned status;
    volatile uint64_t idx;
    volatile Result decision = Result::Abort;
    while (decision == Result::Abort)
    {
        if ((status = _xbegin ()) == _XBEGIN_STARTED)
        {   
            reachedLeafNode = findLeafAndPushInnerNodes(kv.key);
            if (reachedLeafNode->lock) { _xabort(1); continue; }
            reachedLeafNode->_lock();
            idx = reachedLeafNode->findKVIndex(kv.key);
            if (idx != MAX_LEAF_SIZE)
            {
                reachedLeafNode->_unlock();
                _xend();
                return false;
            }
            decision = reachedLeafNode->isFull() ? Result::Split : Result::Insert;
            _xend();
        }
        else
        {
            std::this_thread::sleep_for(std::chrono::nanoseconds(1));
            insert_abort_counter++;
            retriesLeft--;
            if (retriesLeft < 0) 
            {
                lock_insert.acquire(speculative_lock);
                reachedLeafNode = findLeafAndPushInnerNodes(kv.key);
                if (reachedLeafNode->lock) { lock_insert.release(); continue; }
                reachedLeafNode->_lock();
                idx = reachedLeafNode->findKVIndex(kv.key);
                if (idx != MAX_LEAF_SIZE)
                {
                    reachedLeafNode->_unlock();
                    lock_insert.release();
                    return false;
                }
                decision = reachedLeafNode->isFull() ? Result::Split : Result::Insert;
                lock_insert.release();
            }
        }
    }
    splitLeafAndUpdateInnerParents(reachedLeafNode, stack_innerNodes.pop(), decision, kv);

    #ifdef PMEM
        if (decision == Result::Split) D_RW(reachedLeafNode->p_next)->_unlock();
    #else 
        if (decision == Result::Split) reachedLeafNode->p_next->_unlock();
    #endif

    reachedLeafNode->_unlock();
    
    return true;
}



uint64_t FPtree::splitLeaf(LeafNode* leaf)
{
    uint64_t splitKey = findSplitKey(leaf);
    #ifdef PMEM
        TOID(struct LeafNode) *dst = &(leaf->p_next);
        TOID(struct LeafNode) nextLeafNode = leaf->p_next;

        // Copy the content of Leaf into NewLeaf
        struct argLeafNode args(leaf);

        POBJ_ALLOC(pop, dst, struct LeafNode, args.size, constructLeafNode, &args);

        for (size_t i = 0; i < MAX_LEAF_SIZE; i++)
        {
            if (D_RO(*dst)->kv_pairs[i].key < splitKey)
                D_RW(*dst)->bitmap.set(i, 0);
        }
        pmemobj_persist(pop, &D_RO(*dst)->bitmap, sizeof(D_RO(*dst)->bitmap));

        leaf->bitmap = D_RO(*dst)->bitmap;
        if constexpr (MAX_LEAF_SIZE != 1)  leaf->bitmap.flip();

        D_RW(*dst)->p_next = nextLeafNode;
        pmemobj_persist(pop, &D_RO(*dst)->p_next, sizeof(D_RO(*dst)->p_next));

        pmemobj_persist(pop, &leaf->bitmap, sizeof(leaf->bitmap));
        // pmemobj_persist(pop, &leaf->p_next, sizeof(leaf->p_next));
    #else
        LeafNode* newLeafNode = new LeafNode(*leaf);

        for (size_t i = 0; i < MAX_LEAF_SIZE; i++)
        {
            if (newLeafNode->kv_pairs[i].key < splitKey)
                newLeafNode->bitmap.set(i, 0);
        }

        leaf->bitmap = newLeafNode->bitmap;
        if constexpr (MAX_LEAF_SIZE != 1)  leaf->bitmap.flip();
        leaf->p_next = newLeafNode;
    #endif
 
    return splitKey;
}


uint64_t FPtree::findSplitKey(LeafNode* leaf)
{
    KV tempArr[MAX_LEAF_SIZE];
    memcpy(tempArr, leaf->kv_pairs, sizeof(leaf->kv_pairs));

    std::sort(std::begin(tempArr), std::end(tempArr), [] (const KV& kv1, const KV& kv2){
            return kv1.key < kv2.key;
        });

    uint64_t mid = floor(MAX_LEAF_SIZE / 2);
    uint64_t splitKey = tempArr[mid].key;

    return splitKey;
}

void FPtree::removeKeyAndMergeInnerNodes(InnerNode* indexNode, InnerNode* parent, uint64_t child_idx, uint64_t key)
{
    InnerNode* temp, *left, *right;
    uint64_t left_idx;
    bool update_indexnode = true;

    child_idx == 0? parent->removeKey(child_idx, false) : parent->removeKey(child_idx - 1, true);

    while (!parent->nKey) // parent has no key, merge with sibling
    {
        if (parent == root) // entire tree stores 1 kv, convert the only leafnode into root
        {
            temp = reinterpret_cast<InnerNode*> (root);
            root = parent->p_children[0];
            delete temp;
            break;         
        }

        parent = stack_innerNodes.pop();
        child_idx = parent->findChildIndex(key);
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
                if (left == indexNode)
                    update_indexnode = false;
                delete left;
                parent->removeKey(left_idx, false);
            }
            else
            {
                left->addKey(left->nKey, parent->keys[left_idx], right->p_children[0]);
                if (right == indexNode)
                    update_indexnode = false;
                delete right;
                parent->removeKey(left_idx);
            }
        }
        else
            break;
    }
    if (indexNode && update_indexnode)
        indexNode->updateKey(key);
}

bool FPtree::deleteKey(uint64_t key)
{
    LeafNode* leaf, *sibling = nullptr;
    volatile uint64_t kv_idx;
    volatile int retriesLeft = 5;
    volatile int status;
    InnerNode* indexNode, *parent; 
    volatile uint64_t child_idx;
    tbb::speculative_spin_rw_mutex::scoped_lock lock_delete;
    while (true) 
    {
        if ((status = _xbegin()) == _XBEGIN_STARTED)
        {
            leaf = findLeafAndPushInnerNodes(key);
            if (leaf->lock) { _xabort(1); continue; }
            leaf->_lock();
            kv_idx = leaf->findKVIndex(key);
            if (kv_idx == MAX_LEAF_SIZE) { _xend(); leaf->_unlock(); return false; }
            indexNode = INDEX_NODE;
            parent = stack_innerNodes.pop();
            child_idx = CHILD_IDX;
            // if leaf contains more than 1 kv
            if (leaf->countKV() > 1)
            {
                leaf->removeKVByIdx(kv_idx);
                if (indexNode) // key appears in an inner node
                    indexNode->updateKey(key);
                _xend();
                #ifdef PMEM
                    TOID(struct LeafNode) lf = pmemobj_oid(leaf);
                    pmemobj_persist(pop, &D_RO(lf)->bitmap, sizeof(D_RO(lf)->bitmap));
                #endif
                leaf->_unlock();
                return true;
            }
            else if (parent) // may need to transfer/merge inner node
            {
                // find left sibling and try lock it
                if (key > 1 && (sibling = findLeaf(key-1)) == leaf) // TODO: check using stack in future
                    sibling = nullptr;
                if (sibling)
                {
                    if (sibling->lock) { _xabort(1); sibling = nullptr; leaf->_unlock(); continue; }
                    sibling->_lock();
                }
                removeKeyAndMergeInnerNodes(indexNode, parent, child_idx, key);
            }
            _xend();
            break;
        }
        else 
        {
            // std::this_thread::sleep_for(std::chrono::nanoseconds(1));
            retriesLeft--;
            if (!retriesLeft)
            {
                speculative_lock_counter++;
                lock_delete.acquire(speculative_lock, true);
                leaf = findLeafAndPushInnerNodes(key);
                if (leaf->lock) { lock_delete.release(); continue; }
                leaf->_lock();
                kv_idx = leaf->findKVIndex(key);
                if (kv_idx == MAX_LEAF_SIZE) { lock_delete.release(); leaf->_unlock(); return false; }
                indexNode = INDEX_NODE;
                parent = stack_innerNodes.pop();
                child_idx = CHILD_IDX;
                // if leaf contains more than 1 kv
                if (leaf->countKV() > 1)
                {
                    leaf->removeKVByIdx(kv_idx);
                    if (indexNode) // key appears in an inner node
                        indexNode->updateKey(key);
                    lock_delete.release();
                    #ifdef PMEM
                        TOID(struct LeafNode) lf = pmemobj_oid(leaf);
                        pmemobj_persist(pop, &D_RO(lf)->bitmap, sizeof(D_RO(lf)->bitmap));
                    #endif
                    leaf->_unlock();
                    return true;
                }
                else if (parent) // may need to transfer/merge inner node
                {
                    // find left sibling and try lock it
                    if (key > 1 && (sibling = findLeaf(key-1)) == leaf) // TODO: check using stack in future
                        sibling = nullptr;
                    if (sibling)
                    {
                        if (sibling->lock) { lock_delete.release(); sibling = nullptr; leaf->_unlock(); continue; }
                        sibling->_lock();
                    }
                    removeKeyAndMergeInnerNodes(indexNode, parent, child_idx, key);
                }
                lock_delete.release();
                break;
            }
        }
    }

    #ifdef PMEM
        TOID(struct LeafNode) lf = pmemobj_oid(leaf);
        if (sibling) // set and persist sibling's p_next, then unlock sibling node
        {
            TOID(struct LeafNode) sib = pmemobj_oid(sibling);
            D_RW(sib)->p_next = D_RO(lf)->p_next;
            pmemobj_persist(pop, &D_RO(sib)->p_next, sizeof(D_RO(sib)->p_next));
            sibling->_unlock();
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
    #else
        if (sibling)
        {
            sibling->p_next = leaf->p_next;
            sibling->_unlock();
        }
        else if (!parent)
            root = nullptr;
        delete leaf;
    #endif
    return true;
    // if constexpr (MAX_INNER_SIZE == 1)
    // {
    //     bool erase_index = false;
    //     value = leaf->removeKVByIdx(idx);
    //     if (indexNode != nullptr && indexNode != parent)
    //         erase_index = true;
    //     if (leaf->countKV() == 0)
    //     {
    //         if (parent == root)
    //             root = parent->p_children[(child_idx + 1) % 2];
    //         else
    //         {
    //             InnerNode* p = stack_innerNodes.pop();
    //             p->p_children[p->findChildIndex(key)] = parent->p_children[(child_idx + 1) % 2];
    //             if (erase_index)
    //             {
    //                 #ifdef PMEM
    //                     LeafNode* max_leaf  = maxLeaf(indexNode->p_children[0]);
    //                     max_leaf->p_next = leaf->p_next;
    //                     pmemobj_persist(pop, &max_leaf->p_next, sizeof(max_leaf)->p_next);
    //                 #else
    //                     maxLeaf(indexNode->p_children[0])->p_next = leaf->p_next;
    //                 #endif
    //             }
    //         }
    //         if (child_idx == 1) // deleting right child
    //         {
    //             #ifdef PMEM
    //                 LeafNode* max_leaf  = maxLeaf(parent->p_children[0]);
    //                 max_leaf->p_next = leaf->p_next;
    //                 pmemobj_persist(pop, &max_leaf->p_next, sizeof(max_leaf)->p_next);
    //             #else
    //                 maxLeaf(parent->p_children[0])->p_next = leaf->p_next;
    //             #endif
    //         }
    //         parent->nKey = 0;
    //         delete parent;

    //         #ifdef PMEM
    //             TOID(struct LeafNode) pmem_leaf = pmemobj_oid(leaf);
    //             POBJ_FREE(&pmem_leaf);
    //         #else
    //             delete leaf; 
    //         #endif
    //     }
    //     if (erase_index)
    //         indexNode->keys[0] = minKey(indexNode->p_children[1]);
    //     stack_innerNodes.clear();
    //     return true;
    // }

}

bool FPtree::tryBorrowKey(InnerNode* parent, uint64_t receiver_idx, uint64_t sender_idx)
{
    InnerNode* sender = reinterpret_cast<InnerNode*> (parent->p_children[sender_idx]);
    if (sender->nKey <= 1)      // sibling has only 1 key, cannot borrow
        return false;
    InnerNode* receiver = reinterpret_cast<InnerNode*> (parent->p_children[receiver_idx]);
    if (receiver_idx < sender_idx) // borrow from right sibling
    {
        receiver->addKey(0, parent->keys[receiver_idx], sender->p_children[0]);
        parent->keys[receiver_idx] = sender->keys[0];
        sender->removeKey(0, false);
    }
    else // borrow from left sibling
    {
        receiver->addKey(0, receiver->keys[0], sender->p_children[sender->nKey], false);
        parent->keys[sender_idx] = sender->keys[sender->nKey-1];
        sender->removeKey(sender->nKey-1);
    }
    return true;
}


uint64_t FPtree::minKey(BaseNode* node)
{
    while (node->isInnerNode)
        node = reinterpret_cast<InnerNode*> (node)->p_children[0];
    return reinterpret_cast<LeafNode*> (node)->minKV().key;
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


void FPtree::ScanInitialize(uint64_t key)
{
    if (!root)
        return;

    // std::cout << "scan: " << key << std::endl;

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


KV FPtree::ScanNext()
{
    // assert(this->current_leaf != nullptr && "Current scan node was deleted!");
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


bool FPtree::ScanComplete()
{
    return this->current_leaf == nullptr;
}



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



// #if TEST_MODE == 1
//     int main(int argc, char *argv[]) 
//     {
//         FPtree fptree;

//         #ifdef PMEM
//             const char* command = argv[1];
//             if (command != NULL && strcmp(command, "show") == 0)
//             {  
//                 showList();
//                 return 0;
//             }
//         #endif

//         int64_t key;
//         uint64_t value;
//         while (true)
//         {
//             std::cout << "\nEnter the key to insert, delete or update (-1): "; 
//             std::cin >> key;
//             std::cout << std::endl;
//             KV kv = KV(key, key);
//             if (key == 0)
//                 break;
//             else if (key == -1)
//             {
//                 std::cout << "\nEnter the key to update: ";
//                 std::cin >> key;
//                 std::cout << "\nEnter the value to update: ";
//                 std::cin >> value;
//                 fptree.update(KV(key, value));
//             }
//             // else if (fptree.find(kv.key))
//             //     fptree.deleteKey(kv.key);
//             else
//             {
//                 fptree.insert(kv);
//             }
//             fptree.printFPTree("├──", fptree.getRoot());
//             #ifdef PMEM
//                 std::cout << std::endl;
//                 std::cout << "show list: " << std::endl;
//                 showList();
//             #endif
//         }


//         std::cout << "\nEnter the key to initialize scan: "; 
//         std::cin >> key;
//         std::cout << std::endl;
//         fptree.ScanInitialize(key);
//         while(!fptree.ScanComplete())
//         {
//             KV kv = fptree.ScanNext();
//             std::cout << kv.key << "," << kv.value << " ";
//         }
//         std::cout << std::endl;
//     }
// #elif INSPECT_MODE == 0
//     int main(int argc, char *argv[]) 
//     {   
//         uint64_t NUM_OPS = 500000;
//         double elapsed;
//         bool unmatch_result = true;
//         uint64_t unmatch_counter = 0;

//         /* Key value generator */
//         std::independent_bits_engine<std::default_random_engine, 64, uint64_t> rbe;
//         std::vector<uint64_t> keys(NUM_OPS);
//         std::generate(begin(keys), end(keys), std::ref(rbe));
        
//         std::vector<uint64_t> values(NUM_OPS);
//         // uint64_t check_values[NUM_OPS] = {0};
//         std::generate(begin(values), end(values), std::ref(rbe));

//         /* Loading phase */
//         FPtree fptree;
//         for (uint64_t i = 0; i < NUM_OPS; i++)
//             fptree.insert(KV(keys[i], values[i]));

//         /* Testing phase */
//         // std::generate(begin(keys), end(keys), std::ref(rbe));
//         // std::generate(begin(values), end(values), std::ref(rbe));

//         std::cout << "Start testing phase...." << std::endl;

//         auto t1 = std::chrono::high_resolution_clock::now();
//         for (uint64_t i = 0; i < NUM_OPS; i++) 
//             fptree.find(keys[i]);
//         auto t2 = std::chrono::high_resolution_clock::now();

//         std::cout << "Start checking phase...." << std::endl;


//         // for (uint64_t i = 0; i < NUM_OPS; i++) {
//         //     if (values[i] != check_values[i]) {
//         //         std::cout << i << " " << check_values[i] << "  " << values[i] << std::endl;
//         //         unmatch_counter++;
//         //     }
//         // }

//         // std::cout << "unmatch_counter: " << unmatch_counter << std::endl;
//         // std::cout << "Checking result: " << unmatch_result << std::endl;

//         fptree.printTSXInfo();

//         // uint64_t tick = rdtsc();
//         // for (uint64_t i = 0; i < NUM_OPS; i++)
//         //     fptree.insert(KV(keys[i], values[i]));
//         // uint64_t cycles = rdtsc() - tick;

//         /* Getting number of milliseconds as a double */
//         std::chrono::duration<double, std::milli> ms_double = t2 - t1;

//         // /* Get stats */
//         elapsed = ms_double.count();
//         std::cout << "\tRun time: " << elapsed << " milliseconds" << std::endl;
//         std::cout << "\tThroughput: " << std::fixed << NUM_OPS / ( (float)elapsed / 1000 )
//                 << " ops/s" << std::endl;
//         // std::cout << "\tCPU cycles per operation: " << cycles / NUM_OPS << " cycles/op" << std::endl;
//     }

// #endif


// volatile unsigned status;
// if ((status = _xbegin ()) == _XBEGIN_STARTED)
// {
//     updateParents(splitKey, parentNode, newLeafNode);
//     _xend();
// }
// else
// {
//     insert_abort_counter++;
//     tbb::speculative_spin_rw_mutex::scoped_lock lock_split;
//     lock_split.acquire(speculative_lock, false);
//     updateParents(splitKey, parentNode, newLeafNode);
//     lock_split.release();
// }