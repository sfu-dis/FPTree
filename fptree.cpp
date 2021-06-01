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
    memcpy(&this->keys, &inner.keys, sizeof(inner.keys));
    memcpy(&this->p_children, &inner.p_children, sizeof(inner.p_children));
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
        this->lock = ATOMIC_VAR_INIT(false);
    }

    LeafNode::LeafNode(const LeafNode& leaf)
    {
        this->isInnerNode = false;
        this->bitmap = leaf.bitmap;
        memcpy(&this->fingerprints, &leaf.fingerprints, sizeof(leaf.fingerprints));
        memcpy(&this->kv_pairs, &leaf.kv_pairs, sizeof(leaf.kv_pairs));
        this->p_next = leaf.p_next;
        this->lock.store(leaf.lock, std::memory_order_relaxed);
    }

    LeafNode& LeafNode::operator=(const LeafNode& leaf)
    {
        this->isInnerNode = false;
        this->bitmap = leaf.bitmap;
        memcpy(&this->fingerprints, &leaf.fingerprints, sizeof(leaf.fingerprints));
        memcpy(&this->kv_pairs, &leaf.kv_pairs, sizeof(leaf.kv_pairs));
        this->p_next = leaf.p_next;
        this->lock.store(leaf.lock, std::memory_order_relaxed);
        return *this;
    }
#endif

void InnerNode::removeKey(uint64_t index, bool remove_right_child = true)
{
    // assert(this->nKey > index && "Remove key index out of range!");
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
    // assert(this->nKey >= index && "Insert key index out of range!");
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

inline uint64_t LeafNode::findFirstZero()
{
    std::bitset<MAX_LEAF_SIZE> b = bitmap;
    return b.flip()._Find_first();
}

inline void LeafNode::addKV(struct KV kv)
{
    uint64_t idx = this->findFirstZero();
    this->fingerprints[idx] = getOneByteHash(kv.key);
    this->kv_pairs[idx] = kv;
    this->bitmap.set(idx);
}

inline uint64_t LeafNode::removeKV(uint64_t key)
{
    uint64_t idx = findKVIndex(key);
    // assert(idx != MAX_LEAF_SIZE);
    return this->removeKVByIdx(idx);
}

inline uint64_t LeafNode::removeKVByIdx(uint64_t pos)
{
    // assert(this->bitmap.test(pos) == true);
    this->bitmap.set(pos, 0);
    return this->kv_pairs[pos].value;
}

uint64_t LeafNode::findKVIndex(uint64_t key)
{
    size_t key_hash = getOneByteHash(key);
    __m512i key_64B = _mm512_set1_epi8((char)key_hash);

       // b. load meta into another 16B register
    __m512i fgpt_64B= _mm512_load_si512((__m512i*)this->fingerprints);

       // c. compare them
    uint64_t mask = uint64_t(_mm512_cmpeq_epi8_mask(key_64B, fgpt_64B));

    mask &= offset;

    size_t counter = 0;
    while (mask != 0) {
        if (mask & 1 && this->bitmap[counter] && key == this->keys[counter])
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
        if (this->bitmap[i] && this->kv_pairs[i].key <= min_key)
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
        if (this->bitmap[i] && this->kv_pairs[i].key >= max_key)
        {
            max_key = this->kv_pairs[i].key;
            max_key_idx = i;
        }
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
    #ifdef PMEM
        root = NULL;

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
        root = nullptr;
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
                if (D_RO(leafNode)->bitmap[i])
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
        memcpy(&(node->fingerprints), &(a->fingerprints), sizeof(a->fingerprints));
        memcpy(&(node->kv_pairs), &(a->kv_pairs), sizeof(a->kv_pairs));
        node->p_next = TOID_NULL(struct LeafNode);

        pmemobj_persist(pop, node, a->size);

        return 0;
    }
#endif  



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
        	if (node->bitmap[i] == 1)
        	{
        		std::cout << prefix << node->kv_pairs[i].key << "," << node->kv_pairs[i].value << std::endl;
        	}
        }
	}
}


inline std::pair<uint64_t, bool> InnerNode::findChildIndex(uint64_t key)
{
    auto lower = std::lower_bound(std::begin(this->keys), std::begin(this->keys) + this->nKey, key);
    uint64_t idx = lower - std::begin(this->keys);
    if (idx < this->nKey && *lower == key)  
        return std::make_pair(idx + 1, true);
    return std::make_pair(idx, false);
}


inline LeafNode* FPtree::findLeaf(uint64_t key) 
{
    return findLeafWithParent(key).second;
}

std::pair<InnerNode*, LeafNode*> FPtree::findLeafWithParent(uint64_t key)
{
    if (!root->isInnerNode)
    {
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
    // assert(false && "Function called with child == root!");
    return std::make_pair(nullptr, 0);
}


uint64_t FPtree::find(uint64_t key)
{
    if (root != nullptr)
    {
        LeafNode* pLeafNode = findLeaf(key);
        uint64_t idx = pLeafNode->findKVIndex(key);
        if (idx != MAX_LEAF_SIZE)
            return pLeafNode->kv_pairs[idx].value;
    }
    return 0;
}


void FPtree::splitLeafAndUpdateInnerParents(LeafNode* reachedLeafNode, InnerNode* parentNode, 
                                            bool decision, struct KV kv, bool updateFunc = false, uint64_t prevPos = MAX_LEAF_SIZE)
{
    uint64_t splitKey;

    #ifdef PMEM
        TOID(struct LeafNode) insertNode = pmemobj_oid(reachedLeafNode);
    #else
        LeafNode* insertNode = reachedLeafNode;
    #endif

    if (decision == true)
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


    if (decision == true)
    {   
        #ifdef PMEM
            LeafNode* newLeafNode = (struct LeafNode *) pmemobj_direct((reachedLeafNode->p_next).oid);
        #else
            LeafNode* newLeafNode = reachedLeafNode->p_next;
        #endif

        if (root->isInnerNode == false)
        {
            root = new InnerNode();
            reinterpret_cast<InnerNode*> (root)->nKey = 1;
            reinterpret_cast<InnerNode*> (root)->keys[0] = splitKey;
            reinterpret_cast<InnerNode*> (root)->p_children[0] = reachedLeafNode;
            reinterpret_cast<InnerNode*> (root)->p_children[1] = newLeafNode;
            return;
        }
        if constexpr (MAX_INNER_SIZE != 1) 
            updateParents(splitKey, parentNode, newLeafNode);
        else // when inner node size equal to 1 
        {
            InnerNode* newInnerNode = new InnerNode();
            newInnerNode->nKey = 1;
            newInnerNode->keys[0] = splitKey;
            newInnerNode->p_children[0] = reachedLeafNode;
            //newInnerNode->p_children[1] = (struct LeafNode *) pmemobj_direct((reachedLeafNode->p_next).oid);
            newInnerNode->p_children[1] = newLeafNode;
            if (parentNode->keys[0] > splitKey)
                parentNode->p_children[0] = newInnerNode;
            else
                parentNode->p_children[1] = newInnerNode;
        }
    }

}


void FPtree::updateParents(uint64_t splitKey, InnerNode* parent, BaseNode* child)
{
    if (parent->nKey < MAX_INNER_SIZE)
    {
        std::pair<uint64_t, bool> insert_pos = parent->findChildIndex(splitKey);
        //assert(insert_pos.second == false && "Exception: duplicate key index detected!");
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
            root = new InnerNode();
            reinterpret_cast<InnerNode*> (root)->addKey(0, splitKey, parent, false);
            reinterpret_cast<InnerNode*> (root)->p_children[1] = newInnerNode;
            return;
        }
        updateParents(splitKey, findInnerNodeParent(parent).first, newInnerNode);
    }
}


bool FPtree::update(struct KV kv)
{
    // assert(root != nullptr);

    std::pair<InnerNode*, LeafNode*> p = findLeafWithParent(kv.key);
    InnerNode* parentNode = p.first;
    LeafNode* reachedLeafNode = p.second;

    uint64_t prevPos = reachedLeafNode->findKVIndex(kv.key);
    // assert(prevPos != MAX_LEAF_SIZE && "Exception: no key is find");
    if (prevPos == MAX_LEAF_SIZE) return true;

    bool decision = reachedLeafNode->isFull();

    splitLeafAndUpdateInnerParents(reachedLeafNode, parentNode, decision, kv, true, prevPos);

    return true;
}



bool FPtree::insert(struct KV kv) 
{
    // std::cout << "\ninsert: " << kv.key << std::endl;

    #ifdef PMEM
        TOID(struct List) ListHead = POBJ_ROOT(pop, struct List);
        TOID(struct LeafNode) *dst = &D_RW(ListHead)->head;

        if (TOID_IS_NULL(*dst)) 
        {
            struct argLeafNode args;
            args.isInnerNode = false;
            args.size = sizeof(struct LeafNode);
            args.kv_pairs[0] = kv;
            args.fingerprints[0] = getOneByteHash(kv.key);
            args.bitmap.set(0);
            args.lock = ATOMIC_VAR_INIT(false);

            POBJ_ALLOC(pop, dst, struct LeafNode, args.size, constructLeafNode, &args);

            D_RW(ListHead)->head = *dst; 
            pmemobj_persist(pop, &D_RO(ListHead)->head, sizeof(D_RO(ListHead)->head));

            this->root = (struct BaseNode *) pmemobj_direct((*dst).oid);

            return true;
        }
    #else
        if (root == nullptr) 
        {
            root = new LeafNode();
            reinterpret_cast<LeafNode*> (root)->addKV(kv);
            return true;
        }
    #endif

    std::pair<InnerNode*, LeafNode*> p = findLeafWithParent(kv.key);
    InnerNode* parentNode = p.first;
    LeafNode* reachedLeafNode = p.second;

    // return false if key already exists
    uint64_t idx = reachedLeafNode->findKVIndex(kv.key);
    if (idx != MAX_LEAF_SIZE)
        return false;

    bool decision = reachedLeafNode->isFull();

    splitLeafAndUpdateInnerParents(reachedLeafNode, parentNode, decision, kv);

    return true;
}



uint64_t FPtree::splitLeaf(LeafNode* leaf)
{
    #ifdef PMEM
        TOID(struct LeafNode) *dst = &(leaf->p_next);
        TOID(struct LeafNode) nextLeafNode = leaf->p_next;

        // Copy the content of Leaf into NewLeaf
        struct argLeafNode args;
        args.isInnerNode = false;
        args.size = sizeof(struct LeafNode);
        memcpy(&(args.fingerprints), &(leaf->fingerprints), sizeof(leaf->fingerprints));
        memcpy(&(args.kv_pairs), &(leaf->kv_pairs), sizeof(leaf->kv_pairs));
        args.bitmap = leaf->bitmap;
        args.lock.store(leaf->lock, std::memory_order_relaxed);

        POBJ_ALLOC(pop, dst, struct LeafNode, args.size, constructLeafNode, &args);

        uint64_t splitKey = findSplitKey(leaf);

        for (size_t i = 0; i < MAX_LEAF_SIZE; i++)
        {
            if (D_RO(*dst)->kv_pairs[i].key < splitKey)
                D_RW(*dst)->bitmap.set(i, 0);
        }
        pmemobj_persist(pop, &D_RO(*dst)->bitmap, sizeof(D_RO(*dst)->bitmap));

        leaf->bitmap = D_RO(*dst)->bitmap;
        if (MAX_LEAF_SIZE != 1)  leaf->bitmap.flip();
        pmemobj_persist(pop, &leaf->bitmap, sizeof(leaf->bitmap));

        D_RW(*dst)->p_next = nextLeafNode;
        pmemobj_persist(pop, &D_RO(*dst)->p_next, sizeof(D_RO(*dst)->p_next));
        // pmemobj_persist(pop, &leaf->p_next, sizeof(leaf->p_next));
    #else
        LeafNode* newLeafNode = new LeafNode(*leaf);
        uint64_t splitKey = findSplitKey(leaf);

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
    memcpy(&tempArr, &leaf->kv_pairs, sizeof(leaf->kv_pairs));

    std::sort(std::begin(tempArr), std::end(tempArr), [] (const KV& kv1, const KV& kv2){
            return kv1.key < kv2.key;
        });

    uint64_t mid = floor(MAX_LEAF_SIZE / 2);
    uint64_t splitKey = tempArr[mid].key;

    return splitKey;
}



bool FPtree::deleteKey(uint64_t key)
{
    // std::cout << "delete: " << key << std::endl;

    if (!root->isInnerNode)     // tree with root only
    {
        uint64_t idx = reinterpret_cast<LeafNode*> (root)->findKVIndex(key);
        if (idx == MAX_LEAF_SIZE) return false;
        reinterpret_cast<LeafNode*> (root)->removeKVByIdx(idx);
        return true;
    }

    std::tuple<InnerNode*, InnerNode*, uint64_t> tpl = findInnerAndLeafWithParent(key);
    InnerNode* indexNode = std::get<0>(tpl);
    InnerNode* parent = std::get<1>(tpl);
    uint64_t child_idx = std::get<2>(tpl);
    LeafNode* leaf = reinterpret_cast<LeafNode*>(parent->p_children[child_idx]);

    uint64_t idx = leaf->findKVIndex(key);
    if (idx == MAX_LEAF_SIZE) return true;

    uint64_t value;
    if constexpr (MAX_INNER_SIZE == 1)
    {
        bool erase_index = false;
        value = leaf->removeKVByIdx(idx);
        if (indexNode != nullptr && indexNode != parent)
            erase_index = true;
        if (leaf->countKV() == 0)
        {
            if (parent == root)
                root = parent->p_children[(child_idx + 1) % 2];
            else
            {
                std::pair<InnerNode*, uint64_t> p = findInnerNodeParent(parent);
                p.first->p_children[p.second] = parent->p_children[(child_idx + 1) % 2];
                if (erase_index)
                {
                    #ifdef PMEM
                        LeafNode* max_leaf  = maxLeaf(indexNode->p_children[0]);
                        max_leaf->p_next = leaf->p_next;
                        pmemobj_persist(pop, &max_leaf->p_next, sizeof(max_leaf)->p_next);
                    #else
                        maxLeaf(indexNode->p_children[0])->p_next = leaf->p_next;
                    #endif
                }
            }
            if (child_idx == 1) // deleting right child
            {
                #ifdef PMEM
                    LeafNode* max_leaf  = maxLeaf(parent->p_children[0]);
                    max_leaf->p_next = leaf->p_next;
                    pmemobj_persist(pop, &max_leaf->p_next, sizeof(max_leaf)->p_next);
                #else
                    maxLeaf(parent->p_children[0])->p_next = leaf->p_next;
                #endif
            }
            parent->nKey = 0;
            delete parent;

            #ifdef PMEM
                TOID(struct LeafNode) pmem_leaf = pmemobj_oid(leaf);
                POBJ_FREE(&pmem_leaf);
            #else
                delete leaf; 
            #endif
        }
        if (erase_index)
            indexNode->keys[0] = minKey(indexNode->p_children[1]);
        return true;
    }


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
    return true;
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

        #ifdef PMEM
            TOID(struct LeafNode) pmem_left = pmemobj_oid(left);
            TOID(struct LeafNode) pmem_right = pmemobj_oid(right);
            D_RW(pmem_left)->p_next = D_RO(pmem_right)->p_next;
            pmemobj_persist(pop, &D_RO(pmem_left)->p_next, sizeof(D_RO(pmem_left)->p_next));

            POBJ_FREE(&pmem_right); right = nullptr;
        #else
            left->p_next = right->p_next;
            delete right; right = nullptr;
        #endif
        
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
    // assert(receiver_idx == sender_idx + 1 || receiver_idx + 1 == sender_idx && "Sender and receiver are not immediate siblings!");
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
        if (this->current_leaf->bitmap[i])
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



int main(int argc, char *argv[]) 
{
    FPtree fptree;

    // for (uint64_t i = 0; i < 10000000; i++)
    //     fptree.insert(KV(i, i));

    // for (uint64_t i = 0; i < 1000000; i++)
    //     fptree.deleteKey(i);

    // for (uint64_t i = 0; i < 1000000-1; i++) 
    // {
    //     fptree.ScanInitialize(i);
    //     fptree.ScanNext();
    // }
    // fptree.printFPTree("├──", fptree.getRoot());

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
        std::cout << "\nEnter the key to insert, delete or update (-1): "; 
        std::cin >> key;
        std::cout << std::endl;
        if (key == 0)
            break;
        else if (fptree.find(key) != 0)
            fptree.deleteKey(key);
        else if (key == -1)
        {
            std::cout << "\nEnter the key to update: ";
            std::cin >> key;
            std::cout << "\nEnter the value to update: ";
            std::cin >> value;
            fptree.update(KV(key, value));
        }
        else
        {
            fptree.insert(KV(key, key));
        }
        fptree.printFPTree("├──", fptree.getRoot());
        #ifdef PMEM
            std::cout << std::endl;
            std::cout << "show list: " << std::endl;
            showList();
        #endif
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



