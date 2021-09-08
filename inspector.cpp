#include <iostream>
#include <vector>
#include <algorithm>
#include <limits>
#include <cassert>
#include <unordered_map>
#include <thread>
#include <utility>
#include <stdlib.h>
#include <chrono>

#include "fptree.h"


#define NUM_RECORDS 10000000		// Number of records to start with

#define NUM_WORKER_THREAD 16		// Number of worker threads for insert, delete

#define NUM_INSPECTOR_THREAD 48	// Number of threads that walks tree in parallel

#define CHECK_INNER 0			// Whether verifies correctness of innernode

#define CHECK_INSERT 1			// Check tree integrity after loading NUM_RECORDS records

#define DELETE 1				// Whether delete half of keys after loading
#define CHECK_DELETE 1			// Check tree integrity after delete half

#define BULK_LOAD 0				// Create another tree using the test_pool, check integrity

static thread_local std::unordered_map<uint64_t, uint64_t> count_;

struct Queue 
{
public:

    std::thread queue_[NUM_INSPECTOR_THREAD];
    uint64_t head, tail;
    bool empty;

    Queue() : head(0), tail(0), empty(true) {}
    ~Queue() {}

    inline bool push(std::thread t) 
    {
        if (head == tail && !empty)	// if full
        	return false;
    	queue_[tail] = std::move(t);
    	tail = (tail + 1) % NUM_INSPECTOR_THREAD;
    	empty = false;
    	return true;
    }

    inline void pop() {
    	if (empty)
    		return;
    	else
    	{
    		queue_[head].join();
			head = (head + 1) % NUM_INSPECTOR_THREAD;
			if (head == tail)
				empty = true;
    	}
    }

    inline uint64_t size() 
    { 
    	if (head < tail)
    		return tail - head;
    	else if (head > tail)
    		return NUM_INSPECTOR_THREAD - head + tail;
    	else
    		return empty? 0 : NUM_INSPECTOR_THREAD;
    }

    inline bool isEmpty() { return empty; }

    inline bool full() { return head == tail && !empty; }

    inline void clear() { head = 0; tail = 0; empty = true; }
};

class Inspector {
public:
	Inspector();
    ~Inspector();

    void ClearStats();
    bool SanityCheck(FPtree& tree, std::vector<uint64_t>& keys, std::vector<uint64_t>& values);
    void KVPresenceCheck(FPtree& tree, std::vector<uint64_t>& keys, std::vector<uint64_t>& values);
    void InnerNodeOrderCheck(InnerNode* node, std::vector<uint64_t>& keys);
    void SubtreeOrderCheck(BaseNode* node, uint64_t min, uint64_t max, std::vector<uint64_t>& keys, bool stop);

	uint64_t kv_missing_count_;
	uint64_t kv_duplicate_count_;
	uint64_t inner_order_violation_count_;
	uint64_t inner_boundary_violation_count_;
	uint64_t inner_duplicate_count_;
	uint64_t inner_invalid_count_;
};

Inspector::Inspector()
{
	ClearStats();
}

Inspector::~Inspector(){}

bool Inspector::SanityCheck(FPtree& tree, std::vector<uint64_t>& keys, std::vector<uint64_t>& values) 
{
	ClearStats();

	std::cout << "\nLeafNode check\n";
	KVPresenceCheck(tree, keys, values);

	#if CHECK_INNER == 1
		std::cout << "\nInnerNode check\n";
		if (tree.root->isInnerNode){
			
			std::vector<uint64_t> vec(keys);
			std::sort(vec.begin(), vec.end());
			InnerNodeOrderCheck(reinterpret_cast<InnerNode*>(tree.root), vec);
		}
	#else
		printf("Skip innernode check.\n");
	#endif

	return !(kv_missing_count_ || kv_duplicate_count_ || inner_order_violation_count_ || 
	inner_boundary_violation_count_ || inner_duplicate_count_ || inner_invalid_count_);
}

void Inspector::KVPresenceCheck(FPtree& tree, std::vector<uint64_t>& keys, std::vector<uint64_t>& values)
{
	// missing kv check in normal read approach
	uint64_t val, prev_kv_missing_count = kv_missing_count_, prev_kv_duplicate_count_ = kv_duplicate_count_;
	for (uint64_t i = 0; i < keys.size(); i++)
	{
		val = tree.find(keys[i]);
		if (val != values[i])
		{
			std::cout << "Missing Key: " << keys[i] << " Value: " << values[i] << std::endl;
			kv_missing_count_ ++;
		}
	}
	// duplicate check in scan approach
	LeafNode* cur = tree.minLeaf(tree.root);
	std::vector<uint64_t> vec;
	vec.reserve(keys.size());
	uint64_t i;
	while(cur != nullptr)
	{
		for (i = 0; i < MAX_LEAF_SIZE; i++)
			if (cur->bitmap.test(i))
				vec.push_back(cur->kv_pairs[i].key);
		#ifdef PMEM
			cur = (struct LeafNode *) pmemobj_direct((cur->p_next).oid);
		#else
			cur = cur->p_next;
		#endif
	}
	std::sort(vec.begin(), vec.end());
	for (std::vector<uint64_t>::iterator it = vec.begin() + 1 ; it != vec.end(); ++it)
		if (*it == *(it-1))
		{
			std::cout << "Duplicate key: " << *it << std::endl;
			kv_duplicate_count_ ++;
		}
	// deduction
	uint64_t scan_size = vec.size() - (kv_duplicate_count_ - prev_kv_duplicate_count_);
	uint64_t read_size = keys.size() - (kv_missing_count_ - prev_kv_missing_count);
	std::cout << "Records checked through traversing: " << read_size << std::endl;
	std::cout << "Records checked through scanning: " << scan_size << std::endl;
	if (scan_size > read_size)
		std::cout << "Some keys can be accessed through scanning leaf list but not through normal traversal: \
	tree likely contains abandoned leafnode(s) and/or additional records\n";
	else if (scan_size < read_size)
		std::cout << "Some keys can be accessed through normal traversal but not through scanning leaf lists: \
	leaf list is likely broken\n";
	//TODO: add element check in both vectors 
}



void Inspector::InnerNodeOrderCheck(InnerNode* node, std::vector<uint64_t>& keys)
{
	Queue q;
	SubtreeOrderCheck(node, 0, std::numeric_limits<uint64_t>::max(), keys, true);
	uint64_t child_idx = 0, min, max;
	std::cout << "Root has " << node->nKey + 1 << " children\n";
	while (child_idx <= node->nKey)
	{
		min = 0;
		if (child_idx)
			min = node->keys[child_idx - 1];
		max = std::numeric_limits<uint64_t>::max();
		if (child_idx != node->nKey)
			max = node->keys[child_idx];
		if (q.full())
		 	q.pop();
		q.push(std::thread(&Inspector::SubtreeOrderCheck, this, node->p_children[child_idx++], min, max, std::ref(keys), false));
	}
	while(!q.isEmpty())
		q.pop();
}

void Inspector::SubtreeOrderCheck(BaseNode* node, uint64_t min, uint64_t max, std::vector<uint64_t>& keys, bool stop = false)
{
	uint64_t val, cur_min = std::numeric_limits<uint64_t>::max(), cur_max = 0;
	if (node->isInnerNode)
	{
		count_.clear();
		InnerNode *  inner = reinterpret_cast<InnerNode*> (node);
		assert(inner->nKey > 0 && "Reached empty innernode!\n");
		for (uint64_t i = 0; i < inner->nKey; i++)
		{
			val = inner->keys[i];
			count_[val]++;
			if (std::find(keys.begin(), keys.end(), val) == keys.end())
			{
				std::cout << "Innernode invalid key: " << val << std::endl;
				inner_invalid_count_++;
			}
			if (i && val < inner->keys[i-1])
			{
				std::cout << "Innernode order violation: " << inner->keys[i] << " " << inner->keys[i-1] << std::endl;
				inner_order_violation_count_++;
			}
		}
		for (auto &elt : count_)
		{
			if (elt.second > 1)
			{
				std::cout << "Innernode duplicate key: " << elt.first << " found " << elt.second << " times\n";
				inner_duplicate_count_++;
			}
			if (elt.first < cur_min)
				cur_min = elt.first;
			if (elt.first > cur_max)
				cur_max = elt.first;
		}
		if (cur_min < min || cur_max >= max)
		{
			std::cout << "Innernode boundary violation: " << cur_min << " " << cur_max
			<< " Min: " << min << " " << " Max: " << max << std::endl;
			inner_boundary_violation_count_++;
		}
		if (!stop)
		{
			for (uint64_t i = 0; i <= inner->nKey; i++)
			{
				if (i == 0)
					SubtreeOrderCheck(inner->p_children[i], min, inner->keys[0], keys);
				else if (i == inner->nKey)
					SubtreeOrderCheck(inner->p_children[i], inner->keys[inner->nKey-1], max, keys);
				else
					SubtreeOrderCheck(inner->p_children[i], inner->keys[i-1], inner->keys[i], keys);
			}
		}
	}
}

void Inspector::ClearStats()
{
	kv_missing_count_ = 0;
	kv_duplicate_count_ = 0;
	inner_order_violation_count_ = 0;
	inner_boundary_violation_count_ = 0;
	inner_duplicate_count_ = 0;
	inner_invalid_count_ = 0;
}

void shuffle(std::vector<uint64_t>& keys, std::vector<uint64_t>& values) {
	uint64_t i, j, times = keys.size()/2;
	for (uint64_t k = 0; k < times; k++)
	{
		i = rand() % keys.size();
		j = rand() % keys.size();
		std::iter_swap(keys.begin() + i, keys.begin() + j);
		std::iter_swap(values.begin() + i, values.begin() + j);
	}
}

void thread_load(FPtree & tree, std::vector<uint64_t> & keys, std::vector<uint64_t> & values, uint64_t id) {
	uint64_t workload = NUM_RECORDS / NUM_WORKER_THREAD, stop;
	if (id == NUM_WORKER_THREAD - 1)	// last thread, load all keys left
		stop = NUM_RECORDS;
	else	// just normal workload
		stop = (id + 1) * workload;
	for (uint64_t i = id * workload; i < stop; i++)
		if (!tree.insert(KV(keys[i], values[i])))
		{
			printf("Insert failed! Key: %llu Value: %llu\n", keys[i], values[i]);
			exit(1);
		}
}

void thread_delete(FPtree & tree, std::vector<uint64_t> & keys, uint64_t id) {
	uint64_t half = keys.size() / 2;
	uint64_t workload = (NUM_RECORDS - half) / NUM_WORKER_THREAD, stop;
	if (id == NUM_WORKER_THREAD - 1)	// last thread, delete all keys left
		stop = NUM_RECORDS;
	else	// just normal workload
		stop = (id + 1) * workload + half;
	for (uint64_t i = id * workload + half; i < stop; i++)
		if (!tree.deleteKey(keys[i]))
			printf("Delete failed! Key: %llu \n", keys[i]);
}

int main()
{
	printf("Number of Records: %llu\n", NUM_RECORDS);
	printf("Number of worker thread: %d\n", NUM_WORKER_THREAD);
	printf("Number of inspector thread: %d\n", NUM_INSPECTOR_THREAD);


	srand (0); //(time(NULL));
	std::independent_bits_engine<std::default_random_engine, 64, uint64_t> rbe;
    std::vector<uint64_t> keys(NUM_RECORDS);
    std::generate(begin(keys), end(keys), std::ref(rbe));
    std::vector<uint64_t> values(NUM_RECORDS);
    std::generate(begin(values), end(values), std::ref(rbe));
    std::cout << "Key generation complete, start loading...\n";

    FPtree fptree;
    Inspector ins;
    std::vector<std::thread> workers(NUM_WORKER_THREAD);
    
    auto start = std::chrono::steady_clock::now();
    
    for (uint64_t i = 0; i < NUM_WORKER_THREAD; i++)
    	workers[i] = std::thread(thread_load, std::ref(fptree), std::ref(keys), std::ref(values), i);
    for (uint64_t i = 0; i < NUM_WORKER_THREAD; i++)
	    workers[i].join();
    // for (uint64_t i = 0; i < NUM_RECORDS; i++)
    // 	fptree.insert(KV(keys[i], values[i]));
    std::cout << "Loading complete (" << std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - start).count() << " sec). start testing...\n";


    #if CHECK_INSERT == 1
    	printf("Starting sanity check for insert...\n");
	    if (ins.SanityCheck(fptree, keys, values))
	    	std::cout << "Sanity check for insertion passed!\n";
	    else
	    {
	    	std::cout << "Sanity check for insertion failed!\n";
	    	//fptree.printFPTree("├──", fptree.getRoot());
	    	// #ifdef PMEM
      //           showList();
      //       #endif
	    	return -1;
	    }
	#else
	    printf("Skip insertion check.\n");
	#endif


	#if DELETE == 1
	    printf("Deleting half of keys randomly.\n");
	    shuffle(keys, values);
	    start = std::chrono::steady_clock::now();
	    uint64_t half = keys.size() / 2;
	    workers.clear();
	    for (uint64_t i = 0; i < NUM_WORKER_THREAD; i++)
    		workers[i] = std::thread(thread_delete, std::ref(fptree), std::ref(keys), i);
	    for (uint64_t i = 0; i < NUM_WORKER_THREAD; i++)
	    	workers[i].join();
	    std::cout << "Deletion complete (" << std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - start).count() << " sec)\n";
	    keys.erase(keys.begin() + half, keys.end());
	    values.erase(values.begin() + half, values.end());

	    #if CHECK_DELETE == 1
	    	printf("Starting sanity check for delete...\n");
		    if (ins.SanityCheck(fptree, keys, values))
				std::cout << "Sanity check for deletion passed!\n";
		    else
		    	return -1;
		#else
		    printf("Skip deletion check.\n");
		#endif
	#endif

	#if BULK_LOAD
		printf("Bulk load current index!\n");
		FPtree bulk_load_tree;
		if (ins.SanityCheck(bulk_load_tree, keys, values))
			std::cout << "Sanity check for bulk load passed!\n";
	    else
	    	return -1;
	#else
	#endif

	// #if UPDATE == 1
	// 	printf("Updating all values...\n");

	// #endif

	return 0;
}