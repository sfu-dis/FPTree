#include <iostream>
#include <vector>
#include <algorithm>
#include <limits>
#include <cassert>

#include "fptree.h"


#define NUM_RECORDS 5000000

#define INSERT_RATIO 0

#define UPDATE_RATIO 0 

#define READ_RATIO 0 

#define DELETE_RATIO 0


class Inspector {
public:
	Inspector();
    ~Inspector();

    void ClearStats();
    bool SanityCheck(FPtree& tree, std::vector<uint64_t>& keys, std::vector<uint64_t>& values);
    bool InnerNodeOrderCheck(BaseNode* node, uint64_t min, uint64_t max, std::vector<uint64_t>& keys);
    bool KVPresenceCheck(FPtree& tree, std::vector<uint64_t>& keys, std::vector<uint64_t>& values);

private:
	uint64_t kv_missing_count_;
	uint64_t kv_duplicate_count_;
	uint64_t inner_order_violation_count_;
	uint64_t inner_boundary_violation_count_;
	uint64_t inner_duplicate_count_;
	uint64_t inner_invalid_count_;

	uint64_t keyArr[MAX_INNER_SIZE];
};

Inspector::Inspector()
{
	ClearStats();
}

Inspector::~Inspector(){}

bool Inspector::SanityCheck(FPtree& tree, std::vector<uint64_t>& keys, std::vector<uint64_t>& values) 
{
	uint64_t passed = 0;
	std::cout << "\nLeafNode check\n";
	passed += KVPresenceCheck(tree, keys, values);
	std::cout << "\nInnerNode check\n";
	std::vector<uint64_t> vec(keys);
	std::sort(vec.begin(), vec.end());
	passed += InnerNodeOrderCheck(tree.root, 0, std::numeric_limits<uint64_t>::max(), vec);
	
	return passed == 2;
}

bool Inspector::KVPresenceCheck(FPtree& tree, std::vector<uint64_t>& keys, std::vector<uint64_t>& values)
{
	// missing kv check in normal read approach
	uint64_t val, prev_kv_missing_count = kv_missing_count_, prev_kv_duplicate_count_ = kv_duplicate_count_;
	for (uint64_t i = 0; i < NUM_RECORDS; i++)
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
		cur = cur->p_next;
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
	if (kv_missing_count_ != prev_kv_missing_count || kv_duplicate_count_ != prev_kv_duplicate_count_)
		return false;
	return true;
}

bool Inspector::InnerNodeOrderCheck(BaseNode* node, uint64_t min, uint64_t max, std::vector<uint64_t>& keys)
{
	bool ret = true;
	if (node->isInnerNode)
	{
		InnerNode *  inner = reinterpret_cast<InnerNode*> (node);
		assert(inner->nKey > 0 && "Reached empty innernode!\n");
		for (uint64_t i = 0; i < inner->nKey; i++)
		{
			if (std::find(keys.begin(), keys.end(), inner->keys[i]) == keys.end())
			{
				ret = false;
				std::cout << "Innernode invalid key: " << inner->keys[i] << std::endl;
				inner_invalid_count_++;
			}
			if (i && inner->keys[i] < inner->keys[i-1])
			{
				ret = false;
				std::cout << "Innernode order violation: " << inner->keys[i] << " " << inner->keys[i-1] << std::endl;
				inner_order_violation_count_++;
			}
		}
		std::copy(inner->keys, inner->keys + inner->nKey, keyArr);
		if (!ret)
			std::sort(std::begin(keyArr), std::end(keyArr));
		for (uint64_t i = 1; i < inner->nKey; i++)
		{
			if (keyArr[i] == keyArr[i-1])
			{
				ret = false;
				std::cout << "Innernode duplicate key: " << keyArr[i] << std::endl;
				inner_duplicate_count_++;
			}
		}
		if (keyArr[0] < min || keyArr[inner->nKey-1] >= max)
		{
			ret = false;
			std::cout << "Innernode boundary violation: " << keyArr[0] << " " << keyArr[inner->nKey-1] 
			<< " Min: " << min << " " << " Max: " << max << std::endl;
			inner_boundary_violation_count_++;
		}
		for (uint64_t i = 0; i <= inner->nKey; i++)
		{
			if (i == 0)
				ret = ret && InnerNodeOrderCheck(inner->p_children[i], min, inner->keys[0], keys);
			else if (i == inner->nKey)
				ret = ret && InnerNodeOrderCheck(inner->p_children[i], inner->keys[inner->nKey-1], max, keys);
			else
				ret = ret && InnerNodeOrderCheck(inner->p_children[i], inner->keys[i-1], inner->keys[i], keys);
		}
	}
	return ret;
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


int main()
{
	std::independent_bits_engine<std::default_random_engine, 64, uint64_t> rbe;
    std::vector<uint64_t> keys(NUM_RECORDS);
    std::generate(begin(keys), end(keys), std::ref(rbe));
    std::vector<uint64_t> values(NUM_RECORDS);
    std::generate(begin(values), end(values), std::ref(rbe));

    std::cout << "Key generation complete, start loading...\n";

    FPtree fptree;
    for (uint64_t i = 0; i < NUM_RECORDS; i++)
        fptree.insert(KV(keys[i], values[i]));

    Inspector ins;

    std::cout << "Loading complete, start testing...\n";

    ins.SanityCheck(fptree, keys, values);

	return 0;
}