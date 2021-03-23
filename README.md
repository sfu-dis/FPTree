# FPTree
A single-thread implementation of FPTree



Modification:

removed using namespace std;


InnerNode:
	+ InnerNode::InnerNode(const InnerNode& inner)


LeafNode:
	+ LeafNode::LeafNode()

	+ LeafNode::LeafNode(const LeafNode& leaf)

	+ LeafNode& LeafNode::operator=(const LeafNode& other)

	+ void LeafNode::sortKV()

	- static void showList(PMEMobjpool *pop)

	- static int constructLeafNode(PMEMobjpool *pop, void *ptr, void *arg)


FPTree:
	* FPtree::FPtree()

	* FPtree::~FPtree()

	* uint64_t FPtree::find(uint64_t key)

	* bool FPtree::update(struct KV kv)

	* bool FPtree::insert(struct KV kv) 

	- splitLeafAndUpdateInnerParents(...)
	changed to
	+ void FPtree::insertKVAndUpdateTree(LeafNode* reachedLeafNode, InnerNode* parentNode, struct KV kv, uint64_t prevPos = MAX_LEAF_SIZE)

	* void FPtree::updateParents(uint64_t splitKey, InnerNode* parent, BaseNode* child)

	* 


About Scan (Changed logic): 

when need to scan a leaf node, first copy it to dram --> leaf_cpy. Then sort leaf_cpy without worrying about fingerprints
Because leaf_cpy is for scan only.

bitmap_idx is for recording current scan progress, if it == MAX_LEAF_SIZE then scan is complete
		