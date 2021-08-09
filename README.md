# FPTree
An open-source [FPTree](https://wwwdb.inf.tu-dresden.de/misc/papers/2016/Oukid_FPTree.pdf) implementation

```
I. Oukid, J. Lasperas, A. Nica, T. Willhalm, and W. Lehner. FPTree: A Hybrid SCM-DRAM Persistent and Concurrent B-Tree for Storage Class Memory. 
In Proceedings of the 2016 International Conference on Management of Data, SIGMOD’16, pages 371–386. ACM, 2016
```

## Build

### Build PMEM Version

```bash
mkdir build & cd build
cmake -DPMEM_BACKEND=PMEM ..
```

### Build DRAM Version

```bash
mkdir build & cd build
cmake -DPMEM_BACKEND=DRAM ..
```

All executables are in `build/src` folder

#### Interative executable
Above command help you build `fptree` executable

It will pre-load 100 keys and then let you play with

```Enter the key to insert, delete or update (-1):```

Enter `exit` to leave the program and then it will perform the scan operation from the header leaf node

#### Inspector executable
```bash
mkdir build & cd build
cmake -DPMEM_BACKEND=${BACKEND} -DBUILD_INSPECTOR=1 ..
```

Run `inspector` to check the correctness of single/multi-threaded insert, delete operations for both leaf nodes and inner nodes 

If you want to check performance of this implementation, please see PiBench instruction below 

#### Other build options
`-DBUILD_INSPECTOR=1` to build inspector executable which can check the correctness of single/multi-threaded operations (insert, delete..)

`-DTEST_MODE=1` to set the size of leaf nodes & inner nodes. (TEST MODE: MAX_INNER_SIZE=3 MAX_LEAF_SIZE=4 for debug usage)

`-DNDEBUG=1` to disable the assertion statements (set to 0 for debug usage)

## Benchmark on PiBench

We officially support FPTree wrapper for pibench:

Checkout PiBench here: https://github.com/wangtzh/pibench

### Build/Create FPTree shared lib

```bash
mkdir Release & cd Release
cmake -DPMEM_BACKEND=${BACKEND} -DTEST_MODE=0 -DBUILD_INSPECTOR=0 -DNDEBUG=1 ..
```
