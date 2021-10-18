# FPTree
An open-source [FPTree](https://wwwdb.inf.tu-dresden.de/misc/papers/2016/Oukid_FPTree.pdf) implementation

```
I. Oukid, J. Lasperas, A. Nica, T. Willhalm, and W. Lehner. FPTree: A Hybrid SCM-DRAM Persistent and Concurrent B-Tree for Storage Class Memory. 
In Proceedings of the 2016 International Conference on Management of Data, SIGMOD’16, pages 371–386. ACM, 2016
```


## Important information before installation
FP-Tree use Intel Threading Building Blocks (oneTBB) for concurrency control. 
The default retry threshold for oneTBB is only 10 for read write mutex.  <br/>
1. To achieve better scalability, we are using customized TBB library for FP-Tree
(which is also the approach taken by the original author). <br/> Here are the steps to generate libtbb.so:<br/>
	* Clone oneTBB from github (https://github.com/oneapi-src/oneTBB.git)<br/>
	* Modify the read/write retry from **10 to 256** in ***oneTBB/src/tbb/rtm_mutex.cpp*** and ***oneTBB/src/tbb/rtm_rw_mutex.cpp***<br/>
	* `$ cd oneTBB & mkdir build & cd build & cmake -DCMAKE_BUILD_TYPE=Release .. & make -jN`<br/>
	* Check that libtbb.so exists in *oneTBB/build/gnu_11.1_cxx11_64_release*<br/>
    * Modify **CMakeLists.txt** to use custom TBB <br/>
       * delete -ltbb flag in CMAKE_CXX_FLAGS which link to your default TBB built <br/>
       * add and modify them in the proper place in CMakeLists.txt
       ```
         link_directories(oneTBB/build/gnu_11.1_cxx11_64_release) 
         include_directories(/yourpathtoTBB/oneTBB/include)
         target_link_libraries(fptree_pibench_wrapper libtbb.so)
       ``` 
2. Modify `#define PMEMOBJ_POOL_SIZE` in fptree.h if BACKEND = PMEM <br/>
3. Modify `#define MAX_INNER_SIZE 128` and `#define MAX_LEAF_SIZE 64` in fptree.h if you want. These are tunable variable. 

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

## Benchmark on PiBench

We officially support FPTree wrapper for pibench:

Checkout PiBench here: https://github.com/sfu-dis/pibench

### Build/Create FPTree shared lib

```bash
mkdir Release & cd Release
cmake -DPMEM_BACKEND=${BACKEND} -DTEST_MODE=0 -DBUILD_INSPECTOR=0 ..
```
