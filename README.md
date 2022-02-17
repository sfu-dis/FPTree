# FPTree
An open-source [FPTree](https://wwwdb.inf.tu-dresden.de/misc/papers/2016/Oukid_FPTree.pdf) implementation

```
I. Oukid, J. Lasperas, A. Nica, T. Willhalm, and W. Lehner. FPTree: A Hybrid SCM-DRAM Persistent and Concurrent B-Tree for Storage Class Memory. 
In Proceedings of the 2016 International Conference on Management of Data, SIGMOD’16, pages 371–386. ACM, 2016
```


## Important information before build !
FPTree use Intel Threading Building Blocks (oneTBB) for concurrency control. 
The default retry threshold for oneTBB is only 10 for read write mutex.  <br/>
1. To achieve better scalability, we are using customized TBB library for FPTree
(which is also the approach taken by the original author). <br/> Here are the steps to generate libtbb.so:<br/>
	* Clone oneTBB from github (https://github.com/oneapi-src/oneTBB.git)<br/> **to this repo** (i.e., /path/to/your/fptree/oneTBB).
	* Modify the read/write retry from **10 to 256** in `oneTBB/src/tbb/rtm_mutex.cpp` and `oneTBB/src/tbb/rtm_rw_mutex.cpp`<br/>
	* `cd oneTBB && mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=Release .. && make -j`<br/>
	* Check that libtbb.so exists in *oneTBB/build/gnu_11.1_cxx11_64_release*<br/>
    * Modify **CMakeLists.txt** located in FPTree folder to use custom TBB <br/>
       * delete -ltbb flag in CMAKE_CXX_FLAGS which link to your default TBB built <br/>
       * add and modify them in the proper place in CMakeLists.txt
       ```
       link_directories(oneTBB/build/gnu_11.1_cxx11_64_release) # gnu version and cxx version could vary
       include_directories(oneTBB/include)
       ```
	* After the line that adds fptree pibench wrapper library, do target link below.
       ```
       target_link_libraries(fptree_pibench_wrapper libtbb.so)
       ```
    * Change header files in fptree.h to include those from custom tbb like this: 
       ```
       // #include <tbb/spin_mutex.h>
       // #include <tbb/spin_rw_mutex.h>
       #include "oneapi/tbb/spin_mutex.h"
       #include "oneapi/tbb/spin_rw_mutex.h"
       ```
2. Modify `#define PMEMOBJ_POOL_SIZE` in fptree.h if BACKEND = PMEM (defined in CMakeLists.txt)<br/>
3. Modify `#define MAX_INNER_SIZE 128` and `#define MAX_LEAF_SIZE 64` in fptree.h if you want. These are tunable variable. 
4. To use HTM, you will need to turn on TSX on your machine. If you execute `lscpu` and see `Vulnerability Tsx async abort:   Vulnerable`, then TSX is turned on. Otherwise, here is an example of how to turn on TSX on archlinux.
* Make sure everything's up-to-date and consistent. Use pacman to do an update.
* Add this line to /etc/default/grub: 
```GRUB_CMDLINE_LINUX_DEFAULT="tsx=on tsx_async_abort=off loglevel=3 quiet"```
* Run these commands:
```
grub-mkconfig -o /boot/grub/grub.cfg
systemctl reboot
```
## Build (check out the next section for running pibench with the fptree wrapper)

### Build PMEM Version

```bash
mkdir build && cd build
cmake -DPMEM_BACKEND=PMEM ..
```

### Build DRAM Version

```bash
mkdir build && cd build
cmake -DPMEM_BACKEND=DRAM ..
```

All executables are in `build/src` folder

#### Inspector executable
```bash
mkdir build && cd build
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
mkdir Release && cd Release
cmake -DPMEM_BACKEND=<PMEM|DRAM> -DTEST_MODE=0 -DBUILD_INSPECTOR=0 ..
```

### Troubleshooting
(1) If you see the error below when you try to run PiBench with this wrapper:
```
Error in dlopen(): /lib/x86_64-linux-gnu/libjemalloc.so.2: cannot allocate memory in static TLS block
```
You can try adding `LD_PRELOAD` before the PiBench executable:
```
LD_PRELOAD=/path/to/your/libjemalloc.so ./PiBench ...
```
(2) `libpmemobj.so` error.
```
Error in dlopen(): /path/to/libfptree_pibench_wrapper.so: undefined symbol: _pobj_cache_invalidate
```
Add `/path/to/libpmemobj.so` to `LD_PRELOAD`.
