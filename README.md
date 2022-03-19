# FPTree PiBench Wrapper

To use HTM, you will need to turn on TSX on your machine. If you execute `lscpu` and see `Vulnerability Tsx async abort:   Vulnerable`, then TSX is turned on. Otherwise, here is an example of how to turn on TSX on archlinux.
* Make sure everything's up-to-date and consistent. Use pacman to do an update.
* Add this line to /etc/default/grub: 
```GRUB_CMDLINE_LINUX_DEFAULT="tsx=on tsx_async_abort=off loglevel=3 quiet"```
* Run these commands:
```
grub-mkconfig -o /boot/grub/grub.cfg
systemctl reboot
```

### 1. Customize TBB

To achieve better scalability, we customized Intel TBB library for FP-Tree (which is also the approach taken by the original author). Here are the steps to generate `libtbb.so`:

- Clone oneTBB from its original repo: https://github.com/oneapi-src/oneTBB.git
- Replace `oneTBB/src/tbb/rtm_mutex.cpp` and `oneTBB/src/tbb/rtm_rw_mutex.cpp` with files in FPTree folder
- Build it: `$ cd oneTBB && mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=Release .. && make -jN`
- Make sure that `libtbb.so` exists in `oneTBB/build/gnu_11.1_cxx11_64_release`

### 2. Build FPTree
```
$ mkdir build && cd build
```

To build PMEM FPTree
```
$ cmake -DPMEM_BACKEND=PMEM -DTEST_MODE=0 -DNDEBUG=1 .. && make
````

To build PMEM FPTree with var-key support
```
$ cmake -DPMEM_BACKEND=PMEM -DTEST_MODE=0 -DNDEBUG=1 -DVAR_KEY=1 .. && make                      
````

To build DRAM FPTree 
``` 
$ cmake -DPMEM_BACKEND=DRAM -DTEST_MODE=0 -DNDEBUG=1 .. && make 
```


### 3. Notes
A summary of modifications made:
- Modified `CMakeLists.txt` to use the customized TBB (with # retries increased to 256).
- Changed header files in `fptree.h` to include those from the customized TBB.
- For DRAM mode, we introduced in-place node updates for better performance.
- Added var-key support.
