#include "fptree_wrapper.hpp"

extern "C" tree_api* create_tree(const tree_options_t& opt)
{

#ifdef PMEM
    auto path_ptr = new std::string(opt.pool_path);

    if (*path_ptr == "")
        path_ptr->assign("./pool");
    
    const char* path = (*path_ptr).c_str();

    struct stat s;
    if (stat(path, &s) == 0 && (s.st_mode & S_IFDIR))
	path_ptr->assign("./pool");

    printf("hpp path: %s", path);

    long long pool_size = (opt.pool_size == 0) ? PMEMOBJ_POOL_SIZE : pool_size;

    return new fptree_wrapper(path, pool_size);
#else
    return new fptree_wrapper();
#endif
}
