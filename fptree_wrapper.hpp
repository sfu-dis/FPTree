#ifndef __FPTREE_WRAPPER_HPP__
#define __FPTREE_WRAPPER_HPP__

#include "tree_api.hpp"
#include "fptree.h"

#include <cstring>
#include <mutex>
#include <shared_mutex>
#include <libpmemobj.h>


class fptree_wrapper : public tree_api
{
public:
    fptree_wrapper();
    virtual ~fptree_wrapper();
    
    virtual bool find(const char* key, size_t key_sz, char* value_out) override;
    virtual bool insert(const char* key, size_t key_sz, const char* value, size_t value_sz) override;
    virtual bool update(const char* key, size_t key_sz, const char* value, size_t value_sz) override;
    virtual bool remove(const char* key, size_t key_sz) override;
    virtual int scan(const char* key, size_t key_sz, int scan_sz, char*& values_out) override;

private:
    FPtree tree_;
};


fptree_wrapper::fptree_wrapper()
{
}

fptree_wrapper::~fptree_wrapper()
{
}

bool fptree_wrapper::find(const char* key, size_t key_sz, char* value_out)
{
    // For now only support 8 bytes key and value (uint64_t)
    uint64_t value = tree_.find(*reinterpret_cast<uint64_t*>(const_cast<char*>(key)));
    if (value == 0)
        return false;
    memcpy(value_out, &value, sizeof(value));
    return true;
}


bool fptree_wrapper::insert(const char* key, size_t key_sz, const char* value, size_t value_sz)
{
    KV kv = KV(*reinterpret_cast<uint64_t*>(const_cast<char*>(key)), *reinterpret_cast<uint64_t*>(const_cast<char*>(value)));
    tree_.insert(kv);
    return true;
}

bool fptree_wrapper::update(const char* key, size_t key_sz, const char* value, size_t value_sz)
{
    KV kv = KV(*reinterpret_cast<uint64_t*>(const_cast<char*>(key)), *reinterpret_cast<uint64_t*>(const_cast<char*>(value)));
    tree_.update(kv);
    return true;
}

bool fptree_wrapper::remove(const char* key, size_t key_sz)
{
    tree_.deleteKey(*reinterpret_cast<uint64_t*>(const_cast<char*>(key)));
    return true;
}

int fptree_wrapper::scan(const char* key, size_t key_sz, int scan_sz, char*& values_out)
{
    return tree_.rangeScan(*reinterpret_cast<uint64_t*>(const_cast<char*>(key)), (uint64_t)scan_sz, values_out);
}

#endif