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
    // std::shared_mutex mutex_;
};


fptree_wrapper::fptree_wrapper()
{
}

fptree_wrapper::~fptree_wrapper()
{
    tree_.printTSXInfo();
}

bool fptree_wrapper::find(const char* key, size_t key_sz, char* value_out)
{
    // For now only support 8 bytes key and value (uint64_t)
    assert(key_sz == sizeof(uint64_t));
    // std::shared_lock lock(mutex_);
    uint64_t value = tree_.find(*reinterpret_cast<uint64_t*>(const_cast<char*>(key)));
    if (value == 0)
        return false;
    memcpy(value_out, &value, sizeof(value));
    return true;
}


bool fptree_wrapper::insert(const char* key, size_t key_sz, const char* value, size_t value_sz)
{
    // assert(key_sz == sizeof(uint64_t) && value_sz == sizeof(uint64_t));
    // std::unique_lock lock(mutex_);
    KV kv = KV(*reinterpret_cast<uint64_t*>(const_cast<char*>(key)), *reinterpret_cast<uint64_t*>(const_cast<char*>(value)));
    tree_.insert(kv);
    return true;
}

bool fptree_wrapper::update(const char* key, size_t key_sz, const char* value, size_t value_sz)
{
    // assert(key_sz == sizeof(uint64_t) && value_sz == sizeof(uint64_t));
    // std::unique_lock lock(mutex_);
    KV kv = KV(*reinterpret_cast<uint64_t*>(const_cast<char*>(key)), *reinterpret_cast<uint64_t*>(const_cast<char*>(value)));
    tree_.update(kv);
    return true;
}

bool fptree_wrapper::remove(const char* key, size_t key_sz)
{
    // assert(key_sz == sizeof(uint64_t));
    // std::unique_lock lock(mutex_);
    tree_.deleteKey(*reinterpret_cast<uint64_t*>(const_cast<char*>(key)));
    return true;
}

int fptree_wrapper::scan(const char* key, size_t key_sz, int scan_sz, char*& values_out)
{
    // assert(key_sz == sizeof(uint64_t));
    // std::shared_lock lock(mutex_);

    constexpr size_t ONE_MB = 1ULL << 20;
    static thread_local std::array<char, ONE_MB> results;

    int scanned;
    char* dst = reinterpret_cast<char*>(results.data());

    tree_.ScanInitialize(*reinterpret_cast<uint64_t*>(const_cast<char*>(key)));
    if (tree_.ScanComplete())
        return 1;

    for(scanned=0; (scanned < scan_sz) && (!tree_.ScanComplete()); ++scanned)
    {
        KV kv = tree_.ScanNext();
        memcpy(dst, &kv.key, sizeof(uint64_t));
        dst += sizeof(uint64_t);
        memcpy(dst, &kv.value, sizeof(uint64_t));
        dst += sizeof(uint64_t);
    }
    values_out = results.data();
    return scanned;
}

#endif