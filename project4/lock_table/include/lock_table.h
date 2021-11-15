#ifndef __LOCK_TABLE_H__
#define __LOCK_TABLE_H__

#include <stdint.h>
#include <utility>
#include <pthread.h>
#include <unordered_map>
#include <cstdio>

typedef int64_t table_t;
typedef int64_t key__t;
typedef struct lock_t lock_t;
typedef struct entry_t entry_t;
typedef std::pair<table_t, key__t> record_t;

struct entry_t {
    record_t tk;
    lock_t* head;
    lock_t* tail;
    entry_t(table_t t, key__t k, lock_t* h, lock_t* tl) : tk(t, k), head(h), tail(tl) {}
    entry_t() {}
};

struct lock_t {
    lock_t* prev;
    lock_t* next;
    entry_t* sentinel;
    pthread_cond_t condition;
};

struct hash_t {
    auto operator() (const record_t& rec) const {
        return std::hash<int64_t>() (rec.first ^ 0x5555555555555555) ^ std::hash<int64_t>()(rec.second);
    }
};

/* APIs for lock table */
int init_lock_table();
lock_t *lock_acquire(table_t table_id, key__t key);
int lock_release(lock_t* lock_obj);
// void printList();
#endif /* __LOCK_TABLE_H__ */
