#ifndef __TXN_H__
#define __TXN_H__

#include "yjinbpt.h"
#include <stdint.h>
#include <utility>
#include <pthread.h>
#include <unordered_map>
#include <cstdio>

#define SHARED 0
#define EXCLUSIVE 1

typedef struct lock_t lock_t;
typedef struct lock_entry_t lock_entry_t;

struct lock_entry_t {
    std::pair<table_t, pagenum_t> tk;
    lock_t* head;
    lock_t* tail;
    lock_entry_t(table_t t, key__t k, lock_t* h, lock_t* tl) : tk(t, k), head(h), tail(tl) {}
    lock_entry_t() {}
};

struct lock_t {
    lock_t* prev;
    lock_t* next;
    lock_entry_t* sentinel;
    pthread_cond_t condition;
    key__t record_id;
    int lock_mode;
    lock_t* trx_next;
    int trx_id;
};

struct hash_t {
    auto operator() (const std::pair<table_t, pagenum_t>& rec) const {
        return std::hash<int64_t>() (rec.first ^ 0x5555555555555555LL) ^ std::hash<int64_t>()(rec.second);
    }
};


struct trx_entry_t {
    int trx_id;
    pthread_mutex_t mutex;
    lock_t* head;
    lock_t* tail;
    std::map<std::pair<table_t, key__t>, std::vector<std::pair<key__t, std::string> > > old_vals;
    trx_entry_t(int trx_id) : trx_id(trx_id), head(NULL), tail(NULL) {
        pthread_mutex_init(&mutex, NULL);
    }
    trx_entry_t() : trx_id(0), head(NULL), tail(NULL) {
        pthread_mutex_init(&mutex, NULL);
    }
};

extern std::map<int, trx_entry_t> trx_table;
extern std::unordered_map<std::pair<table_t, key__t>, lock_entry_t, hash_t> lock_table;
extern pthread_mutex_t trx_table_latch;
extern pthread_mutex_t lock_table_latch;



int init_lock_table();
lock_t* lock_acquire(table_t table_id, pagenum_t page_id, key__t key, int trx_id, int lock_mode);
int lock_release(lock_t* lock_obj);
bool cycle_made(table_t table_id, pagenum_t pn, key__t key, int trx_id, int lock_mode);
bool trx_init_table();
int trx_begin(void);
int trx_commit(int trx_id);
int trx_abort(int trx_id);
int trx_undo(int trx_id);
int trx_release_locks(int trx_id);
void push_back_lock(lock_t*);
#endif /* __TXN_H__ */