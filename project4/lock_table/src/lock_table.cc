#include "lock_table.h"

std::unordered_map<table_t, std::unordered_map<key__t, entry_t> > lock_table;
pthread_mutex_t lock_table_latch;


// int init_lock_table(void)
// • Initialize any data structures required for implementing lock table, such as hash table, lock table latch, etc.
// • If success, return 0. Otherwise, return non-zero value.
int init_lock_table() {
    return pthread_mutex_init(&lock_table_latch, NULL);
}

// lock_t* lock_acquire(int64_t table_id, int64_t key)
// • Allocate and append a new lock object to the lock list of the record having the key.
// • If there is a predecessor’s lock object in the lock list, sleep until the predecessor to release its lock.
// • If there is no predecessor’s lock object, return the address of the new lock object.
// • If an error occurs, return NULL.
lock_t* lock_acquire(table_t table_id, key__t key) {
    if (pthread_mutex_lock(&lock_table_latch)) {
        printf("in lock_acquire pthread_mutex_lock nonzero return value");
        return NULL;
    }

    auto tmp = lock_table.find(table_id);
    // not in lock table -> insert empty list into table
    if (tmp == lock_table.end() || (tmp->second).find(key) == (tmp->second).end()) { 
        entry_t entry(table_id, key, NULL, NULL);
        lock_table[table_id][key] = entry;
    } 

    entry_t* entry = &(lock_table[table_id][key]);
    lock_t* lock = new lock_t();
    // sleep until the predecessor releases its lock
    if (entry->head) { 
        lock_t* last = entry->tail;
        last->next = lock;
        lock->prev = last;
        lock->next = NULL;
        entry->tail = lock;
        lock->sentinel = entry;
        if (pthread_cond_init(&(lock->condition), NULL)) {
            printf("in lock_acquire pthread_cond_init nonzero return value");
            return NULL;
        }
        if (pthread_cond_wait(&(last->condition), &lock_table_latch)) {
            printf("in lock_acquire pthread_cond_wait nonzero return value");
            return NULL;
        }
    }
    // no predecessor's lock object
    else { 
        lock->prev = NULL;
        lock->next = NULL;
        lock->sentinel = entry;
        if (pthread_cond_init(&(lock->condition), NULL)) {
            printf("in lock_acquire pthread_cond_init nonzero return value");
            return NULL;
        }
        entry->head = entry->tail = lock;
    }

    if (pthread_mutex_unlock(&lock_table_latch)) {
        printf("in lock_acquire pthread_mutex_unlock nonzero return value");
        return NULL;
    }
    return lock;
}

// int lock_release(lock_t* lock_obj)
// • Remove the lock_obj from the lock list.
// • If there is a successor’s lock waiting for the thread releasing the lock, wake up the successor.
// • If success, return 0. Otherwise, return non-zero value.
int lock_release(lock_t* lock_obj) {
    if (pthread_mutex_lock(&lock_table_latch)) {
        printf("in lock_release pthread_mutex_lock nonzero return value");
        return 1;
    }

    entry_t* entry = lock_obj->sentinel;
    if (entry->head == lock_obj) {
        entry->head = lock_obj->next;
    }
    else {
        lock_obj->prev->next = lock_obj->next;
    }

    if (entry->tail == lock_obj) {
        entry->tail = lock_obj->prev;
    }
    else {
        lock_obj->next->prev = lock_obj->prev;
    }

    if (pthread_cond_signal(&(lock_obj->condition))) {
        printf("in lock_release pthread_cond_signal nonzero return value");
        return 1;
    }
    if (pthread_mutex_unlock(&lock_table_latch)) {
        printf("in lock_release pthread_mutex_unlock nonzero return value");
        return 1;
    }
    free(lock_obj);
    return 0;
}
