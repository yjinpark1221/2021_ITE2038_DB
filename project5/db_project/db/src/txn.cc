#ifndef MAINTEST
#include "txn.h"
#endif

pthread_mutex_t trx_table_latch;
pthread_mutex_t lock_table_latch;
std::map<int, trx_entry_t> trx_table;
std::unordered_map<std::pair<table_t, key__t>, lock_entry_t, hash_t> lock_table;

bool trx_init_table() {
    //printf("%s\n", __func__);
    return pthread_mutex_init(&trx_table_latch, NULL);
}

// • Allocate a transaction structure and initialize it.
// • Return a unique transaction id (>= 1) if success, otherwise return 0.
// • Note that the transaction id should be unique for each transaction; that is, you need to allocate a
// transaction id holding a mutex.
int trx_begin(void) {
    //printf("%s\n", __func__);
    if (pthread_mutex_lock(&trx_table_latch)) {
        //printf("in trx_begin pthread_mutex_lock\n");
        return 0;
    }
    static int transaction_id = 1;
    trx_entry_t entry(transaction_id);
    trx_table[transaction_id] = entry;
    int ret = transaction_id++;
    if (pthread_mutex_unlock(&trx_table_latch)) {
        //printf("in trx_begin pthread_mutex_unlock\n");
        return 0;
    }
    return ret;
}

// • Clean up the transaction with the given trx_id (transaction id) and its related information that has
// been used in your lock manager. (Shrinking phase of strict 2PL)
// • Return the completed transaction id if success, otherwise return 0.
int trx_commit(int trx_id) {
    //printf("%s\n", __func__);
    if (pthread_mutex_lock(&trx_table_latch)) {
        //printf("in trx_commit pthread_mutex_lock\n");
        return 0;
    }
    if (trx_release_locks(trx_id)) {
        //printf("in trx_commit trx_release_locks\n");
        return 0;
    }
    trx_table.erase(trx_id);
    if (pthread_mutex_unlock(&trx_table_latch)) {
        //printf("in trx_commit pthread_mutex_unlock\n");
        return 0;
    }
    return trx_id;
}
int trx_abort(int trx_id) {
    if (pthread_mutex_lock(&trx_table_latch)) {
        //printf("in trx_commit pthread_mutex_lock\n");
        return 0;
    }
    trx_undo(trx_id);
    trx_release_locks(trx_id);
    trx_table.erase(trx_id);
    //printf("deadlock\n");
    if (pthread_mutex_unlock(&trx_table_latch)) {
        //printf("in trx_commit pthread_mutex_unlock\n");
        return 0;
    }
    return 1;
}
int trx_undo(int trx_id) {
    //printf("%s\n", __func__);
    auto& entry = trx_table[trx_id];
    for (auto old : entry.old_vals) {
        table_t table_id = old.first.first;
        key__t key = old.first.second;
        pagenum_t pn = old.second.first;
        std::string value = old.second.second;

        page_t page;
        ctrl_t* ctrl = buf_read_page(table_id, pn);
        mleaf_t leaf = *(ctrl->frame);
        auto iter = std::lower_bound(leaf.slots.begin(), leaf.slots.end(), key);
        leaf.values[iter - leaf.slots.begin()] = value;

        page = leaf;
        buf_write_page(table_id, pn, &page);
        pthread_mutex_unlock(&(ctrl->mutex));
    }
    return 0;
}

int trx_release_locks(int trx_id) {
    //printf("%s\n", __func__);
    lock_t* lock = trx_table[trx_id].head;
    for (; lock; lock = lock->trx_next) {
        if (lock_release(lock)) return 1;
    }
    return 0;
}


// • Initialize any data structures required for implementing lock table, such as hash table, lock table latch, etc.
// • If success, return 0. Otherwise, return non-zero value.
int init_lock_table() {
    //printf("%s\n", __func__);
    return pthread_mutex_init(&lock_table_latch, NULL);
}

// • Allocate and append a new lock object to the lock list of the record having the key.
// • If there is a predecessor’s lock object in the lock list, sleep until the predecessor to release its lock.
// • If there is no predecessor’s lock object, return the address of the new lock object.
// • If an error occurs, return NULL.
// • lock_mode: 0 (SHARED) or 1 (EXCLUSIVE)
lock_t* lock_acquire(table_t table_id, pagenum_t page_id, key__t key, int trx_id, int lock_mode) {
    //printf("%s\n", __func__);
    if (pthread_mutex_lock(&lock_table_latch)) {
        //printf("in lock_acquire pthread_mutex_lock nonzero return value");
        return NULL;
    }

    auto tmp = lock_table.find({table_id, page_id});
    // not in lock table -> insert empty list into table
    if (tmp == lock_table.end()) { 
        lock_entry_t entry(table_id, page_id, NULL, NULL);
        lock_table[{table_id, page_id}] = entry;
    } 

    lock_entry_t* entry = &(lock_table[{table_id, page_id}]);
    bool has_predecessor = 0;
    // case : s lock
    if (lock_mode == SHARED) {
        for (lock_t* l = entry->head; l; l = l->next) {
            if (l->record_id == key) has_predecessor = 1;
            if (l->record_id != key || l->trx_id != trx_id) continue;
            // case : s lock or x lock found 
            // do not need to acquire new lock
            else {
                if (pthread_mutex_unlock(&lock_table_latch)) {
                    //printf("in lock_acquire pthread_mutex_unlock nonzero return value");
                    return NULL;
                }                
                return l;
            }
        }    
    }
    // case : x lock
    else {
        for (lock_t* l = entry->head; l; l = l->next) {
            if (l->record_id == key) has_predecessor = 1;
            if (l->record_id != key || l->trx_id == trx_id) continue;
            
            // case : s lock found
            if (l->lock_mode == SHARED) {
                lock_t* last = entry->tail;
                // check if it is the last lock of the record
                bool is_last = 1;
                for (lock_t* ll = entry->head; ll; ll = ll->next) {
                    if (ll->record_id == key && ll != l) {
                        is_last = 0;
                        break;
                    }
                }
                // if last-> do not wait and upgrade the lock to EXCLUSIVE
                if (is_last) {
                    l->lock_mode = EXCLUSIVE;
                    if (pthread_mutex_unlock(&lock_table_latch)) {
                    //printf("in lock_acquire pthread_mutex_unlock nonzero return value");
                        return NULL;
                    }                
                    return l;
                }
                // if not last -> check dead lock and wait

                // case : deadlock
                if (cycle_made(table_id, page_id, key, trx_id, lock_mode)) {
                    return NULL;
                }

                lock_t* lock = (lock_t*)malloc(sizeof(lock_t));
                //printf("entry->head != NULL\n");
                last->next = lock;
                lock->prev = last;
                lock->next = NULL;
                entry->tail = lock;
                lock->sentinel = entry;
                lock->record_id = key;
                lock->lock_mode = lock_mode;
                if (pthread_cond_init(&(lock->condition), NULL)) {
                    //printf("in lock_acquire pthread_cond_init nonzero return value");
                    return NULL;
                }
                if (pthread_cond_wait(&(lock->condition), &lock_table_latch)) {
                    //printf("in lock_acquire pthread_cond_wait nonzero return value");
                    return NULL;
                }
                if (pthread_mutex_unlock(&lock_table_latch)) {
                    //printf("in lock_acquire pthread_mutex_unlock nonzero return value");
                    return NULL;
                }                
                return l;
            }
            // case : x lock found
            // do not need to acquire new lock
            else {
                // if (pthread_cond_wait(&(l->condition), &lock_table_latch)) {
                //     //printf("in lock_acquire pthread_cond_wait nonzero return value");
                //     return NULL;
                // }
                if (pthread_mutex_unlock(&lock_table_latch)) {
                    //printf("in lock_acquire pthread_mutex_unlock nonzero return value");
                    return NULL;
                }                
                return l;
            }
        }
    }
    lock_t* lock = (lock_t*)malloc(sizeof(lock_t));
    // sleep until the predecessor releases its lock
    if (has_predecessor) { 
        lock_t* last = entry->tail;
        // case : deadlock
        if (cycle_made(table_id, page_id, key, trx_id, lock_mode)) {
            return NULL;
        }

        //printf("entry->head != NULL\n");
        last->next = lock;
        lock->prev = last;
        lock->next = NULL;
        entry->tail = lock;
        lock->sentinel = entry;
        lock->record_id = key;
        lock->lock_mode = lock_mode;
        if (pthread_cond_init(&(lock->condition), NULL)) {
            //printf("in lock_acquire pthread_cond_init nonzero return value");
            return NULL;
        }
        if (pthread_cond_wait(&(lock->condition), &lock_table_latch)) {
            //printf("in lock_acquire pthread_cond_wait nonzero return value");
            return NULL;
        }
    }
    // no predecessor
    else { 
        lock->prev = NULL;
        lock->next = NULL;
        lock->sentinel = entry;
        lock->lock_mode = lock_mode;
        if (pthread_cond_init(&(lock->condition), NULL)) {
            //printf("in lock_acquire pthread_cond_init nonzero return value");
            return NULL;
        }
        entry->head = entry->tail = lock;
    }

    if (pthread_mutex_unlock(&lock_table_latch)) {
        //printf("in lock_acquire pthread_mutex_unlock nonzero return value");
        return NULL;
    }
    //printf("returning lock\n");
    return lock;
}

// • Remove the lock_obj from the lock list.
// • If there is a successor’s lock waiting for the thread releasing the lock, wake up the successor.
// • If success, return 0. Otherwise, return non-zero value.
int lock_release(lock_t* lock_obj) {
    //printf("%s\n", __func__);
    if (pthread_mutex_lock(&lock_table_latch)) {
        //printf("in lock_release pthread_mutex_lock nonzero return value");
        return 1;
    }

    lock_entry_t* entry = lock_obj->sentinel;
    bool is_first = 0;

    // find the first lock of the record -> flag
    for (lock_t* l = entry->head; l; l = l->next) {
        if (l->record_id == lock_obj->record_id) {
            if (l == lock_obj) is_first = 1;
            break;
        }
    }

    // remove from list
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


    // case : releasing first lock of the record
    if (is_first) {
        for (lock_t* l = entry->head; l; l = l->next) {
            if (l->record_id != lock_obj->record_id) continue;

            // case : first lock is SHARED
            if (l->lock_mode == SHARED) {
                int cnt = 0;
                pthread_cond_signal(&l->condition);
                for (; l; l = l->next) {
                    if (l->record_id != lock_obj->record_id) continue;
                    // second~  lock is SHARED
                    if (l->lock_mode == SHARED) {
                        ++cnt;
                        pthread_cond_signal(&l->condition);
                    }
                    // second~ lock is EXCLUSIVE
                    else {
                        // case : (first)S1 -> (second)X1 
                        // acquire X1 too
                        if (l->trx_id == lock_obj->trx_id && cnt == 0) {
                            pthread_cond_signal(&l->condition);
                        }
                        break;
                    }
                }
                break;
            }
            // case : first lock is EXCLUSIVE
            else {
                pthread_cond_signal(&l->condition);
                break;
            }
        }
    }
    // case : not first -> do not need to signal next locks


    free(lock_obj);
    if (pthread_mutex_unlock(&lock_table_latch)) {
        //printf("in lock_release pthread_mutex_unlock nonzero return value");
        return 1;
    }
    return 0;
}

bool cycle_made(table_t table_id, pagenum_t pn, key__t key, int trx_id, int lock_mode) {
    //printf("%s\n", __func__);
    std::queue<lock_t*> q;
    auto& entry = lock_table[{table_id, pn}];
    for (lock_t* tail = entry.tail; tail; tail = tail->prev) {
        if (tail->record_id == key) {
            q.push(tail);
            break;
        }
    }
    //printf("q.size = %d\n", q.size());
    for (; !q.empty();) {
        lock_t* fr = q.front();
        q.pop();
        if (fr->trx_id == trx_id) {
            printf("deadlock\n");
            return 1;
        }
        for (lock_t* l = fr->prev; l; l = l->prev) {
            if (l->record_id == fr->record_id) q.push(l);
        }
        if (fr->trx_next) q.push(fr->trx_next);
    }
    //printf("returning 0\n");
    return 0;
}