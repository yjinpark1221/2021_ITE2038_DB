#ifndef MAINTEST
#include "txn.h"
#endif
#include <stack>

#include <cassert>
#include <set>
pthread_mutex_t trx_table_latch;
pthread_mutex_t lock_table_latch;
std::unordered_map<int, trx_entry_t> trx_table;
std::unordered_map<std::pair<table_t, key__t>, lock_entry_t, hash_t> lock_table;
std::set<int> aborted_trx;
bool trx_init_table() {
    // printf("%s\n", __func__);
    return pthread_mutex_init(&trx_table_latch, NULL);
}

// • Allocate a transaction structure and initialize it.
// • Return a unique transaction id (>= 1) if success, otherwise return 0.
// • Note that the transaction id should be unique for each transaction; that is, you need to allocate a
// transaction id holding a mutex.
int trx_begin(void) {
    // printf("%s\n", __func__);
    // printf("%s trylock trx\n", __func__);
    if (pthread_mutex_lock(&trx_table_latch)) {
        // printf("in trx_begin pthread_mutex_lock\n");
        return 0;
    }
    // printf("%s lock trx\n", __func__);
    static int transaction_id = 1;
    // printf("[THREAD %d] trx_begin latch lock\n", transaction_id);
    trx_entry_t entry(transaction_id);
    trx_table[transaction_id] = entry;
    // printf("[THREAD %d] trx_begin latch unlock\n", transaction_id);
    int ret = transaction_id++;
    // printf("%s unlock trx\n", __func__);
    if (pthread_mutex_unlock(&trx_table_latch)) {
        // printf("in trx_begin pthread_mutex_unlock\n");
        return 0;
    }
    return ret;
}

// • Clean up the transaction with the given trx_id (transaction id) and its related information that has
// been used in your lock manager. (Shrinking phase of strict 2PL)
// • Return the completed transaction id if success, otherwise return 0.
int trx_commit(int trx_id) {
    // printf("%d %s trylock trx\n", trx_id, __func__);
    if (pthread_mutex_lock(&trx_table_latch)) {
        // printf("in trx_commit pthread_mutex_lock\n");
        return 0;
    }
    // printf("%d %s lock trx\n", trx_id, __func__);
    // printf("[THREAD %d] trx_commit latch lock\n", trx_id);
    if (trx_release_locks(trx_id)) {
        // printf("in trx_commit trx_release_locks\n");
        return 0;
    }
    // printf("[THREAD %d] trx_commit latch unlock\n", trx_id);
    // printf("%d %s unlock trx\n", trx_id, __func__);
    if (pthread_mutex_unlock(&trx_table_latch)) {
        // printf("in trx_commit pthread_mutex_unlock\n");
        return 0;
    }
    return trx_id;
}
int trx_abort(int trx_id) {
    // if (pthread_mutex_lock(&trx_table_latch)) {
    //     // printf("in trx_commit pthread_mutex_lock\n");
    //     return 0;
    // }
    // printf("[THREAD %d] trx_abort latch lock\n", trx_id);
    // printf("%d %s trylock trx\n", trx_id, __func__);
    pthread_mutex_lock(&trx_table_latch);
    // printf("%d %s lock trx\n", trx_id, __func__);
    trx_undo(trx_id);
    trx_release_locks(trx_id);
    // printf("[THREAD %d] trx_abort latch unlock\n", trx_id);
    // printf("%d %s unlock trx\n", trx_id, __func__);
    if (pthread_mutex_unlock(&trx_table_latch)) {
        // printf("in trx_commit pthread_mutex_unlock\n");
        return 0;
    }
    return 1;
}
int trx_undo(int trx_id) {
    // printf("[THREAD %d] %s\n", trx_id, __func__);
    auto& entry = trx_table[trx_id];
    // printf("entry log size %d\n", entry.old_vals.size());
    for (auto iter = entry.logs.rbegin(); iter != entry.logs.rend(); ++iter) {
        table_t table_id = iter->table_id;
        mslot_t slot = iter->slot;
        pagenum_t pn = iter->pn;
        std::string value = iter->value;
        ctrl_t* ctrl = buf_read_page(table_id, pn);
        page_t page = *(ctrl->frame);
        for (int i = 0; i < slot.size; ++i) {
            page.a[slot.offset + i] = value[i];
        }
        buf_write_page(&page, ctrl);
        pthread_mutex_unlock(&ctrl->mutex);
    }
    return 0;
}

int trx_release_locks(int trx_id) {
    // printf("%s\n", __func__);
    lock_t* lock = trx_table[trx_id].head;
    if (lock == 0) return 0;
    lock_t* next = lock->trx_next;
    pthread_mutex_lock(&lock_table_latch);
    for (; lock; lock = next) {
        // printf("lock_t* l trx_id %d, lock_mode %d, record_id %d\n", lock->trx_id, lock->lock_mode, lock->record_id);
        next = lock->trx_next;
        if (lock_release(lock, 1)) {
            // printf("lock release 1\n");
            return 1;
        }
    }
    pthread_mutex_unlock(&lock_table_latch);
    trx_table.erase(trx_id);
    return 0;
}


// • Initialize any data structures required for implementing lock table, such as hash table, lock table latch, etc.
// • If success, return 0. Otherwise, return non-zero value.
int init_lock_table() {
    // printf("%s\n", __func__);
    return pthread_mutex_init(&lock_table_latch, NULL);
}

// • Allocate and append a new lock object to the lock list of the record having the key.
// • If there is a predecessor’s lock object in the lock list, sleep until the predecessor to release its lock.
// • If there is no predecessor’s lock object, return the address of the new lock object.
// • If an error occurs, return NULL.
// • lock_mode: 0 (SHARED) or 1 (EXCLUSIVE)
lock_t* lock_acquire(table_t table_id, pagenum_t page_id, key__t key, int trx_id, int lock_mode) {
    // printf("%d %s trylock trx\n", trx_id, __func__);
    pthread_mutex_lock(&trx_table_latch);
    // printf("%d %s lock trx\n", trx_id, __func__);
    // printf("%s 1\n", __func__);
    pthread_mutex_lock(&lock_table_latch);
    // printf("%s 2\n", __func__);

    auto tmp = lock_table.find({table_id, page_id});
    // not in lock table -> insert empty list into table
    if (tmp == lock_table.end()) {
        lock_entry_t entry(table_id, page_id, NULL, NULL);
        lock_table[{table_id, page_id}] = entry;
    }

    lock_entry_t* entry = &(lock_table[{table_id, page_id}]);
    bool has_slock = 0;
    bool has_xlock = 0;
    // case : s lock
    if (lock_mode == SHARED) {
        for (lock_t* l = entry->head; l; l = l->next) {
            // printf("lock_t* l trx_id %d, lock_mode %d, record_id %d\n", l->trx_id, l->lock_mode, l->record_id);
            if (l->record_id == key && l->trx_id != trx_id) {
                if (l->lock_mode == SHARED) has_slock = 1;
                else has_xlock = 1;
            }
            if (l->record_id != key || l->trx_id != trx_id) continue;
            // case : s lock or x lock found
            // do not need to acquire new lock
            else {
                // printf("%d %s unlock trx\n", trx_id, __func__);
                pthread_mutex_unlock(&trx_table_latch);
                // printf("this txn(%d) has s or x lock\n", trx_id);
                pthread_mutex_unlock(&lock_table_latch);
                return l;
            }
        }
    }
    // case : x lock
    else {
        for (lock_t* l = entry->head; l; l = l->next) {
            // printf("lock_t* l trx_id %d, lock_mode %d, record_id %d\n", l->trx_id, l->lock_mode, l->record_id);
            if (l->record_id == key && l->trx_id != trx_id) {
                if (l->lock_mode == SHARED) has_slock = 1;
                else has_xlock = 1;
            }
            if (l->record_id != key || l->trx_id != trx_id) continue;

            // case : s lock found
            if (l->lock_mode == SHARED) {
                // printf("this txn(%d) has s lock\n", trx_id);

                // check if it is the only lock of the record
                bool is_last = 1;
                for (lock_t* ll = entry->head; ll; ll = ll->next) {
                    if (ll->record_id == key && ll != l) {
                        // printf("(not last!) lock_t* ll trx_id %d, lock_mode %d, record_id %d\n", l->trx_id, l->lock_mode, l->record_id);
                        is_last = 0;
                        break;
                    }
                }
                // if last-> do not wait and upgrade the lock to EXCLUSIVE
                if (is_last) {
                    // printf("the only lock -> upgrade\n");
                    l->lock_mode = EXCLUSIVE;
                    // printf("%d %s unlock trx\n", trx_id, __func__);
                    pthread_mutex_unlock(&trx_table_latch);
                    pthread_mutex_unlock(&lock_table_latch);
                    return l;
                }
                // if not last -> check dead lock and wait
                // printf("not the only lock-> check deadlock and wait\n");
                // case : deadlock
                lock_t* lock = (lock_t*)malloc(sizeof(lock_t));
                lock->sentinel = entry;
                lock->record_id = key;
                lock->trx_id = trx_id;
                lock->lock_mode = lock_mode;

                add_edge(lock);
                push_back_lock(lock);
                if (bfs(table_id, page_id, key, trx_id, lock_mode)) {
                    // printf("%d %s unlock trx\n", trx_id, __func__);
                    pthread_mutex_unlock(&trx_table_latch);
                    pthread_mutex_unlock(&lock_table_latch);
                    return NULL;
                }

                if (pthread_cond_init(&(lock->condition), NULL)) {
                    // printf("in lock_acquire pthread_cond_init nonzero return value");
                    return NULL;
                }
                // printf("wait\n");
                pthread_mutex_unlock(&trx_table_latch);
                if (pthread_cond_wait(&(lock->condition), &lock_table_latch)) {
                    // printf("in lock_acquire pthread_cond_wait nonzero return value");
                    return NULL;
                }
                // printf("%d %s unlock trx\n", trx_id, __func__);
                pthread_mutex_unlock(&lock_table_latch);
                return lock;
            }
            // case : x lock found
            // do not need to acquire new lock
            else {
                // printf("%d %s unlock trx\n", trx_id, __func__);
                pthread_mutex_unlock(&trx_table_latch);
                pthread_mutex_unlock(&lock_table_latch);
                return l;
            }
        }
    }
    lock_t* lock = (lock_t*)malloc(sizeof(lock_t));
    lock->sentinel = entry;
    lock->record_id = key;
    lock->lock_mode = lock_mode;
    lock->trx_id = trx_id;

    // S1 -> S2 -> (S3)
    if (lock_mode == SHARED && has_slock && !has_xlock) {
        // printf("S1S2S3\n");
        // 락 매달고 안기다리고 리턴
        push_back_lock(lock);

        if (pthread_cond_init(&(lock->condition), NULL)) {
            // printf("in lock_acquire pthread_cond_init nonzero return value");
            return NULL;
        }
    }
    // sleep until the predecessor releases its lock
    else if (has_slock || has_xlock) {
        // case : deadlock
        add_edge(lock);
        push_back_lock(lock);
        if (bfs(table_id, page_id, key, trx_id, lock_mode)) {
            // printf("%d %s unlock trx\n", trx_id, __func__);
            pthread_mutex_unlock(&trx_table_latch);
            pthread_mutex_unlock(&lock_table_latch);
            return NULL;
        }

       if (pthread_cond_init(&(lock->condition), NULL)) {
            // printf("in lock_acquire pthread_cond_init nonzero return value");
            return NULL;
        }
        // printf("wait\n");
        pthread_mutex_unlock(&trx_table_latch);
        if (pthread_cond_wait(&(lock->condition), &lock_table_latch)) {
            // printf("in lock_acquire pthread_cond_wait nonzero return value");
            return NULL;
        }
        pthread_mutex_unlock(&lock_table_latch);
        // printf("returning lock\n");
        return lock;
    }
    // no predecessor for the record!!!!!
    else {
        push_back_lock(lock);

        if (pthread_cond_init(&(lock->condition), NULL)) {
            // printf("in lock_acquire pthread_cond_init nonzero return value");
            return NULL;
        }
    }

    // printf("%d %s unlock trx\n", trx_id, __func__);
    pthread_mutex_unlock(&trx_table_latch);
    pthread_mutex_unlock(&lock_table_latch);
    // printf("returning lock\n");
    return lock;
}

// • Remove the lock_obj from the lock list.
// • If there is a successor’s lock waiting for the thread releasing the lock, wake up the successor.
// • If success, return 0. Otherwise, return non-zero value.
int lock_release(lock_t* lock_obj, int mode) {
    // printf("%s\n", __func__);
    if (mode == 0) {
        if (pthread_mutex_lock(&lock_table_latch)) {
            // printf("in lock_release pthread_mutex_lock nonzero return value");
            return 1;
        }
    }
    // printf("lock_latch_caught\n");
    lock_entry_t* entry = lock_obj->sentinel;
    assert(entry);
    int cnt = 0;

    for (lock_t* l = entry->head; l; l = l->next) {
        if (l->record_id == lock_obj->record_id) {
            ++cnt;
            if (l == lock_obj) {
                break;
            }
        }
    }

    if (entry->head == lock_obj) {
        entry->head = lock_obj->next;
    }
    else {
        assert(lock_obj->prev);
        lock_obj->prev->next = lock_obj->next;
    }

    if (entry->tail == lock_obj) {
        entry->tail = lock_obj->prev;
    }
    else {
        assert(lock_obj->next);
        lock_obj->next->prev = lock_obj->prev;
    }
    // printf("removed from list\n");

    // case : releasing first lock of the record
    if (cnt == 0 || cnt == 1) {
        // printf("is first\n");
        for (lock_t* l = entry->head; l; l = l->next) {
            // printf("lock_t* l trx_id %d, lock_mode %d, record_id %d\n", l->trx_id, l->lock_mode, l->record_id);
            if (l->record_id != lock_obj->record_id) continue;

            // case : first lock is SHARED
            if (l->lock_mode == SHARED) {
                int cnt = 0;
                // printf("signal trx_id %d, lock_mode %d, record_id %d\n", l->trx_id, l->lock_mode, l->record_id);
                pthread_cond_signal(&l->condition);
                for (lock_t* ll = l->next; ll; ll = ll->next) {
                // printf("lock_t* ll trx_id %d, lock_mode %d, record_id %d\n", ll->trx_id, ll->lock_mode, ll->record_id);
                    if (ll->record_id != lock_obj->record_id) continue;
                    // second~  lock is SHARED
                    if (ll->lock_mode == SHARED) {
                        ++cnt;
                        // printf("signal trx_id %d, lock_mode %d, record_id %d\n", ll->trx_id, ll->lock_mode, ll->record_id);
                        pthread_cond_signal(&ll->condition);
                    }
                    // second~ lock is EXCLUSIVE
                    else {
                        // case : (first)S1 -> (second)X1
                        // acquire X1 too
                        if (ll->trx_id == l->trx_id && cnt == 0) {
                            // printf("second lock is exclusive\nsignal trx_id %d, lock_mode %d, record_id %d\n", l->trx_id, l->lock_mode, l->record_id);
                            pthread_cond_signal(&ll->condition);
                        }
                        break;
                    }
                }
                break;
            }
            // case : first lock is EXCLUSIVE
            else {
                // printf("signal trx_id %d, lock_mode %d, record_id %d\n", l->trx_id, l->lock_mode, l->record_id);
                pthread_cond_signal(&l->condition);
                break;
            }
        }
    }
    // case : not first -> do not need to signal next locks

    // printf("free lock\n");
    free(lock_obj);
    if (mode == 0) {
        if (pthread_mutex_unlock(&lock_table_latch)) {
            // printf("in lock_release pthread_mutex_unlock nonzero return value");
            return 1;
        }
    }
    return 0;
}

bool cycle_made(table_t table_id, pagenum_t pn, key__t key, int trx_id, int lock_mode) {
    std::set<lock_t*> st;
    std::queue<lock_t*> q;
    auto& entry = lock_table[{table_id, pn}];
    for (lock_t* tail = entry.tail; tail; tail = tail->prev) {
        if (tail->record_id == key && st.find(tail) == st.end() && tail->trx_id != trx_id) {
            st.insert(tail);
            q.push(tail);
            break;
        }
    }
    for (; !q.empty();) {
        lock_t* fr = q.front();
        q.pop();
        if (fr->trx_id == trx_id) {
            return 1;
        }
        for (lock_t* l = fr->prev; l; l = l->prev) {
            if (l->record_id == fr->record_id && st.find(l) == st.end() && l->trx_id != fr->trx_id) {
                st.insert(l);
                q.push(l);
            }
        }
        if (fr->trx_next && st.find(fr->trx_next) == st.end()) {
            st.insert(fr->trx_next);
            q.push(fr->trx_next);
        }
    }
    return 0;
}

bool bfs(table_t table_id, pagenum_t pn, key__t key, int trx_id, int lock_mode) {
    // printf("%s %d\n", __func__, trx_id);
    std::queue<std::pair<int, int> > q;
    std::set<int> visited;
    visited.insert(trx_id);

    for (auto edge: trx_table[trx_id].wait_edges) {
        q.push({trx_id, edge});
    }

    for (; !q.empty();) {
        // printf("%d q.size() %d\n", trx_id, q.size());
        auto fr = q.front();
        q.pop();
        // a is waiting for **b**
        int a = fr.first;
        int b = fr.second;
        if (b == trx_id) {
            // printf("return 1\n");
            return 1;
        }
        // trx end -> edge remove!
        // when b is aborted
        if (trx_table.find(b) == trx_table.end()) {
            trx_table[a].wait_edges.erase(b);
            continue;
        }
        if (visited.find(b) != visited.end()) continue;
        visited.insert(b);
        for (auto edge : trx_table[b].wait_edges) {
            q.push({b, edge});
        }
    }
    // printf("return 0\n");
    return 0;
}

void add_edge(lock_t* lock) {
    lock_entry_t* lentry = lock->sentinel;
    trx_entry_t* tentry = &(trx_table[lock->trx_id]);

    if (lock->lock_mode == SHARED) {
        lock_t* l = lentry->tail;
        // printf("lock_t* l trx_id %d, lock_mode %d, record_id %d\n", lock->trx_id, lock->lock_mode, lock->record_id);
        for (; l && (l->record_id != lock->record_id || l->lock_mode == SHARED); l = l->prev);
        if (l) {
            tentry->wait_edges.insert(l->trx_id);
            // printf("push back lock_t* l trx_id %d, lock_mode %d, record_id %d\n", lock->trx_id, lock->lock_mode, lock->record_id);
        }
    }
    else {
        lock_t* l = lentry->tail;
        // printf("lock_t* l trx_id %d, lock_mode %d, record_id %d\n", lock->trx_id, lock->lock_mode, lock->record_id);
        for (; l; l = l->prev) {
            if (l->record_id != lock->record_id) continue;
            // printf("push back lock_t* l trx_id %d, lock_mode %d, record_id %d\n", lock->trx_id, lock->lock_mode, lock->record_id);
            tentry->wait_edges.insert(l->trx_id);
            if (l->lock_mode == EXCLUSIVE) break;
        }
    }
}
void push_back_lock(lock_t* lock) {
    // printf("%s\n", __func__);
    lock_entry_t* lentry = lock->sentinel;
    trx_entry_t* tentry = &(trx_table[lock->trx_id]);
    if (tentry->head == NULL) {
        // printf("[THREAD %d] push_back_lock head change\n", lock->trx_id);
        assert(tentry->tail == NULL);
        tentry->head = tentry->tail = lock;
        lock->trx_next = NULL;
    }
    else if (tentry->head == tentry->tail) {
        lock_t* prev = tentry->tail;
        assert(prev->trx_next == NULL);
        prev->trx_next = lock;
        lock->trx_next = NULL;
        tentry->tail = lock;
    }
    else {
        lock_t* last = tentry->tail;
        assert(last->trx_next == NULL);
        last->trx_next = lock;
        lock->trx_next = NULL;
        tentry->tail = lock;
    }
    if (lentry->head == NULL) {
        assert(lentry->tail == NULL);
        lentry->head = lentry->tail = lock;
        lock->prev = NULL;
        lock->next = NULL;
    }
    else if (lentry->head == lentry->tail) {
        lock_t* prev = lentry->tail;
        assert(prev->next == NULL);
        assert(prev->prev == NULL);

        prev->next = lock;
        lock->prev = prev;
        lock->next = NULL;

        lentry->tail = lock;
    }
    else {
        lock_t* last = lentry->tail;
        assert(last->next == NULL);
        last->next = lock;
        lock->prev = last;
        lock->next = NULL;
        lentry->tail = lock;
    }

    // printf("%s done\n", __func__);
}