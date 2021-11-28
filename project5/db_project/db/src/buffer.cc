#ifndef MAINTEST
#include "buffer.h"
#endif

#include <cassert>

pthread_mutex_t buf_latch;
std::map<table_page_t, ctrl_t*> tp2control;
ctrl_t* control;
page_t* cache;
ctrl_t* hcontrol;
page_t* hcache;
int num_buf;
int cur_buf;

static ctrl_t head, tail;

int buf_init(int nb) {
    printf("%s\n", __func__);
    head.next = &tail;
    tail.prev = &head;
    head.prev = NULL;
    tail.next = NULL;
    
    pthread_mutex_init(&buf_latch, NULL);

    num_buf = nb;
    cache = (page_t*) malloc(sizeof(page_t) * num_buf);
    control = (ctrl_t*) malloc(sizeof(ctrl_t) * num_buf);
    hcontrol = (ctrl_t*) malloc(sizeof(ctrl_t) * 20);
    hcache = (page_t*) malloc(sizeof(page_t) * 20);
    
    if (cache == NULL || control == NULL || hcontrol == NULL || hcache == NULL) {
        perror("in buf_init malloc error");
        exit(0);
    }
    
    return 0;
}

table_t buf_open_table_file(const char* pathname) {
    printf("%s\n", __func__);
    pthread_mutex_lock(&buf_latch);
    table_t table_id = file_open_table_file(pathname);
    page_t hp;
    file_read_page(table_id, 0, &hp);
    hcache[openedFds.size() - 1] = hp;

    ctrl_t hc(table_id, 0, hcache + (int)(openedFds.size() - 1));
    hcontrol[openedFds.size() - 1] = hc;
    pthread_mutex_unlock(&buf_latch);
    return table_id;
}

void buf_close_table_file() {
    printf("%s\n", __func__);
    pthread_mutex_lock(&buf_latch);
    // flush headers 
    for (int i = 0; i < openedFds.size(); ++i) {
        flush(hcontrol + i);
    }

    // flush frames
    for (int i = 0; i < cur_buf; ++i) {
        flush(control + i);
    }
    pthread_mutex_unlock(&buf_latch);

    free(cache);
    free(hcache);
    free(control);
    free(hcontrol);
    
    // close files
    file_close_table_file();
}

ctrl_t* buf_alloc_page(table_t table_id) {
    printf("%s\n", __func__);
    pthread_mutex_lock(&buf_latch);
    flush_header(table_id);
    pagenum_t pn = file_alloc_page(table_id);
    read_header(table_id);
    pthread_mutex_unlock(&buf_latch);
    ctrl_t* ctrl = buf_read_page(table_id, pn);

    return ctrl;
}

void buf_free_page(table_t table_id, pagenum_t pagenum) {
    printf("%s\n", __func__);
    pthread_mutex_lock(&buf_latch);
    flush_header(table_id);
    file_free_page(table_id, pagenum);
    read_header(table_id);
    pthread_mutex_unlock(&buf_latch);
}

ctrl_t* buf_read_page(table_t table_id, pagenum_t pagenum) {
    printf("%s\n", __func__);
    pthread_mutex_lock(&buf_latch);
    printf("buf_latch catched\n");
    // reading header page -> must be in hcontrol block
    if (pagenum == 0) {
        printf("\theader page\n");
        for (int i = 0; i < openedFds.size(); ++i) {
            ctrl_t* hc = hcontrol + i;
            if (hc->tp.first == table_id) {
                pthread_mutex_lock(&(hc->mutex));
                pthread_mutex_unlock(&buf_latch);
                return hc;
            }
        }
        perror("in buf_read_page header page not in hcontrol");
        exit(0);
    }

    auto iter = tp2control.find({table_id, pagenum});
    ctrl_t* ct;

    // not in cache
    if (iter == tp2control.end()) {
        // LRU flush
        printf("not in cache\n");
        ct = flush_LRU(table_id, pagenum);
        tp2control[{table_id, pagenum}] = ct;
        move_to_tail(ct);
        // disk to buf
        file_read_page(table_id, pagenum, ct->frame);
    }
    // in cache
    else {
        printf("in cache\n");
        ct = iter->second;        
        move_to_tail(ct);
    }

    // page latch
    pthread_mutex_lock(&(ct->mutex));
    pthread_mutex_unlock(&buf_latch);
    printf("returning buf_read_page\n");
    return ct;
}

void buf_write_page(table_t table_id, pagenum_t pagenum, const page_t* src) {
    printf("%s\n", __func__);
    pthread_mutex_lock(&buf_latch);
    if (pagenum == 0) {
        for (int i = 0; i < openedFds.size(); ++i) {
            ctrl_t* hc = hcontrol + i;
            if (hc->tp.first == table_id) {
                memcpy(hc->frame, src, PAGE_SIZE);
                hc->is_dirty = 1;
                pthread_mutex_unlock(&buf_latch);
                return;
            }
        }
    }
    
    auto iter = tp2control.find({table_id, pagenum});
    ctrl_t* ct;

    // not in cache
    if (iter == tp2control.end()) {
        perror("in buf_write_page not in cache");
        exit(0);
    }

    // in cache
    ct = iter->second;
    memcpy(ct->frame, src, PAGE_SIZE);
    move_to_tail(ct);
    ct->is_dirty = 1;
    pthread_mutex_unlock(&buf_latch);
    return;
}

ctrl_t* flush_LRU(table_t table_id, pagenum_t pagenum) {
    printf("%s\n", __func__);
    // case : buffer not full 
    // -> no need to flush 
    // -> control[end] = {table_id, pagenum}
    if (cur_buf < num_buf) {
        // printf("cur_buf %d\n", cur_buf);
        control[cur_buf].tp = {table_id, pagenum};
        control[cur_buf].is_dirty = 0;
        control[cur_buf].frame = cache + cur_buf;
        control[cur_buf].next = NULL;
        control[cur_buf].prev = NULL;
        tp2control[{table_id, pagenum}] = control + cur_buf;
        move_to_tail(control + cur_buf);
        assert(control[cur_buf].next != NULL);
        assert(control[cur_buf].prev != NULL);
        ++cur_buf;
        return control + cur_buf - 1;
    }

    ctrl_t* ct;
    int count = num_buf;
    for (ct = head.next; ct != &tail ; ct = ct->next) {
        if (ct == &tail) ct = head.next;
        if (pthread_mutex_trylock(&(ct->mutex)) == EBUSY) continue;
        else {
            pthread_mutex_unlock(&(ct->mutex));
            break;
        }
    }   

    auto iter = tp2control.find(ct->tp);
    if (iter == tp2control.end()) {
        perror("in flush_LRU iter not found");
        exit(0);
    }
    tp2control.erase(iter);
    flush(ct);
    ct->tp = {table_id, pagenum};
    tp2control[{table_id, pagenum}] = ct;
    ct->is_dirty = 0;

    move_to_tail(ct);
    return ct;
}

// This function writes the frame to disk if it is dirty
// called flushing the head.next
void flush(ctrl_t* ctrl) {
    printf("%s\n", __func__);
    // TODO : check if mutex is unlocked
    if (ctrl->is_dirty) {
        file_write_page(ctrl->tp.first, ctrl->tp.second, ctrl->frame);
        ctrl->is_dirty = 0;
    }
}

// This function flushes the header page of the table_id 
void flush_header(table_t table_id) {
    printf("%s\n", __func__);
    for (int i = 0; i < openedFds.size(); ++i) {
        ctrl_t* hc = hcontrol + i;
        if (hc->tp.first == 0) return;
        if (hc->tp.first == table_id) {
            file_write_page(table_id, 0, hc->frame);
            return;
        }
    }
}

// This function flushes the header page of the table_id 
void read_header(table_t table_id) {
    printf("%s\n", __func__);
    for (int i = 0; i < openedFds.size(); ++i) {
        ctrl_t* hc = hcontrol + i;
        if (hc->tp.first == 0) return;
        if (hc->tp.first == table_id) {
            file_read_page(table_id, 0, hc->frame);
        }
    }
}

// This function moves ct to the tail
// called when referenced
void move_to_tail(ctrl_t* ct) {
    printf("%s\n", __func__);
    ctrl_t* prev = ct->prev, *next = ct->next, *last = tail.prev;
    if (last == ct) return;
    if (prev)prev->next = next;
    if (next)next->prev = prev;

    // if (tail.prev == &head) puts("!!!!!!!!!!!!!!!!!!!!!!!!!!");
    // printf("%p %p %p %p %p %p\n", &head, &tail, prev, next, last, ct);
    last->next = ct;
    ct->prev = last;

    ct->next = &tail;
    tail.prev = ct;
    printf("moved to tail\n");
    // printf("move to tail done\n");
    // if (head.next == &tail) puts("??????????????????????????");
    // for (auto p = head.next; p != &tail;p = p->next) {
    //     printf("%d %d\n", p->tp.first, p->tp.second);
    // }
}