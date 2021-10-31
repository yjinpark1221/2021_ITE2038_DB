#ifndef MAINTEST
#include "buffer.h"
#endif

#include <cassert>

std::map<table_page_t, control_t*> tp2control;
control_t* control;
page_t* cache;
control_t* hcontrol;
page_t* hcache;
int num_buf;
int cur_buf;

static control_t head, tail;

int buf_init(int nb) {
    head.next = &tail;
    tail.prev = &head;
    head.prev = NULL;
    tail.next = NULL;

    num_buf = nb;
    cache = (page_t*) malloc(sizeof(page_t) * num_buf);
    control = (control_t*) malloc(sizeof(control_t) * num_buf);
    hcontrol = (control_t*) malloc(sizeof(control_t) * 20);
    hcache = (page_t*) malloc(sizeof(page_t) * 20);
    if (cache == NULL || control == NULL || hcontrol == NULL || hcache == NULL) {
        perror("in buf_init malloc error");
        exit(0);
    }
    return 0;
}

table_t buf_open_table_file(const char* pathname) {
    table_t table_id = file_open_table_file(pathname);
    page_t hp;
    file_read_page(table_id, 0, &hp);
    hcache[openedFds.size() - 1] = hp;
    control_t hc(table_id, 0, hcache + (int)(openedFds.size() - 1));
    hcontrol[openedFds.size() - 1] = hc;
    return table_id;
}

void buf_close_table_file() {
    // flush headers 
    for (int i = 0; i < openedFds.size(); ++i) {
        flush(hcontrol + i);
    }

    // flush frames
    for (int i = 0; i < cur_buf; ++i) {
        flush(control + i);
    }
    free(cache);
    free(hcache);
    free(control);
    free(hcontrol);
    // close files
    file_close_table_file();
}

control_t* buf_alloc_page(table_t table_id) {
    flush_header(table_id);
    pagenum_t pn = file_alloc_page(table_id);
    page_t page;
    read_header(table_id);
    control_t* ctrl = buf_read_page(table_id, pn, &page);
    return ctrl;
}

void buf_free_page(table_t table_id, pagenum_t pagenum) {
    flush_header(table_id);
    file_free_page(table_id, pagenum);
    read_header(table_id);
}

control_t* buf_read_page(table_t table_id, pagenum_t pagenum, page_t* dest, bool pin) {
    // reading header page -> must be in hcontrol block
    if (pagenum == 0) {
        for (int i = 0; i < openedFds.size(); ++i) {
            control_t* hc = hcontrol + i;
            if (hc->tp.first == table_id) {
                memcpy(dest, hc->frame, PAGE_SIZE);
                return hc;
            }
        }
        perror("in buf_read_page header page not in hcontrol");
        exit(0);
    }

    auto iter = tp2control.find({table_id, pagenum});
    control_t* ct;

    // not in cache
    if (iter == tp2control.end()) {
        // LRU flush
        ct = flush_LRU(table_id, pagenum);
        tp2control[{table_id, pagenum}] = ct;
        move_to_tail(ct);
        // disk to buf
        file_read_page(table_id, pagenum, ct->frame);
    }
    // in cache
    else {
        ct = iter->second;        
        move_to_tail(ct);
    }

    // buf to index
    memcpy(dest, ct->frame, PAGE_SIZE);
    if (pin) ct->pin_count++;
    if (pagenum == 0 && ct->pin_count) printf("why?\n");
    // TODO : unpin in index layer

    // move_to_tail(ct);
    
    return ct;
}

void buf_write_page(table_t table_id, pagenum_t pagenum, const page_t* src) {
    if (pagenum == 0) {
        for (int i = 0; i < openedFds.size(); ++i) {
            control_t* hc = hcontrol + i;
            if (hc->tp.first == table_id) {
                memcpy(hc->frame, src, PAGE_SIZE);
                hc->is_dirty = 1;
                return;
            }
        }
    }
    
    auto iter = tp2control.find({table_id, pagenum});
    control_t* ct;

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
    return;
}

control_t* flush_LRU(table_t table_id, pagenum_t pagenum) {
    // case : buffer not full 
    // -> no need to flush 
    // -> control[end] = {table_id, pagenum}
    if (cur_buf < num_buf) {
        // printf("cur_buf %d\n", cur_buf);
        control[cur_buf].tp = {table_id, pagenum};
        control[cur_buf].is_dirty = 0;
        control[cur_buf].pin_count = 0;
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

    // case : full buffer
    // TODO : get control block(LRU clock function)

    control_t* ct;
    int count = num_buf;
    for (ct = head.next; ct != &tail && ct->pin_count && num_buf--; ct = ct->next);    
    // case : all pinned(small buffer)
    // WARNING : needs to be reconsidered if multi-thread
    if (ct == &tail || ct->pin_count) {
        perror("small buffer or not unpinned error");
        exit(0);
        return NULL;
    }

    // case : found unpinned frame
    auto iter = tp2control.find(ct->tp);
    if (iter == tp2control.end()) {
        // for (auto p : tp2control) {
        //     printf("%d %d \n", p.first.first, p.first.second);
        // }
        // printf("ct->tp %d %d\n", ct->tp.first, ct->tp.second);
        perror("in flush_LRU iter not found");
        exit(0);
    }
    tp2control.erase(iter);
    flush(ct);

    ct->tp = {table_id, pagenum};
    tp2control[{table_id, pagenum}] = ct;
    ct->is_dirty = 0;
    ct->pin_count = 0;

    move_to_tail(ct);
    return ct;
}

// This function writes the frame to disk if it is dirty
// called flushing the head.next
void flush(control_t* ctrl) {
    if (ctrl->pin_count) {
        if (ctrl - hcontrol >= 0 && ctrl - hcontrol < 20) printf("header\n");
        else {
            printf("%d %d %d\n", ctrl->pin_count, ctrl - control, ctrl - hcontrol);
            perror("flushing pinned page");
            exit(0);
        }
    }
    if (ctrl->is_dirty) {
        file_write_page(ctrl->tp.first, ctrl->tp.second, ctrl->frame);
        ctrl->is_dirty = 0;
    }
}

// This function flushes the header page of the table_id 
void flush_header(table_t table_id) {
    for (int i = 0; i < openedFds.size(); ++i) {
        control_t* hc = hcontrol + i;
        if (hc->tp.first == 0) return;
        if (hc->tp.first == table_id) {
            file_write_page(table_id, 0, hc->frame);
            return;
        }
    }
}

// This function flushes the header page of the table_id 
void read_header(table_t table_id) {
    for (int i = 0; i < openedFds.size(); ++i) {
        control_t* hc = hcontrol + i;
        if (hc->tp.first == 0) return;
        if (hc->tp.first == table_id) {
            file_read_page(table_id, 0, hc->frame);
        }
    }
}

// This function moves ct to the tail
// called when referenced
void move_to_tail(control_t* ct) {
    control_t* prev = ct->prev, *next = ct->next, *last = tail.prev;
    if (last == ct) return;
    if (prev)prev->next = next;
    if (next)next->prev = prev;

    // if (tail.prev == &head) puts("!!!!!!!!!!!!!!!!!!!!!!!!!!");
    // printf("%p %p %p %p %p %p\n", &head, &tail, prev, next, last, ct);
    last->next = ct;
    ct->prev = last;

    ct->next = &tail;
    tail.prev = ct;

    // printf("move to tail done\n");
    // if (head.next == &tail) puts("??????????????????????????");
    // for (auto p = head.next; p != &tail;p = p->next) {
    //     printf("%d %d\n", p->tp.first, p->tp.second);
    // }
}

