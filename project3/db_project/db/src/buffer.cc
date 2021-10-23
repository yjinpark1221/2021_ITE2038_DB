#include "../include/buffer.h"

std::map<table_page_t, control_t*> tp2control;
control_t* control;
page_t* cache;
control_t* hcontrol;
page_t* hcache;
int num_buf;
int cur_buf;

control_t head, tail;

int buf_init(int nb) {
    head.next = &tail;
    tail.prev = &head;
    head.prev = NULL;
    tail.next = NULL;

    num_buf = nb;
    cache = (page_t*) malloc(sizeof(page_t) * num_buf);
    control = (control_t*) malloc(sizeof(control_t) * num_buf);
    for (int i = 0; i < num_buf; ++i) {
        control[i].tp = {0, 0};
    }
    hcontrol = (control_t*) malloc(sizeof(control_t) * 20);
    for (int i = 0; i < 20; ++i) {
        hcontrol[i].tp = {0, 0};
    }
    hcache = (page_t*) malloc(sizeof(page_t) * 20);
    if (cache == NULL || control == NULL) {
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
    for (int i = 0; i < num_buf; ++i) {
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

control_t* buf_read_page(table_t table_id, pagenum_t pagenum, page_t* dest, bool pin = 1) {
    // reading header page -> must be in hcontrol block
    if (pagenum == 0) {
        for (int i = 0; i < openedFds.size(); ++i) {
            control_t* hc = hcontrol + i;
            if (hc->tp.first == table_id) {
                memcpy(dest, hc->frame, PAGE_SIZE);
                hc->pin_count++;

                // TODO : unpin in index layer

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

        // disk to buf
        file_read_page(table_id, pagenum, ct->frame);
    }
    // in cache
    else {
        ct = iter->second;        
    }

    // buf to index
    memcpy(dest, ct->frame, PAGE_SIZE);
    if (pin) ct->pin_count++;
    // TODO : unpin in index layer

    move_to_tail(ct);
    
    return ct;
}

void buf_write_page(table_t table_id, pagenum_t pagenum, const page_t* src) {
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
        if (cur_buf == 0) {
        }
        control[cur_buf].tp = {table_id, pagenum};
        control[cur_buf].is_dirty = 0;
        control[cur_buf].pin_count = 0;
        tp2control[{table_id, pagenum}] = control + cur_buf;
        ++cur_buf;
        return control + cur_buf - 1;
    }

    // case : full buffer
    // TODO : get control block(LRU clock function)

    control_t* ct;
    control_t* first = head.next;
    int count = num_buf;
    for (ct = head.next; ct->pin_count && num_buf--; ct = head.next);

    // case : all pinned(small buffer)
    // WARNING : needs to be reconsidered if multi-thread
    
    if (ct->pin_count) {
        return NULL;
    }

    // case : found unpinned frame

    flush(ct);
    tp2control.remove(ct->tp);

    ct->tp = {table_id, pagenum};
    tp2control[{table_id, pagenum}] = ct;
    ct->is_dirty = 0;
    ct->pin_count = 0;

    return ct;
}

// This function writes the frame to disk if it is dirty
// called flushing the head.next
void flush(control_t* ctrl) {
    if (ctrl->pin_count) {
        perror("flushing pinned page");
        exit(0);
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
    control_t* prev = ct->prev, next = ct->next, last = tail.prev;
    prev->next = next;
    next->prev = prev;

    last->next = ct;
    ct->prev = last;

    ct->next = &tail;
    tail.prev = ct;
}

