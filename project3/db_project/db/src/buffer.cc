#include "../include/buffer.h"

std::map<table_page_t, control_t*> tp2control;
control_t* control;
page_t* cache;
control_t* hcontrol;
page_t* hcache;
int num_buf;

int buf_init(int nb) {
    num_buf = nb;
    cache = (page_t*) malloc(sizeof(page_t) * num_buf);
    control = (control_t*) malloc(sizeof(control_t) * num_buf);
    hcontrol = (control_t*) malloc(sizeof(control_t) * 20);
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
    // flush header 
    for (int i = 0; i < openedFds.size(); ++i) {
        flush(hcontrol + i);
    }

    // flush buffer
    for (int i = 0; i < num_buf; ++i) {
        flush(control + i);
    }

    // close files
    file_close_table_file();
}

pagenum_t buf_alloc_page(table_t table_id) {
    flush_header(table_id);
    pagenum_t pn = file_alloc_page(table_id);
    read_header(table_id);
    return pn;
}

void buf_free_page(table_t table_id, pagenum_t pagenum) {
    flush_header(table_id);
    file_free_page(table_id, pagenum);
    read_header(table_id);
}

control_t* buf_read_page(table_t table_id, pagenum_t pagenum, page_t* dest) {
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
    ct->pin_count++;
    
    // TODO : unpin in index layer
    
    return ct;
}

void buf_write_page(table_t table_id, pagenum_t pagenum, const page_t* src) {
    auto iter = tp2control.find({table_id, pagenum});
    control_t* ct;

    // not in cache
    if (iter == tp2control.end()) {
        // LRU flush
        ct = flush_LRU(table_id, pagenum);
    }

    // in cache
    else {
        ct = iter->second;
    }
    memcpy(ct->frame, src, PAGE_SIZE);
    ct->is_dirty = 1;
    return;
}

void flush(control_t* pc) {
    file_write_page(pc->tp.first, pc->tp.second, pc->frame);
}

control_t* flush_LRU(table_t table_id, pagenum_t pagenum) {
    control_t* control;

    // TODO : control 구하기

    flush(control);
    control->tp = {table_id, pagenum};
    control->is_dirty = 0;
    control->pin_count = 0;
    return control;
}

void flush_header(table_t table_id) {
    for (int i = 0; i < openedFds.size(); ++i) {
        control_t hc = hcontrol[i];
        if (hc.tp.first == table_id) {
            file_write_page(table_id, 0, hc.frame);
            return;
        }
    }
}

void read_header(table_t table_id) {
    for (int i = 0; i < openedFds.size(); ++i) {
        control_t hc = hcontrol[i];
        if (hc.tp.first == table_id) {
            file_read_page(table_id, 0, hc.frame);
        }
    }
}