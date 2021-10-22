#include "../include/buffer.h"

std::map<table_page_t, control_t*> tp2control;
control_t* control;
page_t* cache;
control_t* hcontrol;
page_t* hcache;
int num_buf;
int cur_buf;

int buf_init(int nb) {
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

pagenum_t buf_alloc_page(table_t table_id) {
    flush_header(table_id);
    pagenum_t pn = file_alloc_page(table_id);
    page_t page;
    buf_read_page(table_id, pn, &page);
    read_header(table_id);
    return pn;
}

void buf_free_page(table_t table_id, pagenum_t pagenum) {
    flush_header(table_id);
    file_free_page(table_id, pagenum);
    read_header(table_id);
}

control_t* buf_read_page(table_t table_id, pagenum_t pagenum, page_t* dest) {
    // reading header page -> must be in hcontrol block
    if (pagenum == 0) {
        for (int i = 0; i < openedFds.size(); ++i) {
            control_t* hc = hcontrol + i;
            if (hc->tp.first == table_id) {
                memcpy(dest, hc->frame, PAGE_SIZE);
                hc->pin_count++;
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

control_t* clock() {
    static int hand = 0;
    control_t* ret = NULL;
    while (ret == NULL) {
        control_t* cur = control + hand;
        if (cur->pin_count == 0 && cur->ref == 0) {
            if (cur->is_dirty) {
                flush(cur);
            }
            file_read_page()
        }
        
        // case : pinned
        if (control[hand].pin_count) {
            ++hand;
            continue;
        }

        // case : ref bit set
        else if (control[hand].ref) {
            control[hand].ref = 0;
            ++hand;
            continue;
        }

        // case : unpinned and ref bit unset
        // replace 

    }

    hand = (hand + 1) % num_buf;
    return ret;
}

control_t* flush_LRU(table_t table_id, pagenum_t pagenum) {
    
    // case : buffer not full 
    // -> no need to flush 
    // -> control[end] = {table_id, pagenum}
    if (cur_buf < num_buf) {
        control[cur_buf].tp = {table_id, pagenum};
        ++cur_buf;
        return control + cur_buf;
    }

    // case : full buffer
    // TODO : get control block(LRU clock function)

    control_t* ct = clock();
    flush(ct);
    ct->tp = {table_id, pagenum};
    ct->is_dirty = 0;
    ct->pin_count = 0;
    return ct;
}

void flush(control_t* pc) {
    file_write_page(pc->tp.first, pc->tp.second, pc->frame);
}

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

void read_header(table_t table_id) {
    for (int i = 0; i < openedFds.size(); ++i) {
        control_t* hc = hcontrol + i;
        if (hc->tp.first == 0) return;
        if (hc->tp.first == table_id) {
            file_read_page(table_id, 0, hc->frame);
        }
    }
}