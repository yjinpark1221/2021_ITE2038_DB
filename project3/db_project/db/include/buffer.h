#ifndef __BUF_H__
#define __BUF_H__
#include "file.h"


typedef std::pair<table_t, pagenum_t> table_page_t; 

struct control_t {
    table_page_t tp;
    bool is_dirty;
    page_t* frame;
    int pin_count;
    table_page_t next;
    table_page_t prev;
    control_t(table_t t, pagenum_t p, page_t* f): tp({t, p}), frame(f) {}
};


int buf_init(int nb);

table_t buf_open_table_file(const char* pathname);
void buf_close_table_file();
pagenum_t buf_alloc_page(table_t table_id);
void buf_free_page(table_t table_id, pagenum_t pagenum);
control_t* buf_read_page(table_t table_id, pagenum_t pagenum, page_t* dest);
void buf_write_page(table_t table_id, pagenum_t pagenum, const page_t* src);

void flush(control_t* pc);
control_t* flush_LRU(table_t table_id, pagenum_t pagenum);
void flush_header(table_t table_id);
void read_header(table_t table_id);


#endif  // __DB_FILE_H__