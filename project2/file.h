// file.h
#define PAGE_SIZE 4096
#include <stdint.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/stat.h>
typedef uint64_t pagenum_t;
typedef char byte;
struct header_t {
    pagenum_t freePage;
    pagenum_t numPage;
};
struct free_t {
    pagenum_t nextFree;
};
struct internal_t {
    byte a[4096];
};
struct leaf_t {

};
struct page_t {
// in-memory page structure
    union {
        header_t header;
        free_t free;
        internal_t internal;
        leaf_t leaf; 
    };
};
// Open existing database file or create one if not existed.
int64_t file_open_database_file(char* path);
// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page();
// Free an on-disk page to the free page list
void file_free_page(pagenum_t pagenum);
// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(pagenum_t pagenum, page_t* dest);
// Write an in-memory page(src) to the on-disk page
void file_write_page(pagenum_t pagenum, const page_t* src);
// Stop referencing the database file
void file_close_database_file();