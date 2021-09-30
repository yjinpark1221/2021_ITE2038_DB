#ifndef __DB_FILE_H__
#define __DB_FILE_H__

#include <stdint.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <vector>
#include <stdio.h>

// These definitions are not requirements.
// You may build your own way to handle the constants.
#define INITIAL_DB_FILE_SIZE (10 * 1024 * 1024)  // 10 MiB
#define PAGE_SIZE (4 * 1024)                     // 4 KiB

typedef uint64_t pagenum_t;

struct page_t {
  // in-memory page structure
    char a[PAGE_SIZE];
};


// Open existing database file or create one if it doesn't exist
int file_open_database_file(const char* pathname);

// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page(int fd);

// Free an on-disk page to the free page list
void file_free_page(int fd, pagenum_t pagenum);

// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(int fd, pagenum_t pagenum, page_t* dest);

// Write an in-memory page(src) to the on-disk page
void file_write_page(int fd, pagenum_t pagenum, const page_t* src);

// Close the database file
void file_close_database_file();





pagenum_t file_get_size(int fd);
bool inVec(std::vector<pagenum_t> vec, pagenum_t page);
std::vector<pagenum_t> file_get_free_list(int fd);


#endif  // __DB_FILE_H__