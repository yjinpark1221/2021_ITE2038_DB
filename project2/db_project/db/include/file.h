#ifndef __DB_FILE_H__
#define __DB_FILE_H__
#define INITIAL_DB_FILE_SIZE (10 * 1024 * 1024)  // 10 MiB
#define PAGE_SIZE (4 * 1024)                     // 4 KiB
#include <stdint.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <vector>
#include <stdio.h>
#include <stdint.h>
#include <vector>
#include <string>
#include <cstring>
typedef uint64_t pagenum_t;
typedef int64_t table_t;
typedef int64_t key__t;
typedef uint16_t u16_t;
typedef uint32_t u32_t;
struct page_t;
// These definitions are not requirements.
// You may build your own way to handle the constants.
#ifndef __OPENEDFDS__
#define __OPENEDFDS__
//std::vector<table_t> openedFds;
#endif
// 1. int64_t file_open_table_file (const char * pathname)
// • Open the table file.
// • It opens an existing table file using ‘pathname’ or create a new file if absent.
// • If a new file needs to be created, the default file size should be 10 MiB.
// • Then it returns the table id of the opened table file.
// • All other 5 commands below should be handled after open table file.
// Open existing database file or create one if it doesn't exist
table_t file_open_database_file(const char* pathname);

// 2. uint64_t file_alloc_page (int64_t table_id);
// • Allocate a page.
// • It returns a new page # from the free page list.
// • If the free page list is empty, then it should grow the table file and return a free page #.
// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page(table_t table_id);

// 3. void file_free_page (int64_t table_id, uint64_t page_number);
// • Free a page.
// • It informs the disk space manager of returning the page with ‘table id and page_number’ for freeing it to the
// free page list.
// Free an on-disk page to the free page list
void file_free_page(table_t table_id, pagenum_t pagenum);

// 4. void file_read_page (int64_t table_id, uint64_t page_number, char * dest);
// • Read a page.
// • It fetches the disk page corresponding to ‘table id and page_number’ to the in-memory buffer (i.e., ‘dest’).
// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(table_t table_id, pagenum_t pagenum, page_t* dest);

// 5. void file_write_page (int64_t table_id, uint64_t page_number, const char * src);
// • Write a page.
// • It writes the in-memory page content in the buffer (i.e., ‘src’) to the disk page pointed by ‘table id and
// page_number’.
// Write an in-memory page(src) to the on-disk page
void file_write_page(table_t table_id, pagenum_t pagenum, const page_t* src);

// 6. void file_close_table_files();
// • Close all table files.
// Close the database file
void file_close_database_file();

/// below are the APIs for testing ///

// gets file size from the header page
pagenum_t file_get_size(table_t table_id);

// checks if a page number is in the vector(free page list)
bool inVec(std::vector<pagenum_t> vec, pagenum_t page);

// gets the free page list as a vector
std::vector<pagenum_t> file_get_free_list(table_t table_id);


#endif  // __DB_FILE_H__