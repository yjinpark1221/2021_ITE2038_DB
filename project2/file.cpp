// file.c or file.cpp
#include "file.h"

// Open existing database file or create one if not existed.
// file_open_database_file <pathname> – open database file
// • It opens an existing database file using ‘pathname’ or create a new file if absent.
// • If a new file needs to be created, the default file size should be 10 MiB.
// • Then it returns the id of the database file mapped to the opened database file.
// • All other 5 commands below should be handled after open data file.
int fd;
int64_t file_open_database_file(char* path){
    fd = open(path, O_RDWR | O_SYNC);
    if (fd < 0) {
        fd = creat(path, 0644);
        byte* buf = (byte*) malloc(sizeof(pagenum_t) * 2);
        (pagenum_t*)buf[0] = 1;
        (pagenum_t*)buf[1] = 2560;
        pwrite(fd, buf, sizeof(pagenum_t) * 2, 0); // change header page;
        for (pagenum_t i = 1; i < 2560; ++i) {
            (pagenum_t*)buf[0] = (i + 1) % 2560;
            pwrite(fd, buf, sizeof(pagenum_t), i * PAGE_SIZE);
        }
        free(buf);
    }
    return fd;
}
// Allocate an on-disk page from the free page list
// file_alloc_page – allocate page
// • It returns a new page # from the free page list.
// • If the free page list is empty, then it should grow the database file and return a free page #.
pagenum_t file_alloc_page(){
    page_t header;
    file_read_page(0, &header);
    pagenum_t freePage = header.header.freePage;
    pagenum_t numPage = header.header.numPage;
    if (freePage) return freePage;
    
    // else allocate numPage pages
    byte* buf = (byte*) malloc(sizeof(pagenum_t) * 2);
    (pagenum_t*)buf[0] = numPage;
    (pagenum_t*)buf[1] = numPage * 2;
    pwrite(fd, buf, sizeof(pagenum_t) * 2, 0); // change first free page
    for (int i = 0; i < numPage - 1; ++i) {
        (pagenum_t*)buf[0] = (numPage + i + 1) % (2 * numPage);
        pwrite(fd, buf, sizeof(pagenum_t), (numPage + i) * PAGE_SIZE)
    }
    // add free pages : numPage ~ (2 * numPage - 1)
    return numPage;
    
    
    // if (freePage == 0) //modified
    // pagenum_t fs = lseek(fd, 0, SEEK_END);              // numPage * PAGE_SIZE
    // byte* buf = (byte*)malloc(fs * sizeof(byte) * 2);
    // read(fd, buf, fs);
    // /*
    // may be changed to page size reading
    // and read header page only
    // */
    // pagenum_t freePage = *(pagenum_t*)buf;
    // if (freePage == 0) {
    //     // allocate
    //     pagenum_t numPage = *((pagenum_t*)buf + 1);     // numPage before allocation
    //     *(pagenum_t*)buf = numPage;                     // change first free page
    //     *((pagenum_t*)buf + 1) = numPage * 2;           // change numPage
    //     for (pagenum_t i = 0; i < numPage - 1; ++i) {
    //         *(pagenum_t*)(buf + PAGE_SIZE * (numPage + i)) = i + numPage + 1;
    //         // add free pages : numPage ~ (2 * numPage - 2)
    //     }
    //     *(pagenum_t*)(buf + PAGE_SIZE * (2 * numPage - 1)) = (pagenum_t)0;
    //     // add free page  : 2 * numPage - 1
    //     pwrite(fd, buf, PAGE_SIZE * (numPage * 2), 0);
    //     free(buf);
    //     return numPage;
    // }
}

// Free an on-disk page to the free page list
// file_free_page <page_number> - free page
// • It informs the disk space manager of returning the page with ‘page_number’ for freeing it to the free page list.
void file_free_page(pagenum_t pagenum){
    (byte*)buf = (byte*) malloc(PAGE_SIZE * sizeof(byte));
    if (buf == NULL) {
        perror("file_free_page malloc error");
        exit(0);
    }
    if (pread(fd, buf, PAGE_SIZE, 0) <= 0) {
        perror("file_free_page pread error");
        exit(0);
    }
    pagenum_t freePage = *(pagenum_t*)buf; // read original free page
    *(pagenum_t*)buf = pagenum;
    pwrite(fd, buf, sizeof(pagenum_t), 0); // change the first free page
    *(pagenum_t*)buf = freePage;
    pwrite(fd, buf, sizeof(pagenum_t), pagenum * PAGE_SIZE); // link to original free page
    return;
}
// Read an on-disk page into the in-memory page structure(dest)
// file_read_page <page_number, dest> - read page
// • It fetches the disk page corresponding to ‘page_number’ to the in-memory buffer (i.e., ‘dest’).
void file_read_page(pagenum_t pagenum, page_t* dest){
    if (dest == NULL) {
        perror("file_read_page dest NULL");
        exit(0);
    }
    if (pread(fd, dest, PAGE_SIZE, pagenum * PAGE_SIZE) <= 0) {
        perror("file_read_page pread error");
        exit(0);
    }
}
// Write an in-memory page(src) to the on-disk page
void file_write_page(pagenum_t pagenum, const page_t* src){
    if (src == NULL) {
        perror("file_write_page src NULL");
        exit(0);
    }
    if (pwrite(fd, src, PAGE_SIZE, pagenum * PAGE_SIZE) <= 0) {
        perror("file_write_page pwrite error");
        exit(0);
    }
}
// Stop referencing the database file
void file_close_database_file(){
    close(fd);
}