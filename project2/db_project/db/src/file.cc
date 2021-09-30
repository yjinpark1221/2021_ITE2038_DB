#include "file.h"
std::vector<pagenum_t> openedFds;

// Open existing database file or create one if it doesn't exist
// int file_open_database_file (const char * pathname)
// • Open the database file.
// • It opens an existing database file using ‘pathname’ or create a new file if absent.
// • If a new file needs to be created, the default file size should be 10 MiB.
// • Then it returns the file descriptor of the opened database file.
// • All other 5 commands below should be handled after open data file.
int file_open_database_file(const char* pathname) {
    int fd = open(pathname, O_RDWR | O_SYNC);
    if (fd < 0) {
        fd = open(pathname, O_RDWR | O_SYNC | O_CREAT, 0644);
        openedFds.push_back(fd);
        pagenum_t* b = (pagenum_t*)malloc(PAGE_SIZE * 2560);
        b[0] = 1;
        b[1] = 2560;
        for (pagenum_t i = 1; i < 2560; ++i) {
            b[512 * i] = (i + 1) % 2560;
        }
        if (pwrite(fd, b, PAGE_SIZE * 2560, 0) <= 0) {
            perror("in file_open_database_file pwrite error");
            exit(0);
        }
        free(b);
    }
    return fd;
}

// Allocate an on-disk page from the free page list
// uint64_t file_alloc_page (int fd);
// • Allocate a page.
// • It returns a new page # from the free page list.
// • If the free page list is empty, then it should grow the database file and return a free page #.
pagenum_t file_alloc_page(int fd) {
    pagenum_t buf[2];
    if (pread(fd, buf, sizeof(pagenum_t) * 2, 0) <= 0) {
        perror("file_alloc_page pread error");
        exit(0);
    }
    pagenum_t freePage = buf[0], numPage = buf[1];
    if (freePage == 0) {
        buf[0] = numPage;
        buf[1] = numPage * 2;
        if (pwrite(fd, buf, sizeof(pagenum_t) * 2, 0) <= 0) {
            perror("file_alloc_page pwrite error");
            exit(0);
        }// change first free page
        for (int i = 0; i < numPage - 1; ++i) {
            buf[0] = (numPage + i + 1) % (2 * numPage);
            if (pwrite(fd, buf, sizeof(pagenum_t), (numPage + i) * PAGE_SIZE) <= 0) {
                perror("file_alloc_page pwrite error");
                exit(0);
            }
        }// add free pages : numPage ~ (2 * numPage - 1)
        freePage = numPage;
    }
    if (pread(fd, buf, sizeof(pagenum_t), freePage * PAGE_SIZE) <= 0) {
        perror("file_use_free_page pread error");
        exit(0);
    }//get next free page
    if (pwrite(fd, buf, sizeof(pagenum_t), 0) <= 0) {
        perror("file_use_free_page pread error");
        exit(0);
    }//change the first free page
    return freePage;
}

// Free an on-disk page to the free page list
// void file_free_page (int fd, uint64_t page_number);
// • Free a page.
// • It informs the disk space manager of returning the page with ‘page_number’ for freeing it to the free page list.
void file_free_page(int fd, pagenum_t pagenum) {
    pagenum_t buf;
    if (pread(fd, &buf, sizeof(pagenum_t), 0) <= 0) {
        perror("file_free_page pread error");
        exit(0);
    }
    pagenum_t freePage = buf;            // read original free page
    buf = pagenum;
    if (pwrite(fd, &buf, sizeof(pagenum_t), 0) <= 0) {
        perror("file_free_page pwrite error");
        exit(0);
    }// change the first free page
    buf = freePage;
    if (pwrite(fd, &buf, sizeof(pagenum_t), pagenum * PAGE_SIZE) <= 0) {
        perror("file_free_page pwrite error");
        exit(0);
    }// link to original free page
    return;
}

// Read an on-disk page into the in-memory page structure(dest)
// file_read_page <page_number, dest> - read page
// • It fetches the disk page corresponding to ‘page_number’ to the in-memory buffer (i.e., ‘dest’).
void file_read_page(int fd, pagenum_t pagenum, page_t* dest) {
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
// void file_write_page (int fd, uint64_t page_number, const char * src);
// • Write a page.
// • It writes the in-memory page content in the buffer (i.e., ‘src’) to the disk page pointed by ‘page_number’
void file_write_page(int fd, pagenum_t pagenum, const page_t* src) {
    if (src == NULL) {
        perror("file_write_page src NULL");
        exit(0);
    }
    if (pwrite(fd, src, PAGE_SIZE, pagenum * PAGE_SIZE) <= 0) {
        perror("file_write_page pwrite error");
        exit(0);
    }
}

// Close the database file
// void file_close_database_file();
// • Close the database file.
// • This API doesn’t receive a file descriptor as a parameter. So a means for referencing the descriptor of the opened file(i.e., global variable) is required.
void file_close_database_file(){
    for (int fd : openedFds) {
        if (close(fd) < 0) {
            perror("file_close_database_file close error");
            exit(0);
        }
    }
}

// Get total page number 
// for testing
pagenum_t file_get_size(int fd) {
    pagenum_t buf;
    pread(fd, &buf, sizeof(pagenum_t), sizeof(pagenum_t));
    return buf;
}

// Check if page is in the vector
// for testing
bool inVec(std::vector<pagenum_t> vec, pagenum_t page) {
    for (pagenum_t p : vec) {
        if (page == p) return 1;
    }
    return 0;
}

// Get the free page list
// for testing
std::vector<pagenum_t> file_get_free_list(int fd) {
    pagenum_t pagenum = lseek(fd, 0, SEEK_END) / PAGE_SIZE;
    std::vector<pagenum_t> vec;
    pagenum_t* buf = (pagenum_t*) malloc(PAGE_SIZE * pagenum);
    pread(fd, buf, PAGE_SIZE * pagenum, 0);
    pagenum_t freePage = buf[0];
    for (; freePage; freePage = buf[PAGE_SIZE / 8 * freePage]) {
        vec.push_back(freePage);
    }
    free(buf);
    return vec;
}
