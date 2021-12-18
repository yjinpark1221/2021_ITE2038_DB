#ifndef __RECOVERY_H__
#define __RECOVERY_H__

#include "buffer.h"
#define BEGIN 0
#define UPDATE 1
#define COMMIT 2
#define ROLLBACK 3
#define COMPENSATE 4

typedef uint64_t lsn_t;
typedef uint32_t logsize_t;
typedef int32_t type_t;

struct mlog_t;

struct log_t {
    char a[300];
    log_t(mlog_t log) {
        *((logsize_t*)a) = log.size;
        *((lsn_t*)(a + 4)) = log.lsn;
        *((lsn_t*)(a + 12)) = log.prevlsn;
        *((u32_t*)(a + 20)) = log.trx_id;
        *((type_t*)(a + 24)) = log.type;
        *((table_t*)(a + 28)) = log.table_id;
        *((pagenum_t*)(a + 36)) = log.pagenum;
        *((u16_t*)(a + 44)) = log.offset;
        *((u16_t*)(a + 46)) = log.size;
        memcpy(a + 48, log.old_image.c_str(), log.size);
        memcpy(a + 48 + log.size, log.new_image.c_str(), log.size);
        *((lsn_t*)(a + 48 + 2 * log.size)) = log.nextundolsn;
    }
    log_t() : log_t(mlog_t()) {}
};

struct mlog_t {
    logsize_t log_size;
    lsn_t lsn;
    lsn_t prevlsn;
    int32_t trx_id;
    u32_t type;
    table_t table_id;
    pagenum_t pagenum;
    u16_t offset;
    u16_t size;
    std::string old_image;
    std::string new_image;
    lsn_t nextundolsn;
    bool dirty;
    mlog_t() {
        type = 5;
    }
    mlog_t(log_t& log) {
        size = *((logsize_t*)log.a);
        lsn = *((lsn_t*)(log.a + 4));
        prevlsn = *((lsn_t*)(log.a + 12));
        trx_id = *((u32_t*)(log.a + 20));
        type = *((type_t*)(log.a + 24));
        table_id = *((table_t*)(log.a + 28));
        pagenum = *((pagenum_t*)(log.a + 36));
        offset = *((u16_t*)(log.a + 44));
        size = *((u16_t*)(log.a + 46));
        old_image = std::string(log.a + 48, size);
        new_image = std::string(log.a + 48 + size, size);
        nextundolsn = *((lsn_t*)(log.a + 48 + 2 * size));
        dirty = 0;
    }
};

extern int logfd;
extern std::map<table_page_t, lsn_t> dirty_pages;

logsize_t getLogSize(type_t type, u16_t size = 0);
void flush_logs();
mlog_t& getLog(lsn_t lsn);
void force();
void analyze(FILE* logmsgfp);
void apply_redo(mlog_t& log, FILE* logmsgfp);
void redo(int flag, int log_num, FILE* logmsgfp);

#endif