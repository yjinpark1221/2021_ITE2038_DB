#include "recovery.h"

int logfd;
std::map<lsn_t, mlog_t> logs;
std::map<table_page_t, lsn_t> dirty_pages;

logsize_t getLogSize(type_t type, u16_t size = 0) {
    if (type == BEGIN || type == COMMIT || type == ROLLBACK) {
        return 28;
    }
    else if (type == UPDATE) {
        return 48 + size;
    }
    else if (type == COMPENSATE) {
        return 56 + 2 * size;
    }
    else {
        return 0;
    }
}

void flush_logs() {
    for (auto log: logs) {
        if (log.second.dirty) {
            log_t l(log.second);
            pwrite(logfd, &log, log.second.log_size, log.second.lsn);
        }
    }
    logs.clear();
}

mlog_t& getLog(lsn_t lsn) {
    auto iter = logs.find(lsn);
    if (iter != logs.end()) {
        return iter->second;
    }
    if (logs.size() == 10000) {
        flush_logs();
    }
    log_t log;
    pread(logfd, &log, sizeof(log_t), lsn);
    logs[lsn] = mlog_t(log);
    return logs[lsn];
}

void force() {
    for (auto dirty : dirty_pages) {
        flush(tp2control[dirty.first]);
    }
    flush_logs();
}

void analyze(FILE* logmsgfp) {
    fprintf(logmsgfp, "[ANALYSIS] Analysis pass start\n");

    std::set<int> winners, losers;
    int offset = 0;
    for (mlog_t log = getLog(0); log.type != 5; log = getLog(offset)) {
        offset += log.log_size;

        if (log.type == BEGIN) {
            losers.insert(log.trx_id);
        }
        else if (log.type == COMMIT || log.type == ROLLBACK) {
            assert(losers.find(log.trx_id) != losers.end());
            losers.erase(log.trx_id);
            winners.insert(log.trx_id);
        }
    }

    fprintf(logmsgfp, "[ANALYSIS] Analysis success. Winner :");
    for (auto win : winners) {
        fprintf(logmsgfp, " %d", win);
    }
    fprintf(logmsgfp, ", Loser:");
    for (auto lose : losers) {
        fprintf(logmsgfp, " %d", lose);
    }
    fprintf(logmsgfp, "\n");
}

void apply_redo(mlog_t& log, FILE* logmsgfp) {
    if (log.type == BEGIN) {
        fprintf(logmsgfp, "LSN %lu [BEGIN] Transaction id %d\n", log.lsn, log.trx_id);
    }
    
    else if (log.type == UPDATE) {
        ctrl_t* ctrl = buf_read_page(log.table_id, log.pagenum);
        page_t page = *ctrl->frame;
        lsn_t pagelsn = *(lsn_t*)(page.a + 24);
        if (pagelsn >= log.lsn) {
            fprintf(logmsgfp, "LSN %lu [CONSIDER-REDO] Transaction id %d\n", log.lsn, log.trx_id);
            pthread_mutex_unlock(&(ctrl->mutex));
        }
        else {
            fprintf(logmsgfp, "LSN %lu [UPDATE] Transaction id %d redo apply\n", log.lsn, log.trx_id);
            *(lsn_t*)(page.a + 24) = log.lsn;
            buf_write_page(&page, ctrl);
            pthread_mutex_unlock(&(ctrl->mutex));
        }
    }
    else if (log.type == COMMIT) {
        fprintf(logmsgfp, "LSN %lu [COMMIT] Transaction id %d\n", log.lsn, log.trx_id);
    }
    else if (log.type == ROLLBACK) {
        fprintf(logmsgfp, "LSN %lu [ROLLBACK] Transaction id %d\n", log.lsn, log.trx_id);
    }

    else if (log.type == COMPENSATE) {
        ctrl_t* ctrl = buf_read_page(log.table_id, log.pagenum);
        page_t page = *ctrl->frame;
        lsn_t pagelsn = *(lsn_t*)(page.a + 24);

        if (pagelsn >= log.lsn) {
            fprintf(logmsgfp, "LSN %lu [CONSIDER-REDO] Transaction id %d\n", log.lsn, log.trx_id);
            pthread_mutex_unlock(&(ctrl->mutex));
        }
        else {
            fprintf(logmsgfp, "LSN %lu [CLR] next undo lsn %lu\n", log.lsn, log.nextundolsn);
            *(lsn_t*)(page.a + 24) = log.lsn;
            buf_write_page(&page, ctrl);
            pthread_mutex_unlock(&(ctrl->mutex));
        }
    }
}

void redo(int flag, int log_num, FILE* logmsgfp) {
    fprintf(logmsgfp, "[REDO] Redo pass start\n");

    int offset = 0, cnt = 0;
    for (mlog_t log = getLog(0); log.type != 5; log = getLog(offset)) {
        if (flag == 1 && cnt == log_num) return;
        ++cnt;
        offset += log.log_size;
        apply_redo(log, logmsgfp);
    }

    fprintf(logmsgfp, "[REDO] Redo pass end\n");
}

void undo(int flag, int log_num, FILE* logmsgfp) {
    fprintf(logmsgfp, "[UNDO] Undo pass start\n");
    



    fprintf(logmsgfp, "[UNDO] Undo pass end\n");
}
