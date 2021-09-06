// Log Mnanager
#include "bpt.h"

void init_logMgr() {
    logMgr.log_mutex = PTHREAD_MUTEX_INITIALIZER;
    log_fd = open("logFile", O_RDWR | O_CREAT | O_APPEND, 0600);  // log - append only
    analyzeLog();
    redoLog();
    undoLog();
}

void initLogPtr(Log* retLog, int trx_id, TrxType trx_type, TableId table_id, int pageNumber, int offset, int dataLength, char* old_image, char* new_image) {
    int sizeCount = 0;
    retLog->prevLSN = currentLSN;
    retLog->transactionID = trx_id;
    retLog->type = trx_type;
    retLog->tableId = table_id;
    retLog->pageNumber = pageNumber;
    retLog->offset = offset;
    retLog->dataLength = dataLength;
    if(trx_type == UPDATE) {
        strcpy(retLog->oldImage, old_image);
        strcpy(retLog->newImage, new_image);
        sizeCount = sizeof(LSN) * 2 + sizeof(int) + sizeof(TrxType) + sizeof(TableId) + sizeof(int) + sizeof(int) * 2 + sizeof(char) * dataLength * 2;
    } else {
        sizeCount = sizeof(LSN) * 2 + sizeof(int) + sizeof(TrxType) + sizeof(TableId) + sizeof(int) * 3;
    }
    currentLSN += sizeCount;
    retLog->presentLSN = currentLSN;
}

void flushLog() {
    pthread_mutex_lock(&logMgr.log_mutex);
    list<Log>::iterator it; 
    for(it = logMgr.logPool.begin(); it != logMgr.logPool.end(); it++) {
        int size = (*it).presentLSN - (*it).prevLSN;
        pwrite(log_fd, &(*it), size, 0); // log - append only
        fsync(log_fd);
    }
    logMgr.logPool.clear();

    pthread_mutex_unlock(&logMgr.log_mutex);
}

void analyzeLog() {
    int offset = 0;
    int bufSize = lseek(log_fd, 0, SEEK_END);
    if(bufSize == 0) return;
    char fromStableLog[bufSize + 1];
    memset(fromStableLog, 0, bufSize + 1);
    lseek(log_fd, 0, SEEK_SET);
    if(pread(log_fd, fromStableLog, bufSize + 1, 0) == -1) {
        perror("pread error");
    }
    while(offset < bufSize) {
        Log saveLog;
        memcpy(&saveLog.presentLSN, fromStableLog + offset, 8);
        memcpy(&saveLog.prevLSN, fromStableLog + sizeof(LSN) + offset, 8);
        int diff = saveLog.presentLSN - saveLog.prevLSN;
        /*
        if(saveLog.presentLSN - saveLog.prevLSN == 40) {
            memcpy(&saveLog, fromStableLog + offset, 40);
            offset += 40;
        } else {
            memcpy(&saveLog, fromStableLog + offset, 280);
            offset += 280;
        }*/
        memset(&saveLog, 0, sizeof(saveLog));
        memcpy(&saveLog, fromStableLog + offset, diff);
        offset += diff;
        logMgr.logPool.push_back(saveLog);
        if(saveLog.tableId > -1) {
            allTableId.insert(saveLog.tableId);
            loserLog.insert(pair<int, Log>(saveLog.transactionID, saveLog));
        }
        if(saveLog.type == COMMIT) {
            loserLog.erase(saveLog.transactionID);
        }
    }
    /*
    multimap<int, Log>::iterator it2;
    for(it2 = loserLog.begin(); it2 != loserLog.end(); it2++) {
        Log tmp = (*it2).second;
        printf("txn id is %d and string is %s to %s\n", tmp.transactionID, tmp.oldImage, tmp.newImage);
    }*/

}
void redoLog() {
    set<int>::iterator it;
    buffer_control_block* bufferPage = NULL;
    record_t* editRecord = NULL;
    for(it = allTableId.begin(); it != allTableId.end(); it++) {
        int table_id = *it;
        char pathname[20];
        sprintf(pathname, "%s%d", "DATA", table_id);
        open_table(pathname);
    }
    list<Log>::iterator it2;
    for(it2 = logMgr.logPool.begin(); it2 != logMgr.logPool.end(); it2++) {
        if((*it2).type == UPDATE) {
            int tableId = (*it2).tableId;
            Key keyValue = 0;
            bufferPage = buffer_write_page(tableId, (*it2).pageNumber);
            keyValue = bufferPage->frame.record[(*it2).offset / 128 - 1].key;
            editRecord = find(tableId, keyValue);
            printf("redo change %s to %s\n", editRecord->value, (*it2).newImage);
            strcpy(editRecord->value, (*it2).newImage);
        } 
    }
}
void undoLog() {
    multimap<int, Log>::reverse_iterator it;
    buffer_control_block* page;
    record_t* editRecord;
    for(it = loserLog.rbegin(); it != loserLog.rend(); it++) {
        Log tmpLog = (*it).second;
        Log newLog;
        pthread_mutex_lock(&logMgr.log_mutex);
        if(tmpLog.type == UPDATE) {
            printf("undo from %s to %s\n", tmpLog.newImage, tmpLog.oldImage);
            initLogPtr(&newLog, (*it).first, UPDATE, tmpLog.tableId, tmpLog.pageNumber, tmpLog.offset, tmpLog.dataLength, tmpLog.newImage, tmpLog.oldImage);
            logMgr.logPool.push_back(newLog);
            page = buffer_write_page(tmpLog.tableId, tmpLog.pageNumber);
            Key keyValue = page->frame.record[tmpLog.offset / 128 - 1].key;
            editRecord = find(tmpLog.tableId, keyValue);
            strcpy(editRecord->value, tmpLog.oldImage);
        } else if(tmpLog.type == BEGIN) {
            initLogPtr(&newLog, (*it).first, COMMIT, tmpLog.tableId, -1, -1, 0, NULL, NULL);
        }
        pthread_mutex_unlock(&logMgr.log_mutex);
    }
    logMgr.logPool.clear(); // 로그를 전부 적용한 후 로그버퍼를 비워준다.
}

