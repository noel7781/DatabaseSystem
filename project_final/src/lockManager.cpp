//lockManager
#include "bpt.h"

int begin_trx() {
    pthread_mutex_lock(&txnMgr.trx_sys_mutex);
    trx_count++;
    if(trx_count > MAX_TXN_COUNT) {
        printf("TOO MUCH TRANSACTION. EDIT CONSTANT MAX_TRHEAD_NUM VALUE\n");
        trx_count--;
        pthread_mutex_unlock(&txnMgr.trx_sys_mutex);
        return -1;
    }
    int i = 0;
    for(i; i < MAX_TXN_COUNT; i++) {
        if(txnMgr.trx_table[i]->trx_id == -1) break;
    }
    Transaction* new_trx = txnMgr.trx_table[i];
    new_trx->state = IDLE;
    new_trx->wait_lock = NULL;
    new_trx->trx_mutex = PTHREAD_MUTEX_INITIALIZER;
    new_trx->trx_cond = PTHREAD_COND_INITIALIZER;
    new_trx->trx_id = gNoTrx++;
    new_trx->acquired_locks.clear();
    new_trx->undo_log_list.clear();
    new_trx->is_abort = FALSE;
    txnMgr.next_trx_id = gNoTrx;

    pthread_mutex_lock(&logMgr.log_mutex);
    Log newLog;
    initLogPtr(&newLog, new_trx->trx_id, BEGIN, -1, -1, 0, 0, NULL, NULL);
    logMgr.logPool.push_back(newLog);
    pthread_mutex_unlock(&logMgr.log_mutex);
    pthread_mutex_unlock(&txnMgr.trx_sys_mutex);
    return new_trx->trx_id;
}

int end_trx(int tid) {

    pthread_mutex_lock(&logMgr.log_mutex);
    Log newLog;
    initLogPtr(&newLog, tid, COMMIT, -1, -1, 0, 0, NULL, NULL);
    logMgr.logPool.push_back(newLog);
    pthread_mutex_unlock(&logMgr.log_mutex);
    flushLog();

    if(txnMgr.trx_table[tid]->trx_id == -1) { // 이미 END TRANSACTION 실행된 트랜잭션일 경우
        return -1;
    }
    TransactionPtr p_txn = txnMgr.trx_table[tid];
    LockPtr p_lock = NULL;
    LockPtr next_lock = NULL;
    list<Lock*>::iterator it;

    pthread_mutex_lock(&lockMgr.lock_sys_mutex);
    for(it = txnMgr.trx_table[tid]->acquired_locks.begin(); it != txnMgr.trx_table[tid]->acquired_locks.end(); it++) {
        bool isExec = FALSE;
        p_lock = *it;
        next_lock = p_lock;
        int compKey = p_lock->key;
        if(p_lock == NULL) continue;
        if(p_lock->acquired == TRUE) {
            p_lock->acquired = FALSE;
            p_lock->prev->next = p_lock->next;
            if(p_lock->next) {
                next_lock = p_lock->next;
                p_lock->next->prev = p_lock->prev;
            }
            if(p_lock != NULL) {
                delete[] p_lock;
                p_lock = NULL;
            }
            while(next_lock) {
                int shareCount = 0;
                if(next_lock->trx->trx_id == -1) break;
                if(next_lock->trx->trx_id == tid || next_lock->key != compKey) { 
                    next_lock = next_lock->next;
                } else {
                    if(!isExec) {
                        LockPtr viewLock = NULL;
                        if(next_lock->acquired == TRUE) {
                            if(next_lock->prev->table_id != -1) {
                                viewLock = next_lock->prev;                        
                                while(viewLock->table_id != -1) {
                                    if(viewLock->key != next_lock->key) {
                                        viewLock = viewLock->prev;
                                    } else {
                                        if(viewLock->acquired == TRUE) {
                                            shareCount++;
                                            break;
                                        } else {
                                            viewLock = viewLock->prev;
                                        }
                                    }
                                }
                            }
                            if(next_lock->next) {
                                viewLock = next_lock->next;
                                while(viewLock) {
                                    if(viewLock->key != next_lock->key) {
                                        viewLock = viewLock->next;
                                    } else {
                                        if(viewLock->acquired == TRUE) {
                                            shareCount++;
                                            break;
                                        } else {
                                            viewLock = viewLock->next;
                                        }
                                    }
                                }
                            }
                        }
                        if(shareCount > 0) break;
                        
                        if(next_lock->acquired == TRUE) {
                            next_lock = next_lock->next;
                            continue;
                        }
                        next_lock->trx->state = RUNNING;
                        next_lock->acquired = TRUE;
                        next_lock->trx->acquired_locks.push_back(next_lock);
                        pthread_cond_signal(&(next_lock->trx->trx_cond));
                        if(next_lock->lock_mode == EXCLUSIVE) break;
                        else isExec = TRUE;
                        next_lock = next_lock->next;
                    } else {
                        if(next_lock->lock_mode == SHARED) {
                            next_lock->trx->state = RUNNING;
                            next_lock->acquired = TRUE;
                            next_lock->trx->acquired_locks.push_back(next_lock);
                            pthread_cond_signal(&(next_lock->trx->trx_cond));
                            next_lock = next_lock->next;
                        } else {//처음깨우는게 아니고 EXCLUSIVE 모드일때는 깨우지않는다.
                            break;
                        }
                    }
                }
            }
        }
    }
    pthread_mutex_unlock(&lockMgr.lock_sys_mutex);

    pthread_mutex_lock(&txnMgr.trx_sys_mutex);
    //delete txn from txn table
    txnMgr.trx_table[tid]->state = END;
    txnMgr.trx_table[tid]->acquired_locks.clear();
    txnMgr.trx_table[tid]->trx_id = -1;
    txnMgr.trx_table[tid]->wait_lock = NULL;
    txnMgr.trx_table[tid]->undo_log_list.clear();
    trx_count--;
    pthread_mutex_unlock(&txnMgr.trx_sys_mutex);
    //pthread_exit(NULL);
    return 0;
}

int abort_trx(int table_id, Key key, int tid) {
    printf("abort transaction called : %d\n", tid);
    buffer_control_block* page;
    TransactionPtr p_txn = txnMgr.trx_table[tid];
    p_txn->is_abort = TRUE;
    pthread_mutex_lock(&logMgr.log_mutex);
    Log newLog;
    initLogPtr(&newLog, tid, ABORT, -1, -1, 0, 0, NULL, NULL);
    logMgr.logPool.push_back(newLog);
    pthread_mutex_unlock(&logMgr.log_mutex);

    list<Log>::reverse_iterator log_iterator;
    log_iterator = logMgr.logPool.rbegin();
    while(log_iterator != logMgr.logPool.rend()) {
        pthread_mutex_lock(&buffer_pool_latch);
        page = find_leaf(table_id, key);
        if(pthread_mutex_trylock(&page->buffer_page_latch) != 0) {
            pthread_mutex_unlock(&buffer_pool_latch);
            continue;
        } else {
            pthread_mutex_unlock(&buffer_pool_latch);
            pthread_mutex_lock(&logMgr.log_mutex);
            if((*log_iterator).transactionID == tid && (*log_iterator).type == BEGIN)  {
                pthread_mutex_unlock(&logMgr.log_mutex);
                pthread_mutex_unlock(&page->buffer_page_latch);
                break; // BEGIN찾으면 더이상 반복하지 않아도 되므로 끝냄
            } else if((*log_iterator).transactionID == tid && (*log_iterator).type == UPDATE){
                char current[128] = {0, };
                char before[128] = {0, };
                strcpy(current, (*log_iterator).newImage);
                strcpy(before, (*log_iterator).oldImage);
                int keyValue = page->frame.record[(*log_iterator).offset / 128 - 1].key;
                Log newLog;
                record_t* editRecord = find(table_id, keyValue);
                initLogPtr(&newLog, tid, UPDATE, table_id, page->page_num, (*log_iterator).offset , 120, current, before);
                logMgr.logPool.push_back(newLog);
                printf("rollback from %s to %s\n", current, before);
                strcpy(editRecord->value, before);
                log_iterator++;
                pthread_mutex_unlock(&logMgr.log_mutex);
                pthread_mutex_unlock(&page->buffer_page_latch);
            } else {
                log_iterator++;
                pthread_mutex_unlock(&logMgr.log_mutex);
                pthread_mutex_unlock(&page->buffer_page_latch);
            }
        }
    }
    end_trx(tid);
}
void init_TxnMgr() {
    txnMgr.trx_table = new TransactionPtr[MAX_TXN_COUNT];
    for(int i = 0; i < MAX_TXN_COUNT; i++) {
        txnMgr.trx_table[i] = new Transaction;
        txnMgr.trx_table[i]->trx_id = -1;
        txnMgr.trx_table[i]->wait_lock = NULL;
    }
    txnMgr.next_trx_id = gNoTrx;
    txnMgr.trx_sys_mutex = PTHREAD_MUTEX_INITIALIZER;
}
void init_lockMgr() {
    lockMgr.lock_table = new Lock*[HASH_SIZE];
    for(int i = 0; i < HASH_SIZE; i++) {
        lockMgr.lock_table[i] = new Lock;
        lockMgr.lock_table[i]->table_id = -1;
        lockMgr.lock_table[i]->trx = NULL;
        lockMgr.lock_table[i]->page_id = -1;
        lockMgr.lock_table[i]->key = -1;
        lockMgr.lock_table[i]->acquired = FALSE;
        lockMgr.lock_table[i]->lock_mode = NOLOCK;
        lockMgr.lock_table[i]->prev = lockMgr.lock_table[i];
        lockMgr.lock_table[i]->next = NULL;
    }
    lockMgr.lock_sys_mutex = PTHREAD_MUTEX_INITIALIZER;
}
int lock_hash(int table_id, pagenum_t pageNo) {
    return (table_id * table_id * (pageNo + 1)) % (HASH_SIZE + 1);
}

LockPtr initLockObject(int table_id, TransactionPtr transaction, PageId pageId, Key key, bool acquired, LockMode mode, LockPtr prev, LockPtr next) {
    LockPtr newLock = new Lock;
    newLock->table_id = table_id;
    newLock->trx = transaction;
    newLock->page_id = pageId;
    newLock->key = key;
    newLock->acquired = acquired;
    newLock->lock_mode = mode;
    newLock->prev = prev;
    newLock->next = next;
    return newLock;
}

Lock* find_matched_last_lock(Lock* head, Lock* target, int table_id, int64_t key) {
    Lock* cur = head;
    Lock* tmp = cur;
    while(tmp != target) {
        if(tmp->table_id == table_id && tmp->key == key) {
            cur = tmp;
            tmp = tmp->next;
        } else {
            tmp = tmp->next;
        }
    }
    return cur;
}