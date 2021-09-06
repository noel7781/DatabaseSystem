#define Version "1.14"

#include <iostream>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include "bpt.h"
#include "insertion_index.cpp"
#include "deletion_index.cpp"
#include "lockManager.cpp"
#include "join.cpp"
#include "diskManager.cpp"
#include "bufferManager.cpp"
#include "logManager.cpp"

void usage_2( void ) {
    printf("1. open <pathame> \n2. insert <key, value> \n3. delete <key> \n4. find <key>\n5. print <tableid>\n6. close\n7. join <table1, table2>\n8. exit\n");
}

int find_Id(const char* pathname) {
    char* tmpPathname = new char[512];
    strcpy(tmpPathname, pathname);
    char* ptr = strtok(tmpPathname, "DATA");
    int ret = atoi(ptr);
    delete[] tmpPathname;
    return ret;
}

int open_table(const char *pathname) {
    int delay_plus = 0;
    int tableId = 1;
    if(access(pathname, F_OK) == -1) {
        delay_plus = 1;
    }
    if (delay_plus == 0) {
        printf("EXIST TABLE\n");
        tableId = find_Id(pathname);
        strcpy(myFile[tableId].path, pathname);
        myFile[tableId].fd = open(pathname, O_RDWR | O_CREAT, 0600);
        myFile[tableId].is_open = TRUE;
    } else {
        printf("NEW TABLE CREATE\n");
        tableId = find_Id(pathname);
        strcpy(myFile[tableId].path, pathname);
        myFile[tableId].fd = open(pathname, O_RDWR | O_CREAT, 0600);
        myFile[tableId].is_open = TRUE;
        printf("DEFAULT PAGE SIZE 8000\n");
        set_header(tableId, &head);
        file_write_page(tableId, HEADER_PAGE_NUMBER, &head);
        file_read_page(tableId, head.free_page_number, &free_page);
        free_page.next = 2;
        file_write_page(tableId, head.free_page_number, &free_page);
        for(int i = 2; i < 8000; i++) {
            free_page.next = i + 1;
            file_write_page(tableId, i, &free_page);
        }
    }
    printf("table id is %d\n", tableId);
    return tableId;
}

int close_table(int table_id) {
    buffer_control_block* cur = controller[0];
    if(cur->next == NULL) return 0;
    cur = cur->next;
    while(cur != controller[0]) {
        if(cur->table_id == table_id) {
            if(cur->is_dirty == TRUE) {
                buffer_put_page(cur);
            }
        }
        cur = cur->next;
    }
    myFile[table_id].is_open = FALSE;
    return 0;
}

int shutdown_db() {
    buffer_control_block* cur = controller[0];
    int count = 0;
    if(cur->next == NULL) return 0;
    cur = cur->next;
    count++;
    while(cur != controller[0]) {
        if(cur->is_dirty == TRUE) {
            buffer_put_page(cur);
        }
        cur = cur->next;
        count++;
    }
    free(freeNumberNode);
    for(int i = 0; i <= count; i++) {
        free(controller[i]);
    }
    free(controller);
}
void append_freeNumberNode(int num_buf, int i) {
    if(i > num_buf) return;
    freeNumber* head = freeNumberNode;
    freeNumber* newNode = (freeNumber*)malloc(sizeof(freeNumber));
    newNode->next = NULL;
    newNode->free = i;
    newNode->is_used = FALSE;
    if(head->next == NULL) {
        head->next = newNode;
    } else {
        while(head->next != NULL) {
            head = head->next;
        }
        head->next = newNode;
    }
}
int pop_freeNumberNode(int i) {
    freeNumber* p = freeNumberNode;
    if(p->next == NULL) return -1; // error state
    p = p->next;
    while(p->free != i) {
        p = p->next;
    }
    p->is_used = TRUE;
    return i;
}
int init_db(int num_buf) {
    int i = 0;
    controller = (buffer_control_block**)malloc(sizeof(buffer_control_block*) * (num_buf + 1));
    buffer_pool_latch = PTHREAD_MUTEX_INITIALIZER;
    init_TxnMgr();
    init_lockMgr();
    freeNumberNode = (freeNumber*)malloc(sizeof(freeNumber));
    freeNumberNode->free = -1;
    freeNumberNode->next = NULL;
    for(i = 0; i <= num_buf; i++) {
        controller[i] = (buffer_control_block*)malloc(sizeof(buffer_control_block));
    }
    for(int i = 1; i <= num_buf; i++) {
        append_freeNumberNode(num_buf, i);
    }
    for(int i = 1; i <= num_buf; i++) {
        controller[i]->table_id = -1;
        controller[i]->is_dirty = 0;
        controller[i]->is_pinned = 0;
        controller[i]->next = NULL;
        controller[i]->prev = NULL;
        controller[i]->buffer_page_latch = PTHREAD_MUTEX_INITIALIZER;
    }
    controller[0]->next_freeNode_header = freeNumberNode;
    if(controller != NULL) {
        init_logMgr();
        return 0;
    } else {
        return -1;
    }
}
int db_insert(int table_id, int64_t key, const char * value) {
    record_t new_record;
    new_record.key = key;
    strcpy(new_record.value, value);
    if(insert_master(table_id, key, value) != NULL) {
        return 0;
    }
    return -1;
}

//lock mode shared
int db_find(int table_id, int64_t key, char * ret_val, int trx_id) {
    if(txnMgr.trx_table[trx_id]->is_abort == TRUE) { // after abort, don't need to find or update
        return -1;
    }
    buffer_control_block* target;
    while(SUCCESS) {
        if(txnMgr.trx_table[trx_id]->is_abort == TRUE) { // after abort, don't need to find or update
            return -1;
        }
        pthread_mutex_lock(&buffer_pool_latch);
        target = find_leaf(table_id, key);
        if(target == NULL) {
            return -1; // 찾고자하는 leaf 페이지가 없을 경우 -1 리턴
        }
        //버퍼페이지에 락을 거는것을 실패한경우 버퍼풀의 래치를 풀어준다.
        if(pthread_mutex_trylock(&target->buffer_page_latch) != 0) {
            pthread_mutex_unlock(&buffer_pool_latch);
            continue;
        } else { // 레코드 락을 걸어줄 차례
            pthread_mutex_unlock(&buffer_pool_latch);
            pthread_mutex_lock(&lockMgr.lock_sys_mutex); // 락테이블 래치를 잡는다.
            bool acquired_status = FALSE;
            //lock_hash라는 함수를 만들어서 lock table에 들어갈 index를 찾는 함수를 구현하자.
            //using lock_hash function, find out index of hashed index
            int lock_hashed_index = lock_hash(table_id, target->page_num);
            //이미 존재하는지 확인
            Lock* move = lockMgr.lock_table[lock_hashed_index];
            Lock* endPoint = move;
            while(endPoint->next) {
                endPoint = endPoint->next;
            }
            //먼저 lock 구조체를 생성하고 초기화한다.
            Lock* new_lock_object = initLockObject(table_id, txnMgr.trx_table[trx_id], target->page_num, key, acquired_status, SHARED, NULL, NULL);
            new_lock_object->prev = endPoint;
            endPoint->next = new_lock_object;
            //만약 해당하는 레코드 lock object가 존재하지 않는다면 자신이 RUNNING상태가 된다.
            while(1) {
                move = find_matched_last_lock(lockMgr.lock_table[lock_hashed_index], new_lock_object, table_id, key);
                if(move == lockMgr.lock_table[lock_hashed_index]) {
                    //printf("FIRST LOCK OBJECT APPENDED\n");
                    txnMgr.trx_table[trx_id]->state = RUNNING;
                    new_lock_object->acquired = TRUE;
                    txnMgr.trx_table[trx_id]->acquired_locks.push_back(new_lock_object);
                    break;
                } else { //해당하는 레코드 lock object가 존재한다면 앞 트랜잭션이 WAIT이거나 X LOCK시 WAIT
                    //printf("LOCK OBJECT ALREADY APPENDED, FIND OK OR NOT\n this txn is %d and move's transaction is %d\n", trx_id, move->trx->trx_id);
                    if(move->acquired == FALSE || (move->acquired == TRUE && move->trx->trx_id != trx_id  && move->lock_mode == EXCLUSIVE)) {
                        txnMgr.trx_table[trx_id]->state = WAITING;
                        txnMgr.trx_table[trx_id]->wait_lock = move;
                        Lock* find_wait_lock = move;
                        if(move->trx->trx_id == -1) {
                            txnMgr.trx_table[trx_id]->state = RUNNING;
                            new_lock_object->acquired = TRUE;
                            txnMgr.trx_table[trx_id]->acquired_locks.push_back(new_lock_object);
                            break;
                        }
                        while(find_wait_lock->trx->trx_id != trx_id && find_wait_lock->trx->trx_id != -1) {
                            find_wait_lock = find_wait_lock->trx->wait_lock;
                            if(find_wait_lock == NULL) break;
                        }
                        if(find_wait_lock != NULL && find_wait_lock->key == new_lock_object->key) {
                            txnMgr.trx_table[trx_id]->state = RUNNING;
                            new_lock_object->acquired = TRUE;
                            txnMgr.trx_table[trx_id]->acquired_locks.push_back(new_lock_object);
                            break;
                        } else if(find_wait_lock != NULL) {
                            printf("DEADLOCK DETECTED\n");
                            pthread_mutex_unlock(&target->buffer_page_latch);
                            pthread_mutex_unlock(&lockMgr.lock_sys_mutex);
                            abort_trx(table_id, key, trx_id);
                            break;
                        }
                        else {
                            printf("CONFLICT DETECTED\n");
                            pthread_mutex_unlock(&lockMgr.lock_sys_mutex);
                            pthread_mutex_unlock(&target->buffer_page_latch);
                            pthread_mutex_lock(&txnMgr.trx_table[trx_id]->trx_mutex);
                            pthread_cond_wait(&txnMgr.trx_table[trx_id]->trx_cond, &txnMgr.trx_table[trx_id]->trx_mutex);
                            pthread_mutex_unlock(&txnMgr.trx_table[trx_id]->trx_mutex);
                            continue;
                        }
                    } else {
                        txnMgr.trx_table[trx_id]->state = RUNNING;
                        new_lock_object->acquired = TRUE;
                        txnMgr.trx_table[trx_id]->acquired_locks.push_back(new_lock_object);
                        break;
                    }
                }
            }
            if(txnMgr.trx_table[trx_id]->state == RUNNING) {
                record_t* tmp = find(table_id, key);
                if(tmp != NULL) {
                    strcpy(ret_val, tmp->value);
                }
                pthread_mutex_unlock(&lockMgr.lock_sys_mutex);
                pthread_mutex_unlock(&target->buffer_page_latch);
                //트랜잭션의 state를 나타내야한다.
                break;
            }
        }
    }
    /*모든과정이 끝난후*/
    return 0;
}

//lock mode exclusive
int db_update(int table_id, int64_t key, char* values, int trx_id) {
    if(txnMgr.trx_table[trx_id]->is_abort == TRUE) { // after abort, don't need to find or update
        return -1;
    }
    buffer_control_block* target;
    record_t* tmp;
    while(SUCCESS) {
        if(txnMgr.trx_table[trx_id]->is_abort == TRUE) { // after abort, don't need to find or update
            return -1;
        }
        pthread_mutex_lock(&buffer_pool_latch);
        target = find_leaf(table_id, key);
        if(target == NULL) {
            return -1; // 찾고자하는 leaf 페이지가 없을 경우 -1 리턴
        }
        //버퍼페이지에 락을 거는것을 실패한경우 버퍼풀의 래치를 풀어준다.
        if(pthread_mutex_trylock(&target->buffer_page_latch) != 0) {
            pthread_mutex_unlock(&buffer_pool_latch);
            continue;
        } else { // 레코드 락을 걸어줄 차례
            pthread_mutex_unlock(&buffer_pool_latch);
            pthread_mutex_lock(&lockMgr.lock_sys_mutex); // 락테이블 래치를 잡는다.
            bool acquired_status = FALSE;
            //lock_hash라는 함수를 만들어서 lock table에 들어갈 index를 찾는 함수를 구현하자.
            //using lock_hash function, find out index of hashed index
            int lock_hashed_index = lock_hash(table_id, target->page_num);
            //이미 존재하는지 확인
            Lock* move = lockMgr.lock_table[lock_hashed_index];
            Lock* endPoint = move;
            while(endPoint->next) {
                endPoint = endPoint->next;
            }
            //먼저 lock 구조체를 생성하고 초기화한다.
            Lock* new_lock_object = initLockObject(table_id, txnMgr.trx_table[trx_id], target->page_num, key, acquired_status, EXCLUSIVE, NULL, NULL);
            //new_lock_object->prev = endPoint->prev;
            //endPoint->prev->next = new_lock_object;
            new_lock_object->prev = endPoint;
            endPoint->next = new_lock_object;
            //트랜잭션에 lock상태를 update해준다.
            //만약 해당하는 레코드 lock object가 존재하지 않는다면 자신이 RUNNING상태가 된다.
            while(1) {
                move = find_matched_last_lock(lockMgr.lock_table[lock_hashed_index], new_lock_object, table_id, key);
                if(move == lockMgr.lock_table[lock_hashed_index]) {
                    txnMgr.trx_table[trx_id]->state = RUNNING;
                    new_lock_object->acquired = TRUE;
                    txnMgr.trx_table[trx_id]->acquired_locks.push_back(new_lock_object);
                    break;
                } else { //해당하는 레코드 lock object가 존재한다면 앞 트랜잭션이 WAIT이거나 X LOCK시 WAIT
                    if(move->trx->trx_id == -1) {
                        txnMgr.trx_table[trx_id]->state = RUNNING;
                        new_lock_object->acquired = TRUE;
                        txnMgr.trx_table[trx_id]->acquired_locks.push_back(new_lock_object);
                        break;
                    }
                    if(move->acquired == FALSE || (move->acquired == TRUE && move->trx->trx_id != trx_id  && (move->lock_mode == EXCLUSIVE || move->lock_mode == SHARED))) {
                        txnMgr.trx_table[trx_id]->state = WAITING;
                        txnMgr.trx_table[trx_id]->wait_lock = move;
                        Lock* find_wait_lock = move;
                        while(find_wait_lock->trx->trx_id != trx_id && find_wait_lock->trx->trx_id != -1) {
                            find_wait_lock = find_wait_lock->trx->wait_lock;
                            if(find_wait_lock == NULL) break;
                        }
                        if(find_wait_lock != NULL && find_wait_lock->key == new_lock_object->key && find_wait_lock->lock_mode == EXCLUSIVE) {
                            txnMgr.trx_table[trx_id]->state = RUNNING;
                            new_lock_object->acquired = TRUE;
                            txnMgr.trx_table[trx_id]->acquired_locks.push_back(new_lock_object);
                            break;
                        } else if(find_wait_lock != NULL) {
                            printf("DEADLOCK DETECTED\n");
                            pthread_mutex_unlock(&target->buffer_page_latch);
                            pthread_mutex_unlock(&lockMgr.lock_sys_mutex);
                            abort_trx(table_id, key, trx_id);
                            break;
                        }
                        else {
                            printf("CONFLICT DETECTED\n");
                            pthread_mutex_unlock(&lockMgr.lock_sys_mutex);
                            pthread_mutex_unlock(&target->buffer_page_latch);
                            pthread_mutex_lock(&txnMgr.trx_table[trx_id]->trx_mutex);
                            pthread_cond_wait(&txnMgr.trx_table[trx_id]->trx_cond, &txnMgr.trx_table[trx_id]->trx_mutex);
                            pthread_mutex_unlock(&txnMgr.trx_table[trx_id]->trx_mutex);
                            continue;
                        }
                    } else if (move->acquired == TRUE && move->trx->trx_id == trx_id) { // S락으로 줄줄히 달려있다면 conflict상황.
                        move = find_matched_last_lock(lockMgr.lock_table[lock_hashed_index], move, table_id, key);
                        if(new_lock_object->acquired == FALSE && (move->lock_mode == SHARED && move != lockMgr.lock_table[lock_hashed_index])) {
                            printf("CONFLICT DETECTED\n");
                            pthread_mutex_unlock(&lockMgr.lock_sys_mutex);
                            pthread_mutex_unlock(&target->buffer_page_latch);
                            pthread_mutex_lock(&txnMgr.trx_table[trx_id]->trx_mutex);
                            pthread_cond_wait(&txnMgr.trx_table[trx_id]->trx_cond, &txnMgr.trx_table[trx_id]->trx_mutex);
                            pthread_mutex_unlock(&txnMgr.trx_table[trx_id]->trx_mutex);
                            continue;
                        } else if(new_lock_object->acquired == FALSE) {
                            txnMgr.trx_table[trx_id]->state = RUNNING;
                            new_lock_object->acquired = TRUE;
                            txnMgr.trx_table[trx_id]->acquired_locks.push_back(new_lock_object);
                        }
                        else {
                            break;
                        }
                    }
                    else {
                        txnMgr.trx_table[trx_id]->state = RUNNING;
                        new_lock_object->acquired = TRUE;
                        txnMgr.trx_table[trx_id]->acquired_locks.push_back(new_lock_object);
                        break;
                    }
                }
            }
            if(txnMgr.trx_table[trx_id]->state == RUNNING) {
                tmp = find(table_id, key);
                int page_index = binary_search(target->frame.record, target->frame.number_of_keys, key);
                if(tmp != NULL) {
                    pthread_mutex_lock(&logMgr.log_mutex);
                    Log newLog;
                    initLogPtr(&newLog, trx_id, UPDATE, table_id, target->page_num, 128 * (page_index + 1) + 8, 120, tmp->value, values);
                    logMgr.logPool.push_back(newLog);
                    printf("log appended\n");
                    pthread_mutex_unlock(&logMgr.log_mutex);

                    strcpy(tmp->value, values);
                    target->frame.pageLSN = currentLSN;
                    printf("updated to %s\n", tmp->value);
                }
                pthread_mutex_unlock(&lockMgr.lock_sys_mutex);
                pthread_mutex_unlock(&target->buffer_page_latch);
                //트랜잭션의 state를 나타내야한다.
                break;
            }

        }
    }
    return 0;
}

int db_delete(int table_id, int64_t key) {
    if(delete_master(table_id, key) != NULL) {
        return 0;
    }
    return -1;
}

int binary_search(record_t *arr, int size, int target) {
    int start_idx = 0;
    int end_idx = size - 1;
    int mid_idx = 0;
    while(start_idx <= end_idx) {
        mid_idx = (start_idx + end_idx) / 2;
        if(target == arr[mid_idx].key) return mid_idx;
        else if(arr[mid_idx].key > target) {
            end_idx = mid_idx - 1;
        } else {
            start_idx = mid_idx + 1;
        }
    }
    return -1;
}
/* Prints the bottom row of keys
 * of the tree (with their respective
 * pointers, if the verbose_output flag is set.
 */

void print_leaves(int table_id) {
    if(myFile[table_id].is_open == FALSE) {
        printf("table open yet\n");
        return;
    }
    int i;
    buffer_control_block* cur = buffer_write_page(table_id, HEADER_PAGE_NUMBER);
    buffer_control_block* cur_2;
    if (cur->frame.root_page_number == 0) {
        //printf("Empty tree.\n");
        return;
    }
    cur_2 = buffer_write_page(table_id, cur->frame.root_page_number);
    cur->is_pinned = FALSE;
    cur = cur_2;
    while (!cur->frame.is_leaf) {
        cur_2 = buffer_write_page(table_id, cur->frame.more_page_number);
        cur->is_pinned = FALSE;
        cur = cur_2;
    }
    while (true) {
        for (i = 0; i < cur->frame.number_of_keys; i++) {
            //printf("%lu ", cur->frame.record[i].key);
            printf("%lu | %s\n", cur->frame.record[i].key, cur->frame.record[i].value);
        }
        if (cur->frame.more_page_number != 0) {
            //printf(" | ");
            cur_2 = buffer_write_page(table_id, cur->frame.more_page_number);
            cur->is_pinned = FALSE;
            cur = cur_2;
        }
        else
            break;
    }
    cur->is_pinned = FALSE;
    //printf("\n");
}

buffer_control_block* find_leaf(int table_id, int key) {
    int i = 0;
    buffer_control_block* leaf_page_block;
    buffer_control_block* head_block;
    head_block = buffer_write_page(table_id, HEADER_PAGE_NUMBER);
    if (head_block->frame.root_page_number == 0) {
        //printf("Empty tree.\n");
        head_block->is_pinned = FALSE;
        return NULL;
    }
    buffer_control_block* p = buffer_write_page(table_id, head_block->frame.root_page_number);
    head_block->is_pinned = FALSE;
    if(p->frame.is_leaf) {
        leaf_page_block = p;
    }
    while (!p->frame.is_leaf) {
        i = 0;
        while (i < p->frame.number_of_keys) {
            if (key >= p->frame.keyPage[i].key) i++;
            else break;
        }
        if(i == 0) {
            leaf_page_block = buffer_write_page(table_id, p->frame.more_page_number);
            p->is_pinned = FALSE;
            p = leaf_page_block;
        } else {
            leaf_page_block = buffer_write_page(table_id, p->frame.keyPage[i - 1].pageNum);
            p->is_pinned = FALSE;
            p = leaf_page_block;
        }
    }
    p->is_pinned = FALSE;
    return p;
}


/* Finds and returns the record to which
 * a key refers.
 */
record_t* find(int table_id, int key) {
    int i = 0;
    page_t tmp;
    buffer_control_block* leaf_block = find_leaf(table_id, key);
    if (leaf_block == NULL) {
        return NULL;
    }
    for (i = 0; i < leaf_block->frame.number_of_keys; i++)
        if (leaf_block->frame.record[i].key == key) break;
    if (i == leaf_block->frame.number_of_keys) {
        leaf_block->is_pinned = FALSE;
        return NULL;
    }
    else {
        rec = &(leaf_block->frame.record[i]);
    }
    leaf_block->is_pinned = FALSE;
    return rec;
}

/* Finds the appropriate place to
 * split a node that is too big into two.
 */
int cut( int length ) {
    if (length % 2 == 0)
        return length/2;
    else
        return length/2 + 1;
}