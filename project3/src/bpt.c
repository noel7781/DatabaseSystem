#define Version "1.14"


#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "bpt.h"
// GLOBALS.

page_t head;
page_t free_page;
buffer_control_block** controller;
freeNumber* freeNumberNode;
MyFile_t myFile[30];
record* rec;
void usage_2( void ) {
    printf("1. open <pathame> \n2. insert <key, value> \n3. delete <key> \n4. find <key>\n5. print\n6. close\n");
}

int set_header(int table_id, page_t* header) {
    file_read_page(table_id, HEADER_PAGE_NUMBER, header);
    header->free_page_number = 1;
    header->root_page_number = 0;
    header->number_of_pages = 8000;
    file_write_page(table_id, HEADER_PAGE_NUMBER, header);
}

// Allocate an on-disk page from the free page list 
/*
pagenum_t file_alloc_page(int table_id) {
    file_read_page(table_id, HEADER_PAGE_NUMBER, &head);
    pagenum_t p1 = head.free_page_number;
    page_t tmp;
    file_read_page(table_id, p1, &tmp);
    pagenum_t p2 = tmp.next;
    head.free_page_number = p2;
    file_write_page(table_id, HEADER_PAGE_NUMBER, &head);
    printf("%lu page allocated\n", p1);
    return p1;
}
*/
// Free an on-disk page to the free page list 

/*
void file_free_page(int table_id, pagenum_t pagenum) {
    file_read_page(table_id, HEADER_PAGE_NUMBER, &head);
    page_t tmp_page;
    file_read_page(table_id, pagenum, &tmp_page);
    pagenum_t tmp = head.free_page_number;
    head.free_page_number = pagenum;
    memset(&tmp_page, 0, sizeof(page_t));
    tmp_page.next = tmp;
    file_write_page(table_id, HEADER_PAGE_NUMBER, &head);
    file_write_page(table_id, pagenum, &tmp_page);
}
*/

// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(int table_id, pagenum_t pagenum, page_t* dest) {
    int size = 0;
    if((size = pread(myFile[table_id].fd, dest, 4096, pagenum * 4096)) == - 1) {
        perror("pread");
    }
}
// Write an in-memory page(src) to the on-disk page
void file_write_page(int table_id, pagenum_t pagenum, const page_t* src) {
    int size = 0;
    if((size = pwrite(myFile[table_id].fd, src, 4096, pagenum * 4096)) == -1) {
        perror("pwrite");
    }
    fsync(myFile[table_id].fd);
}
buffer_control_block* evict_block() {
    buffer_control_block* p = controller[0];
    p = p->next;
    while(p->is_pinned && p != controller[0]) {
        p = p->next;
    }
    return p; // doubly linked list의 처음 block을 evict시킴.
}
//set controller[0] AS head
buffer_control_block* buffer_write_page(int table_id, pagenum_t pageNum) {
    //printf("INPUT PAGE NUMBER is %lu\n", pageNum);
    buffer_control_block* check = buffer_check_page(table_id, pageNum);
    if(check != NULL) {
        check->is_pinned = TRUE;
        if(controller[0]->prev == check) {
            return check;
        } else {
            check->prev->next = check->next;
            check->next->prev = check->prev;
            check->prev = controller[0]->prev;
            controller[0]->prev->next = check;
            controller[0]->prev = check;
            check->next = controller[0];
            return check;
        }
    } else {
        //printf("buffer will added\n");
        buffer_control_block* p = controller[0];
        int idx = 0;
        int count = 0;
        page_t tmp;
        freeNumber* q = p->next_freeNode_header;
        q = q->next;
        while(q->is_used == TRUE && q->next != NULL) {
            q = q->next;
        }
        file_read_page(table_id, pageNum, &tmp);
        if(p->next == NULL) {
            idx = q->free;
            q->is_used = TRUE;
            p->prev = controller[idx];
            p->next = controller[idx];
            controller[idx]->prev = p;
            controller[idx]->table_id = table_id;
            controller[idx]->page_num = pageNum;
            controller[idx]->is_pinned = 1;
            controller[idx]->frame = tmp;
            controller[idx]->next = controller[0];
            return controller[idx];
        } else {
            while(p->next != controller[0]) {
                p = p->next;
            }
            if(q->next != NULL) {
                idx = q->free;
                q->is_used = TRUE;
                p->next = controller[idx];
                controller[idx]->prev = p;
                controller[idx]->table_id = table_id;
                controller[idx]->page_num = pageNum;
                controller[idx]->is_pinned = 1;
                controller[idx]->frame = tmp;
                controller[idx]->next = controller[0];
                controller[0]->prev = controller[idx];
                return controller[idx];
            } else {
                buffer_control_block* victim = evict_block();
                if(victim->is_dirty == 1) {
                    buffer_put_page(victim); // write page to disk part 
                }
                victim->page_num = pageNum;
                victim->table_id = table_id;
                victim->is_pinned = 1;
                victim->frame = tmp;
                victim = buffer_write_page(table_id, pageNum);
                return victim;
            }
        }
    }
    return check;
}
pagenum_t buffer_alloc_page(int table_id) {
    buffer_control_block* header_block = buffer_write_page(table_id, HEADER_PAGE_NUMBER);
    pagenum_t p1 = header_block->frame.free_page_number;
    buffer_control_block* tmp = buffer_write_page(table_id, p1);
    pagenum_t p2 = tmp->frame.next;
    header_block->frame.free_page_number = p2;
    header_block->is_dirty = TRUE;
    header_block->is_pinned = FALSE;
    tmp->is_pinned = FALSE;
    return p1;
}
/*
void buffer_free_page(int table_id, buffer_control_block* blk) {
    blk->table_id = -1;
    //memset(blk, 0, sizeof(buffer_control_block));
}
*/
void buffer_free_page(int table_id, pagenum_t pagenum) {
    buffer_control_block* header_block = buffer_write_page(table_id, HEADER_PAGE_NUMBER);
    buffer_control_block* tmp_block = buffer_write_page(table_id, pagenum);
    pagenum_t tmp = header_block->frame.free_page_number;
    header_block->frame.free_page_number = pagenum;
    memset(&tmp_block->frame, 0, sizeof(page_t));
    //file_free_page(table_id, tmp_block->page_num);
    tmp_block->frame.next = tmp;
    header_block->is_dirty = TRUE;
    tmp_block->is_dirty = TRUE;
    header_block->is_pinned = FALSE;
    tmp_block->is_pinned = FALSE;
}

void buffer_flush_page(int table_id, buffer_control_block* blk) {
    blk->table_id = -1;
}
//버퍼에 해당 페이지가 있는지 체크하는 함수. 만약 한바퀴를 돌아 자기 자신으로 돌아온다면
//버퍼에 해당 페이지가 없다.
buffer_control_block* buffer_check_page(int table_id, pagenum_t pageNum) {
    buffer_control_block* p = controller[0];
    if(p->next == NULL) return NULL;
    else {
        p = p->next;
        while(p != controller[0]) {
            if(p->table_id == table_id && p->page_num == pageNum) {
                return p;
            }
            p = p->next;
        }
        return NULL;
    }
}
//버퍼에 있는걸 디스크로 내리는 함수
void buffer_put_page(buffer_control_block* blk) {
    pagenum_t pgNum = blk->page_num;
    page_t page = blk->frame;
    file_write_page(blk->table_id, pgNum, &page);
    blk->is_dirty = FALSE;
    blk->is_pinned = FALSE;
}

int find_path(char* pathname) {
    int i = 1;

    for(i = 1; i <= 30; i++) {
        if(strcmp(myFile[i].path, pathname) == 0) {
            break;
        }
    }
    if(i == 31) {
        for(i = 1; i <= 30; i++) {
            if(myFile[i].fd == 0) {
                return i;
            }
        }
    }
    return i;
}

int open_table(char *pathname) {
    int delay_plus = 0;
    int tableId = 1;
    if(access(pathname, F_OK) == -1) {
        delay_plus = 1;
    }
    if (delay_plus == 0) {
        tableId = find_path(pathname);
        strcpy(myFile[tableId].path, pathname);
        myFile[tableId].fd = open(pathname, O_RDWR | O_CREAT, 0600);
        myFile[tableId].is_used = TRUE;
    } else {
        int count = 1;
        while(myFile[count].fd != 0) {
            count++;
        }
        tableId = count;
        strcpy(myFile[tableId].path, pathname);
        myFile[tableId].fd = open(pathname, O_RDWR | O_CREAT, 0600);
        myFile[tableId].is_used = TRUE;
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
    //fd[table_id] = 0;
    myFile[table_id].is_used = FALSE;
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
         controller[i]->is_ref = 0;
         controller[i]->next = NULL;
         controller[i]->prev = NULL;
     }
     controller[0]->next_freeNode_header = freeNumberNode;
     if(controller != NULL) {
         return 0;
     } else {
         return -1;
     }
}
int db_insert(int table_id, int64_t key, char * value) {
    record new_record;
    new_record.key = key;
    strcpy(new_record.value, value);
    if(insert_master(table_id, key, value) != NULL) {
        return 0;
    }
    return -1;
}
int db_find(int table_id, int64_t key, char * ret_val) {
    record* tmp = find(table_id, key);
    if(tmp != NULL) {
        strcpy(ret_val, tmp->value);
        return 0;
    }
    return -1;
}


int db_delete(int table_id, int64_t key) {
    if(delete_master(table_id, key) != NULL) {
        return 0;
    }
    return -1;
}


/* Prints the bottom row of keys
 * of the tree (with their respective
 * pointers, if the verbose_output flag is set.
 */

void print_leaves(int table_id) {
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
            printf("%lu ", cur->frame.record[i].key);
        }
        if (cur->frame.more_page_number != 0) {
            printf(" | ");
            cur_2 = buffer_write_page(table_id, cur->frame.more_page_number);
            cur->is_pinned = FALSE;
            cur = cur_2;
        }
        else
            break;
    }
    cur->is_pinned = FALSE;
    printf("\n");
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
record* find(int table_id, int key) {
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


// INSERTION

record * make_record(int key, char* value) {
    record * new_record = (record *)malloc(sizeof(record));
    if (new_record == NULL) {
        perror("Record creation.");
        exit(EXIT_FAILURE);
    }
    else {
        new_record->key = key;
        strcpy(new_record->value, value);
    }
    return new_record;
}


pagenum_t make_page(int table_id) {
    buffer_control_block* header_block= buffer_write_page(table_id, HEADER_PAGE_NUMBER);
    pagenum_t ret_num = buffer_alloc_page(table_id);
    buffer_control_block* tmp = buffer_write_page(table_id, ret_num);
    header_block = buffer_write_page(table_id, HEADER_PAGE_NUMBER);
    if(header_block->frame.root_page_number == 0) {
        header_block->frame.root_page_number = ret_num;
    }
    
    if (ret_num == 0) {
        perror("page creation.");
        exit(EXIT_FAILURE);
    }

    tmp->frame.is_leaf = 0;
    tmp->frame.number_of_keys = 0;
    tmp->frame.more_page_number = 0;
    tmp->is_dirty = TRUE;
    header_block->is_pinned = FALSE;
    tmp->is_pinned = FALSE;
    return ret_num;
}

pagenum_t make_leaf(int table_id) {
    pagenum_t leafPNum = make_page(table_id);
    buffer_control_block* tmp = buffer_write_page(table_id, leafPNum);
    tmp->frame.is_leaf = 1;
    tmp->is_dirty = TRUE;
    tmp->is_pinned = FALSE;
    return leafPNum;
}


/* Helper function used in insert_into_parent
 * to find the index of the parent's pointer to 
 * the node to the left of the key to be inserted.
 */
//�����������ѹ����������Լ�.
int get_left_index(int table_id, pagenum_t parent, pagenum_t left) {

    int left_index = 0;
    buffer_control_block* parent_block = buffer_write_page(table_id, parent);
    buffer_control_block* p = buffer_write_page(table_id, left);
    if(left == parent_block->frame.more_page_number) {
        p->is_pinned = FALSE;
        parent_block->is_pinned = FALSE;
        return left_index;
    } else {
        left_index = 1;
        while(left_index <= parent_block->frame.number_of_keys) {
            parent = parent_block->frame.keyPage[left_index - 1].pageNum;
            if(left != parent) {
                left_index++;
            } else {
                break;
            }
        }
    }
    parent_block->is_pinned = FALSE;
    p->is_pinned = FALSE;
    return left_index;
}

/* Inserts a new pointer to a record and its corresponding
 * key into a leaf.
 * Returns the altered leaf.
 */
buffer_control_block* insert_into_leaf(int table_id, pagenum_t leaf, int key, record * pointer ) {
    //printf("insert into leaf fucntion called\n");
    buffer_control_block* p = buffer_write_page(table_id, leaf);
    int i, insertion_point;
    insertion_point = 0;
    while (insertion_point < p->frame.number_of_keys && p->frame.record[insertion_point].key < key)
        insertion_point++;

    for (i = p->frame.number_of_keys; i > insertion_point; i--) {
        p->frame.record[i] = p->frame.record[i - 1];
    }
    p->frame.record[insertion_point] = *pointer;
    p->frame.number_of_keys++;
    p->is_dirty = TRUE;
    p->is_pinned = FALSE;
    return p;
}



buffer_control_block* insert_into_leaf_after_splitting(int table_id, pagenum_t leaf, int key, record * pointer) {
   // printf("insert into leaf after splitting fucntion called\n");
    pagenum_t new_leaf;
    int* temp_keys; 
    record* temp_pointers; 
    int insertion_index, split, new_key, i, j;

    buffer_control_block* p = buffer_write_page(table_id, leaf);
    new_leaf = make_leaf(table_id);
    buffer_control_block* q = buffer_write_page(table_id, new_leaf);
    temp_keys = malloc(32 * sizeof(int));
    if (temp_keys == NULL) {
        perror("Temporary keys array.");
        exit(EXIT_FAILURE);
    }

    temp_pointers = (record*)malloc(32 * sizeof(record));
    if (temp_pointers == NULL) {
        perror("Temporary pointers array.");
        exit(EXIT_FAILURE);
    }

    insertion_index = 0;
    while (insertion_index < 32 - 1 && p->frame.record[insertion_index].key < key)
        insertion_index++;

    for (i = 0, j = 0; i < p->frame.number_of_keys; i++, j++) {
        if (j == insertion_index) j++;
        temp_pointers[j] = p->frame.record[i];
    }

    temp_pointers[insertion_index] = *pointer;

    p->frame.number_of_keys = 0;

    split = cut(32 - 1);

    p->is_dirty = TRUE;
    for (i = 0; i < split; i++) {
        p->frame.record[i] = temp_pointers[i];
        p->frame.number_of_keys++;
    }

    q->is_dirty = TRUE;
    for (i = split, j = 0; i <= 31; i++, j++) {
        q->frame.record[j] = temp_pointers[i];
        q->frame.number_of_keys++;
    }
    free(temp_pointers);
    free(temp_keys);
    temp_pointers = NULL;
    temp_keys = NULL;
    q->frame.more_page_number = p->frame.more_page_number;
    p->frame.more_page_number = new_leaf; 

    q->frame.parent_page_number = p->frame.parent_page_number;
    new_key = q->frame.record[0].key;
    p->is_pinned = FALSE;
    q->is_pinned = FALSE;
    return insert_into_parent(table_id, p->page_num, new_key, q->page_num);
}


/* Inserts a new key and pointer to a node
 * into a node into which these can fit
 * without violating the B+ tree properties.
 */
buffer_control_block* insert_into_node(int table_id, pagenum_t parent, 
        int left_index, int key, pagenum_t right) {
            //printf("insert into node fucntion called\n");
    int i;
    buffer_control_block* parent_block = buffer_write_page(table_id, parent);
    buffer_control_block* q = buffer_write_page(table_id, right);
    for (i = parent_block->frame.number_of_keys; i > left_index; i--) {
        parent_block->frame.keyPage[i] = parent_block->frame.keyPage[i - 1];
    }
    parent_block->frame.number_of_keys++;
    parent_block->frame.keyPage[left_index].pageNum = right;
    q->frame.parent_page_number = parent;
    parent_block->frame.keyPage[left_index].key = key;
    parent_block->is_dirty = TRUE;
    q->is_dirty = TRUE;
    parent_block->is_pinned = FALSE;
    q->is_pinned = FALSE;
    return parent_block;
}


/* Inserts a new key and pointer to a node
 * into a node, causing the node's size to exceed
 * the order, and causing the node to split into two.
 */
buffer_control_block* insert_into_node_after_splitting(int table_id, pagenum_t old, int left_index, 
        int key, pagenum_t right) {
            //printf("insert into node after splitting fucntion called\n");
    int i, j, split, k_prime;
    pagenum_t new;
    buffer_control_block* old_block = buffer_write_page(table_id, old);
    int * temp_keys;
    key_pageNum* temp_pointers;

    /* First create a temporary set of keys and pointers
     * to hold everything in order, including
     * the new key and pointer, inserted in their
     * correct places. 
     * Then create a new node and copy half of the 
     * keys and pointers to the old node and
     * the other half to the new.
     */
    // old node�� parent�� ���Ѵ�.
    temp_pointers = (key_pageNum*)malloc((249 + 1) * sizeof(key_pageNum));
    if (temp_pointers == NULL) {
        perror("Temporary pointers array for splitting nodes.");
        exit(EXIT_FAILURE);
    }
    temp_keys = malloc(249 * sizeof(int));
    if (temp_keys == NULL) {
        perror("Temporary keys array for splitting nodes.");
        exit(EXIT_FAILURE);
    }

    for (i = 0, j = 0; i < old_block->frame.number_of_keys + 1; i++, j++) {
        if (j == left_index) j++;
        temp_pointers[j] = old_block->frame.keyPage[i];
    }

    key_pageNum tmp;
    tmp.key = key;
    tmp.pageNum = right;
    temp_pointers[left_index] = tmp;

    split = cut(249);
    new = make_page(table_id);
    buffer_control_block* new_block = buffer_write_page(table_id, new);
    new_block->is_dirty = TRUE;
    old_block->frame.number_of_keys = 0;
    old_block->is_dirty = TRUE;
    for (i = 0; i < split - 1; i++) {
        old_block->frame.keyPage[i] = temp_pointers[i];
        old_block->frame.number_of_keys++;
    }

    new_block->frame.more_page_number = temp_pointers[split - 1].pageNum;
    k_prime = temp_pointers[split - 1].key;
    for (++i, j = 0; i < 249; i++, j++) {
        new_block->frame.keyPage[j] = temp_pointers[i];
        new_block->frame.number_of_keys++;
    }
    free(temp_pointers);
    free(temp_keys);
    temp_pointers = NULL;
    temp_keys = NULL;
    new_block->frame.parent_page_number = old_block->frame.parent_page_number;
    i = 0;
    buffer_control_block* child_block = buffer_write_page(table_id, new_block->frame.more_page_number);
    child_block->is_dirty = TRUE;
    child_block->frame.parent_page_number = new;
    child_block->is_pinned = FALSE;
    for (i = 1; i <= new_block->frame.number_of_keys; i++) {
        child_block = buffer_write_page(table_id, new_block->frame.keyPage[i - 1].pageNum);
        child_block->is_dirty = TRUE;
        child_block->frame.parent_page_number = new;
        child_block->is_pinned = FALSE;
    }
    old_block->is_pinned = FALSE;
    new_block->is_pinned = FALSE;
    /* Insert a new key into the parent of the two
     * nodes resulting from the split, with
     * the old node to the left and the new to the right.
     */

    return insert_into_parent(table_id, old_block->page_num, k_prime, new);
}



/* Inserts a new node (leaf or internal node) into the B+ tree.
 * Returns the root of the tree after insertion.
 */
buffer_control_block* insert_into_parent(int table_id, pagenum_t left, int key, pagenum_t right) {
    //printf("insert into parent fucntion called\n");
    int left_index;
    pagenum_t parent; // p - left q - right r -parent
    buffer_control_block* p = buffer_write_page(table_id, left);
    parent = p->frame.parent_page_number;
    /* Case: new root. */

    if (parent == 0) {
        p->is_pinned = FALSE;
        return insert_into_new_root(table_id, left, key, right);
    }

    /* Case: leaf or node. (Remainder of
     * function body.)  
     */

    /* Find the parent's pointer to the left 
     * node.
     */
    
    buffer_control_block* r = buffer_write_page(table_id, parent);
    left_index = get_left_index(table_id, parent, left);
    p->is_pinned = FALSE;
    /* Simple case: the new key fits into the node. 
     */

    if (r->frame.number_of_keys < 249 - 1) {
        r->is_dirty = TRUE;
        r->is_pinned = FALSE;
        return insert_into_node(table_id, parent, left_index, key, right);
    }

    /* Harder case:  split a node in order 
     * to preserve the B+ tree properties.
     */
    r->is_dirty = TRUE;
    r->is_pinned = FALSE;
    return insert_into_node_after_splitting(table_id, parent, left_index, key, right);
}


/* Creates a new root for two subtrees
 * and inserts the appropriate key into
 * the new root.
 */
buffer_control_block* insert_into_new_root(int table_id, pagenum_t left, int key, pagenum_t right) {
    //printf("insert into new root fucntion called\n");
    pagenum_t root;
    root = make_page(table_id);
    buffer_control_block* h = buffer_write_page(table_id, HEADER_PAGE_NUMBER);
    buffer_control_block* root_page_buffer = buffer_write_page(table_id, root);
    buffer_control_block* p = buffer_write_page(table_id, left);
    buffer_control_block* q = buffer_write_page(table_id, right);
    h->frame.root_page_number = root;
    root_page_buffer->frame.keyPage[0].key = key;
    root_page_buffer->frame.more_page_number = left;
    root_page_buffer->frame.keyPage[0].pageNum = right;
    root_page_buffer->frame.number_of_keys++;
    root_page_buffer->frame.parent_page_number = 0;
    p->frame.parent_page_number = root;
    q->frame.parent_page_number = root;
    h->is_dirty = TRUE;
    root_page_buffer->is_dirty = TRUE;
    p->is_dirty = TRUE;
    q->is_dirty = TRUE;
    h-> is_pinned = FALSE;
    root_page_buffer->is_pinned = FALSE;
    p->is_pinned = FALSE;
    q->is_pinned = FALSE;
    return root_page_buffer;
}



/* First insertion:
 * start a new tree.
 */
buffer_control_block* start_new_tree(int table_id, int key, record* pointer) {
    buffer_control_block* head_block = buffer_write_page(table_id, HEADER_PAGE_NUMBER);
    //printf("start new tree\n");
    pagenum_t root;
    root = make_leaf(table_id);
    buffer_control_block* p = buffer_write_page(table_id, root);
    head_block->frame.root_page_number = root;
    head_block->is_dirty = TRUE;
    p->frame.parent_page_number = 0;
    p->frame.record[0] = *pointer;
    p->frame.number_of_keys = 1;
    p->frame.more_page_number = 0; //����������
    p->is_pinned = FALSE;
    head_block->is_pinned = FALSE;
    p->is_dirty = TRUE;
    return p;
}



/* Master insertion function.
 * Inserts a key and an associated value into
 * the B+ tree, causing the tree to be adjusted
 * however necessary to maintain the B+ tree
 * properties.
 */
//node * insert( node * root, int key, int value ) {
buffer_control_block* insert_master(int table_id, int key, char* value) {
    //printf("insert master fucntion called\n");
    record* record;
    buffer_control_block* leaf_block;
    page_t tmp;
    /* The current implementation ignores
     * duplicates.
     */
    if (find(table_id, key) != NULL) {
        return 0;
    }
    /* Create a new record for the
     * value.
     */
    record = make_record(key, value);


    /* Case: the tree does not exist yet.
     * Start a new tree.
     */

    buffer_control_block* p = buffer_write_page(table_id, HEADER_PAGE_NUMBER);
    if (p->frame.root_page_number == 0)  { 
        p->is_pinned = FALSE;
        return start_new_tree(table_id, key, record);
    }


    /* Case: the tree already exists.
     * (Rest of function body.)
     */

    leaf_block = find_leaf(table_id, key);
    /* Case: leaf has room for key and pointer.
     */
    if (leaf_block->frame.number_of_keys < 31) {
        leaf_block = insert_into_leaf(table_id, leaf_block->page_num, key, record);
        leaf_block->is_dirty = TRUE;
        leaf_block->is_pinned = FALSE;
        p->is_pinned = FALSE;
        return leaf_block;
    }


    /* Case:  leaf must be split.
     */

    p->is_pinned = FALSE;
    leaf_block->is_pinned = FALSE;
    return insert_into_leaf_after_splitting(table_id, leaf_block->page_num, key, record);
}



// DELETION.

/* Utility function for deletion.  Retrieves
 * the index of a node's nearest neighbor (sibling)
 * to the left if one exists.  If not (the node
 * is the leftmost child), returns -1 to signify
 * this special case.
 */
int get_neighbor_index(int table_id, pagenum_t n) {
    int i;
    pagenum_t parent;
    buffer_control_block* n_block = buffer_write_page(table_id, n);
    parent = n_block->frame.parent_page_number;
    n_block->is_pinned = FALSE;
    buffer_control_block* parent_block = buffer_write_page(table_id, parent);
    /* Return the index of the key to the left
     * of the pointer in the parent pointing
     * to n.  
     * If n is the leftmost child, this means
     * return -1.
     */
    for (i = 0; i <= parent_block->frame.number_of_keys; i++) {
        if(i == 0) {
            if(parent_block->frame.more_page_number == n) {
                parent_block->is_pinned = FALSE;
                return -1;
            }
        } else {
            if(parent_block->frame.keyPage[i - 1].pageNum == n) {
                parent_block->is_pinned = FALSE;
                return i - 1;
            }
        }
    }
    // Error state.
    printf("Search for nonexistent pointer to node in parent.\n");
    exit(EXIT_FAILURE);
}

//remove ��ȣ�� 0������ ũ�� �������� �����ϴ� �Լ�. 0�̸� record�� �����ϴ� �Լ�

buffer_control_block*remove_entry_from_node(int table_id, pagenum_t n, int key, pagenum_t remove) {
    //printf("remove from entry node\n");
    int i, num_pointers;
    buffer_control_block* n_block = buffer_write_page(table_id, n);
    // Remove the key and shift other keys accordingly.
    i = 0;
    if(remove > 0) {
        if(i == 0) {
            if(n_block->frame.more_page_number != remove) i++;
        } 
        if(i == 1) {
            while(n_block->frame.keyPage[i-1].pageNum != remove) {
                i++;
            }
        }
        for(i; i < n_block->frame.number_of_keys; i++) {
            if(i > 0) {
                n_block->frame.keyPage[i - 1] = n_block->frame.keyPage[i];
            }
        }
        //file_free_page(remove);
    } else {
        while (n_block->frame.record[i].key != key) {
            i++;
        }
        for (++i; i < n_block->frame.number_of_keys; i++) {
            n_block->frame.record[i - 1] = n_block->frame.record[i];
        }        
    }

    n_block->frame.number_of_keys--;
    n_block->is_dirty = TRUE;
    n_block->is_pinned = FALSE;
    return n_block;
}


buffer_control_block* adjust_root(int table_id) {
    //printf("adjust root");
    pagenum_t new_root;
    buffer_control_block* header_block = buffer_write_page(table_id, HEADER_PAGE_NUMBER);
    pagenum_t root_number = header_block->frame.root_page_number;
    buffer_control_block* root_block = buffer_write_page(table_id, root_number);
    buffer_control_block* new_root_block;
    /* Case: nonempty root.
     * Key and pointer have already been deleted,
     * so nothing to be done.
     */

    if (root_block->frame.number_of_keys > 0) {
        root_block->is_pinned = FALSE;
        header_block->is_pinned = FALSE;
        return root_block;
    }

    /* Case: empty root. 
     */

    // If it has a child, promote 
    // the first (only) child
    // as the new root.

    if (!root_block->frame.is_leaf) {
        new_root = root_block->frame.more_page_number;
        new_root_block = buffer_write_page(table_id, new_root);
        new_root_block->frame.parent_page_number = 0;
        new_root_block->is_dirty = TRUE;
        header_block->frame.root_page_number = new_root;
        header_block->is_dirty = TRUE;
    }

    // If it is a leaf (has no children),
    // then the whole tree is empty.

    else {
        new_root = 0;
        header_block->frame.root_page_number = 0;
        header_block->is_dirty = TRUE;
    }
    //file_free_page(root_number);
    header_block->is_pinned = FALSE;
    root_block->is_pinned = FALSE;
    new_root_block = buffer_write_page(table_id, new_root);
    new_root_block->is_dirty = TRUE;
    buffer_free_page(table_id, root_block->page_num);
    new_root_block->is_pinned = FALSE;
    return new_root_block;
}


/* Coalesces a node that has become
 * too small after deletion
 * with a neighboring node that
 * can accept the additional entries
 * without exceeding the maximum.
 */
buffer_control_block* coalesce_nodes(int table_id, pagenum_t n, pagenum_t neighbor, int neighbor_index, int k_prime) {


    //printf("coalesce node\n");
    int i, j, neighbor_insertion_index, n_end;
    pagenum_t tmp;
    buffer_control_block* n_block = buffer_write_page(table_id, n);
    buffer_control_block* neighbor_block = buffer_write_page(table_id, neighbor);

    /* Swap neighbor with node if node is on the
     * extreme left and neighbor is to its right.
     */

    if (neighbor_index == -1) {
        tmp = n;
        n = neighbor;
        neighbor = tmp;
        n_block = buffer_write_page(table_id, n);
        neighbor_block = buffer_write_page(table_id, neighbor);
        n_block->is_dirty = TRUE;
        neighbor_block->is_dirty = TRUE;
    }
    /* Starting point in the neighbor for copying
     * keys and pointers from n.
     * Recall that n and neighbor have swapped places
     * in the special case of n being a leftmost child.
     */

    neighbor_insertion_index = neighbor_block->frame.number_of_keys;

    /* Case:  nonleaf node.
     * Append k_prime and the following pointer.
     * Append all pointers and keys from the neighbor.
     */

    if (!n_block->frame.is_leaf) {
        neighbor_block->is_dirty = TRUE;


        n_end = n_block->frame.number_of_keys;

        n_block->is_dirty = TRUE;
        for (i = neighbor_insertion_index, j = 0; j < n_end; i++, j++) {
            neighbor_block->frame.keyPage[i] = n_block->frame.keyPage[j];
            neighbor_block->frame.number_of_keys++;
            n_block->frame.number_of_keys--;
        }

        /* The number of pointers is always
         * one more than the number of keys.
         */


        /* All children must now point up to the same parent.
         */
        
        buffer_control_block* tmp_block;
        for (i = 0; i <= neighbor_block->frame.number_of_keys; i++) {
            if(i == 0) {
                tmp = neighbor_block->frame.more_page_number;
            } else {
                tmp = neighbor_block->frame.keyPage[i - 1].pageNum;
            }    
            tmp_block = buffer_write_page(table_id, tmp);
            tmp_block->frame.parent_page_number = neighbor;
            tmp_block->is_dirty = TRUE;
            tmp_block->is_pinned = FALSE;
        } 
/*
        tmp = neighbor_block->frame.parent_page_number;
        int idx = get_neighbor_index(table_id, neighbor);
        if(idx == -1) {
            tmp_block = buffer_write_page(table_id, tmp);
            tmp_block->frame.more_page_number = neighbor;
            tmp_block->is_dirty = TRUE;
            tmp_block->is_pinned = FALSE;
        }
        */
    }

    /* In a leaf, append the keys and pointers of
     * n to the neighbor.
     * Set the neighbor's last pointer to point to
     * what had been n's right neighbor.
     */

    else {
        n_end = n_block->frame.number_of_keys;
        neighbor_block->is_dirty = TRUE;
        n_block->is_dirty = TRUE;
        for (i = neighbor_insertion_index, j = 0; j < n_end; i++, j++) {
            neighbor_block->frame.record[i] = n_block->frame.record[j];
            neighbor_block->frame.number_of_keys++;
            n_block->frame.number_of_keys--;
        }
        
        tmp = neighbor_block->frame.parent_page_number;
        buffer_control_block* tmp_block = buffer_write_page(table_id, tmp);
        int idx = get_neighbor_index(table_id, neighbor);
        if(idx == -1) {
            tmp_block->frame.more_page_number = neighbor;
            tmp_block->is_dirty = TRUE;
        }
        tmp_block->is_pinned = FALSE;
        neighbor_block->frame.more_page_number = n_block->frame.more_page_number;
    }
    buffer_control_block* ret = delete_entry(table_id, n_block->frame.parent_page_number, k_prime, n);



/*
    if(ret == neighbor) {
        file_read_page(neighbor, &neighbor_page);
    }
    file_write_page(neighbor, &neighbor_page);
    */
    //file_free_page(n);
    neighbor_block->is_pinned = FALSE;
    n_block->is_pinned = FALSE;
    buffer_free_page(table_id, n_block->page_num);
    return ret;
}
  
/* Deletes an entry from the B+ tree.
 * Removes the record and its key and pointer
 * from the leaf, and then makes all appropriate
 * changes to preserve the B+ tree properties.
 */

buffer_control_block* delete_entry(int table_id, pagenum_t n, int key, pagenum_t remove) {
    // n = key_leaf key = key remove�� 0�̸� ���ڵ�, 0���� ũ�� ������
    //printf("delete entry\n");
    int min_keys;
    pagenum_t neighbor, parent;
    int neighbor_index;
    int k_prime_index, k_prime;
    int capacity;

    buffer_control_block* header_block = buffer_write_page(table_id, HEADER_PAGE_NUMBER);
    buffer_control_block* n_block = buffer_write_page(table_id, n);

    // Remove key and pointer from node.
    n_block->is_dirty = TRUE;
    n_block = remove_entry_from_node(table_id, n, key, remove);
    /* Case:  deletion from the root. 
     */
    
    if (n_block->page_num == header_block->frame.root_page_number) {
        header_block->is_pinned = FALSE;
        n_block->is_pinned = FALSE;
        return adjust_root(table_id);
    }
    header_block->is_pinned = FALSE;
    /* Case:  deletion from a node below the root.
     * (Rest of function body.)
     */

    /* Determine minimum allowable size of node,
     * to be preserved after deletion.
     */

    //min_keys = n->is_leaf ? cut(32 - 1) : cut(249) - 1;

    min_keys = 1;

    /* Case:  node stays at or above minimum.
     * (The simple case.)
     */
    
    n_block = buffer_write_page(table_id, n);
    if (n_block->frame.number_of_keys >= min_keys) {
        n_block = buffer_write_page(table_id, n);
        n_block->is_dirty = TRUE;
        n_block->is_pinned = FALSE;
        return n_block;
    }
    /* Case:  node falls below minimum. == 0
     */

    /* Find the appropriate neighbor node with which
     * to coalesce.
     * Also find the key (k_prime) in the parent
     * between the pointer to node n and the pointer
     * to the neighbor.
     */
    
    neighbor_index = get_neighbor_index(table_id, n);
    parent = n_block->frame.parent_page_number;
    buffer_control_block* parent_block = buffer_write_page(table_id, parent);
    
    k_prime_index = neighbor_index == -1 ? 0 : neighbor_index;
    //k_prime = parent_block->frame.keyPage[k_prime_index].key;
    
    if(k_prime_index == 0) {
        k_prime = parent_block->frame.more_page_number;
    } else {
        k_prime = parent_block->frame.keyPage[k_prime_index - 1].key;
    }
    //k_prime = key;

    if(neighbor_index == -1) {
        neighbor = parent_block->frame.keyPage[0].pageNum;
    } else if(neighbor_index == 0) {
        neighbor = parent_block->frame.more_page_number;
    } else {
        neighbor = parent_block->frame.keyPage[neighbor_index - 1].pageNum;
    }
    parent_block->is_pinned = FALSE;
    n_block->is_pinned = FALSE;
    /* Coalescence. */
    return coalesce_nodes(table_id, n, neighbor, neighbor_index, k_prime);
}



/* Master deletion function.
 */


buffer_control_block* delete_master(int table_id, int key) {
    buffer_control_block* p;
    pagenum_t key_leaf;
    record* key_record;
    key_record = find(table_id, key);
    p = find_leaf(table_id, key);
    if(p == NULL) return NULL;
    key_leaf = p->page_num;
    if (key_record != NULL && key_leaf != 0) {
        delete_entry(table_id, key_leaf, key, 0);
    } else {
        return NULL;
    }
    return p;
}
