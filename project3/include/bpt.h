#ifndef __BPT_H__
#define __BPT_H__

// Uncomment the line below if you are compiling on Windows.
// #define WINDOWS
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#define FALSE 0
#define TRUE 1

#define HEADER_PAGE_NUMBER 0
#define HEADER_PAGE_OFFSET 0
#define PAGE_SIZE 1000

typedef uint64_t pagenum_t;
typedef uint64_t offset_t;

typedef struct My_File {
    int fd;
    char path[30];
    bool is_used;
} MyFile_t;
typedef struct record
{
    int64_t key;
    char value[120];
} record;

typedef struct key_pageNum
{
    pagenum_t key;
    pagenum_t pageNum;
} key_pageNum;

typedef enum
{
    PAGE_INTERNAL = 0,
    PAGE_LEAF = 1
} PageType;

typedef struct page_t
{
    union {
        // internal or leaf page
        struct
        {
            pagenum_t parent_page_number;
            PageType is_leaf;
            int number_of_keys;
            char reserve[128 - 24];
            pagenum_t more_page_number;
            union {
                record record[31];
                key_pageNum keyPage[248];
            };
        };
        // header page
        struct
        {
            pagenum_t free_page_number;
            pagenum_t root_page_number;
            pagenum_t number_of_pages;
            char reserve2[4096 - 24];
        };
        // free page
        struct
        {
            pagenum_t next;
            char reserve3[4096 - 8];
        };
    };
} page_t;

typedef struct freeNumber {
    int free;
    bool is_used;
    struct freeNumber* next;
}freeNumber;

typedef struct buffer_control_block {
    page_t frame;
    int table_id;
    freeNumber* next_freeNode_header;
    pagenum_t page_num;
    bool is_dirty;
    bool is_pinned;
    bool is_ref;
    struct buffer_control_block* next;
    struct buffer_control_block* prev;
}buffer_control_block;

void usage_2(void);
void usage_3(void);
/*
int height(node *root);
int path_to_root(node *root, node *child);
*/
void print_leaves();
void print_tree();
/*
void find_and_print(node *root, int key, bool verbose);
void find_and_print_range(node *root, int range1, int range2, bool verbose);
int find_range(node *root, int key_start, int key_end, bool verbose,
               int returned_keys[], void *returned_pointers[]);
               */
buffer_control_block* find_leaf(int table_id, int key);
record* find(int table_id, int key);
int cut(int length);

// Insertion.

record *make_record(int key, char *value);
pagenum_t make_node(int table_id);
pagenum_t make_leaf(int table_id);
int get_left_index(int table_id, pagenum_t parent, pagenum_t left);
buffer_control_block* insert_into_leaf(int table_id, pagenum_t leaf, int key, record *pointer);
buffer_control_block* insert_into_leaf_after_splitting(int table_id, pagenum_t leaf, int key, record *pointer);
buffer_control_block*insert_into_node(int table_id, pagenum_t parent,
                         int left_index, int key, pagenum_t right);
buffer_control_block* insert_into_node_after_splitting(int table_id, pagenum_t old_node, int left_index,
                                         int key, pagenum_t right);
buffer_control_block* insert_into_parent(int table_id, pagenum_t left, int key, pagenum_t right);
buffer_control_block* insert_into_new_root(int table_id, pagenum_t left, int key, pagenum_t right);
buffer_control_block* start_new_tree(int table_id, int key, record *pointer);
buffer_control_block* insert_master(int table_id, int key, char *value);

// Deletion.

int get_neighbor_index(int table_id, pagenum_t n);
buffer_control_block* remove_entry_from_node(int table_id, pagenum_t n, int key, pagenum_t remove);
buffer_control_block* adjust_root(int table_id);
buffer_control_block* coalesce_nodes(int table_id, pagenum_t n, pagenum_t neighbor, int neighbor_index, int k_prime);
buffer_control_block* delete_entry(int table_id, pagenum_t n, int key, pagenum_t remove);
buffer_control_block* delete_master (int table_id, int key);
/*
void destroy_tree_nodes(node *root);
node *destroy_tree(node *root);
*/
// myFunction.
pagenum_t file_alloc_page(int table_id);
void file_free_page(int table_id, pagenum_t pagenum);
void file_read_page(int table_id, pagenum_t pagenum, page_t *dest);
void file_write_page(int table_id, pagenum_t pagenum, const page_t *src);
int open_table(char *pathname);
int db_insert(int table_id, int64_t key, char *value);
int db_find(int table_id, int64_t key, char *ret_val);
int db_delete(int table_id, int64_t key);

int close_table(int table_id);
int init_db(int num_buf);
void append_freeNumberNode(int num_buf, int i);
int pop_freeNumberNode(int i);
void buffer_put_page(buffer_control_block* blk);
buffer_control_block* buffer_check_page(int table_id, pagenum_t pageNum);
//void buffer_free_page(int table_id, buffer_control_block* blk) ;
void buffer_free_page(int table_id, pagenum_t pageNum) ;
buffer_control_block* buffer_write_page(int table_id, pagenum_t pageNum);
buffer_control_block* evict_block();


#endif /* __BPT_H__*/
