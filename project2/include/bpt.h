#ifndef __BPT_H__
#define __BPT_H__

// Uncomment the line below if you are compiling on Windows.
// #define WINDOWS
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#ifdef WINDOWS
#define bool char
#define false 0
#define true 1
#endif

// Constants for printing part or all of the GPL license.
#define LICENSE_FILE "LICENSE.txt"
#define LICENSE_WARRANTEE 0
#define LICENSE_WARRANTEE_START 592
#define LICENSE_WARRANTEE_END 624
#define LICENSE_CONDITIONS 1
#define LICENSE_CONDITIONS_START 70
#define LICENSE_CONDITIONS_END 625

// TYPES.

/* Type representing the record
 * to which a given key refers.
 * In a real B+ tree system, the
 * record would hold data (in a database)
 * or a file (in an operating system)
 * or some other information.
 * Users can rewrite this part of the code
 * to change the type and content
 * of the value field.
 */

/* Type representing a node in the B+ tree.
 * This type is general enough to serve for both
 * the leaf and the internal node.
 * The heart of the node is the array
 * of keys and the array of corresponding
 * pointers.  The relation between keys
 * and pointers differs between leaves and
 * internal nodes.  In a leaf, the index
 * of each key equals the index of its corresponding
 * pointer, with a maximum of order - 1 key-pointer
 * pairs.  The last pointer points to the
 * leaf to the right (or NULL in the case
 * of the rightmost leaf).
 * In an internal node, the first pointer
 * refers to lower nodes with keys less than
 * the smallest key in the keys array.  Then,
 * with indices i starting at 0, the pointer
 * at i + 1 points to the subtree with keys
 * greater than or equal to the key in this
 * node at index i.
 * The num_keys field is used to keep
 * track of the number of valid keys.
 * In an internal node, the number of valid
 * pointers is always num_keys + 1.
 * In a leaf, the number of valid pointers
 * to data is always num_keys.  The
 * last leaf pointer points to the next leaf.
 */

#define HEADER_PAGE_NUMBER 0
#define HEADER_PAGE_OFFSET 0
#define PAGE_SIZE 8000

typedef uint64_t pagenum_t;
typedef uint64_t offset_t;

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
pagenum_t find_leaf(int key);
record *find(int key);
int cut(int length);

// Insertion.

record *make_record(int key, char *value);
pagenum_t make_node(void);
pagenum_t make_leaf(void);
int get_left_index(pagenum_t parent, pagenum_t left);
pagenum_t insert_into_leaf(pagenum_t leaf, int key, record *pointer);
pagenum_t insert_into_leaf_after_splitting(pagenum_t leaf, int key, record *pointer);
pagenum_t insert_into_node(pagenum_t parent,
                         int left_index, int key, pagenum_t right);
pagenum_t insert_into_node_after_splitting(pagenum_t old_node, int left_index,
                                         int key, pagenum_t right);
pagenum_t insert_into_parent(pagenum_t left, int key, pagenum_t right);
pagenum_t insert_into_new_root(pagenum_t left, int key, pagenum_t right);
pagenum_t start_new_tree(int key, record *pointer);
pagenum_t insert(int key, char *value);

// Deletion.

int get_neighbor_index(pagenum_t n);
pagenum_t remove_entry_from_node(pagenum_t n, int key, pagenum_t remove);
pagenum_t adjust_root();
pagenum_t coalesce_nodes(pagenum_t n, pagenum_t neighbor, int neighbor_index, int k_prime);
pagenum_t delete_entry(pagenum_t n, int key, pagenum_t remove);
pagenum_t delete_master (int key);
/*
void destroy_tree_nodes(node *root);
node *destroy_tree(node *root);
*/
// myFunction.
pagenum_t file_alloc_page();
void file_free_page(pagenum_t pagenum);
void file_read_page(pagenum_t pagenum, page_t *dest);
void file_write_page(pagenum_t pagenum, const page_t *src);
int open_table(char *pathname);
int db_insert(int64_t key, char *value);
int db_find(int64_t key, char *ret_val);
int db_delete(int64_t key);
pagenum_t getPageNumber(page_t *target);
page_t *getPageNumberWithPtr(page_t *new_root, page_t *target, pagenum_t *pNum);


/*
offset_t get_page_offset(page_t* page, int index);
pagenum_t get_offset_by_pageNumber(pagenum_t pagenum);
offset_t get_pagenumber_by_offset(offset offset);
offset_t get_root_page_offset(page_t* header);
offset_t get_free_page_offset(page_t* header);
pagenum_t get_number_of_pages(page_t* header);
offset_t get_parent_page_offset(page_t* child_page);
offset_t get_next_free_page_offset(page_t* free_page);

int set_page_offset(page_t* page, int index, offset_t offset);
int set_root_page_offset(page_t* header, offset_t offset);
int set_free_page_offset(page_t* header, offset_t offset);
int set_number_of_pages(page_t* header, int size);
int set_parent_page_offset(page_t* child_page, offset_t offset);
int set_next_free_page_offset(page_t* free_page, offset_t offset);
*/
#endif /* __BPT_H__*/
