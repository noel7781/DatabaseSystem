#define Version "1.14"


#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <time.h>
#include "bpt.h"
// GLOBALS.

pagenum_t last_alloc_number;
pagenum_t found_leaf_number = 1;

page_t head;
page_t free_page;
record* rec;
void usage_2( void ) {
    printf("1. open <pathame> \n2. insert <key, value> \n3. delete <key> \n4. find <key>\n5. print\n\n");
}

//파일서술자 0으로 초기화.

int fd = 0;

pagenum_t get_root_pagenumber(page_t* header) {
    return header->root_page_number;
}
pagenum_t get_free_pagenumber(page_t* header) {
    return header->free_page_number;
}
pagenum_t get_number_of_pages(page_t* header) {
    return header->number_of_pages;
}
pagenum_t get_parent_pagenumber(page_t* child_page) {
    return child_page->parent_page_number;
}
pagenum_t get_next_free_pagenumber(page_t* free_page) {
    return free_page->next;
}

int set_root_pagenumber(page_t* header, pagenum_t pagenum) {
    if(header != NULL) {
        header->root_page_number = pagenum;
        return 1;
    } else {
        return 0;
    }
}
int set_free_pagenumber(page_t* header, pagenum_t pagenum) {
    if(header != NULL) {
        header->free_page_number = pagenum;
        return 1;
    } else {
        return 0;
    }
}
int set_number_of_pages(page_t* header, int size) {
    if(header != NULL) {
        header->number_of_pages = size;
        return 1;
    } else {
        return 0;
    }
}
int set_parent_pagenumbert(page_t* child_page, pagenum_t pagenum) {
    child_page->parent_page_number = pagenum;
    return 1;
}
int set_next_free_pagenumber(page_t* free_page, pagenum_t pagenum) {
    free_page->next = pagenum;
    return 1;
}
int set_header(page_t* header) {
    header->free_page_number = 1;
    header->root_page_number = 0;
    header->number_of_pages = 8000;
}

// Allocate an on-disk page from the free page list 
pagenum_t file_alloc_page() {
    file_read_page(HEADER_PAGE_NUMBER, &head);
    pagenum_t p1 = head.free_page_number;
    page_t tmp;
    file_read_page(p1, &tmp);
    pagenum_t p2 = tmp.next;
    head.free_page_number = p2;
    file_write_page(HEADER_PAGE_NUMBER, &head);
    return p1;
}
// Free an on-disk page to the free page list 
// 프리페이지를 추가시켜준다.
void file_free_page(pagenum_t pagenum) {
    file_read_page(HEADER_PAGE_NUMBER, &head);
    page_t tmp_page;
    file_read_page(pagenum, &tmp_page);
    pagenum_t tmp = head.free_page_number;
    head.free_page_number = pagenum;
    memset(&tmp_page, 0, sizeof(page_t));
    tmp_page.next = tmp;
    file_write_page(HEADER_PAGE_NUMBER, &head);
    file_write_page(pagenum, &tmp_page);
}
// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(pagenum_t pagenum, page_t* dest) {
    int size = 0;
    if((size = pread(fd, dest, 4096, pagenum * 4096)) == - 1) {
        perror("pread");
    }
}
// Write an in-memory page(src) to the on-disk page
void file_write_page(pagenum_t pagenum, const page_t* src) {
    int size = 0;
    if((size = pwrite(fd, src, 4096, pagenum * 4096)) == -1) {
        perror("pwrite");
    }
    fsync(fd);
}

int open_table(char *pathname) {
    int delay_plus = 0;
    if(access(pathname, F_OK) == -1) {
        delay_plus = 1;
    }
    fd = open(pathname, O_RDWR | O_CREAT, 0600);
    if(fd < 0) {
        return -1;
    }
    if (delay_plus == 1) { // 처음 파일을 연다면
        printf("DEFAULT PAGE SIZE 8000\n");
        set_header(&head);
        file_write_page(HEADER_PAGE_NUMBER, &head);
        free_page.next = 2;
        file_write_page(head.free_page_number, &free_page);
        for(int i = 2; i < 8000; i++) {
            free_page.next = i + 1;
            file_write_page(i, &free_page);
        }
    }
    file_read_page(HEADER_PAGE_NUMBER, &head);
    return (int)time(NULL);
}

int db_insert(int64_t key, char * value) {
    record new_record;
    new_record.key = key;
    strcpy(new_record.value, value);
    if(insert(key, value) != 0) {
        return 0;
    }
    return -1;
}
int db_find(int64_t key, char * ret_val) {
    record* tmp = find(key);
    if(tmp != NULL) {
        strcpy(ret_val, tmp->value);
        return 0;
    }
    return -1;
}

int db_delete(int64_t key) {
    if(delete_master(key) != 0) {
        return 0;
    }
    return -1;
}


/* Prints the bottom row of keys
 * of the tree (with their respective
 * pointers, if the verbose_output flag is set.
 */
void print_leaves() {
    int i;
    file_read_page(HEADER_PAGE_NUMBER, &head);
    if (head.root_page_number == 0) {
        printf("Empty tree.\n");
        return;
    }
    page_t c;
    file_read_page(head.root_page_number, &c);
    while (!c.is_leaf) {
        file_read_page(c.more_page_number, &c);
    }
    while (true) {
        for (i = 0; i < c.number_of_keys; i++) {
            printf("%lu ", c.record[i].key);
        }
        if (c.more_page_number != 0) {
            printf(" | ");
            file_read_page(c.more_page_number, &c);
        }
        else
            break;
    }
    printf("\n");
}

// 해당 키를 가진 leaf 페이지를 리턴해주는 함수.
pagenum_t find_leaf(int key) {
    int i = 0;
    pagenum_t leaf_page_number;
    file_read_page(HEADER_PAGE_NUMBER, &head);
    if (head.root_page_number == 0) {
        printf("Empty tree.\n");
        return 0;
    }
    page_t c;
    file_read_page(head.root_page_number, &c);
    if(c.is_leaf) {
        leaf_page_number = head.root_page_number;
    }
    while (!c.is_leaf) {
        i = 0;
        while (i < c.number_of_keys) {
            if (key >= c.keyPage[i].key) i++;
            else break;
        }
        if(i == 0) {
            leaf_page_number = c.more_page_number;
            file_read_page(leaf_page_number, &c);
        } else {
            leaf_page_number = c.keyPage[i - 1].pageNum;
            file_read_page(leaf_page_number, &c);
        }
    }
    return leaf_page_number;
}


/* Finds and returns the record to which
 * a key refers.
 */
record* find(int key) {
    int i = 0;
    page_t tmp;
    pagenum_t leafPNum = find_leaf(key);
    file_read_page(leafPNum, &tmp);
    if (leafPNum == 0) return NULL;
    for (i = 0; i < tmp.number_of_keys; i++)
        if (tmp.record[i].key == key) break;
    if (i == tmp.number_of_keys) {
        return NULL;
    }
    else {
        rec = &tmp.record[i];
    }
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

//레코드를 만드는 함수(key, value)
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


//free 페이지에서 하나의 페이지를 가져와서 internal 페이지로 만드는 함수.
pagenum_t make_page( void ) {
    file_read_page(HEADER_PAGE_NUMBER, &head);
    page_t tmp;
    pagenum_t ret_num = file_alloc_page();
    file_read_page(ret_num, &tmp);
    
    if(head.root_page_number == 0) {
        head.root_page_number = ret_num; //처음 페이지를 만들때만 루트페이지로 할당.
    }
    
    if (ret_num == 0) {
        perror("page creation.");
        exit(EXIT_FAILURE);
    }

    tmp.is_leaf = 0;
    tmp.number_of_keys = 0;
    tmp.more_page_number = 0;

    file_write_page(HEADER_PAGE_NUMBER, &head);
    file_write_page(ret_num, &tmp);
    return ret_num;
}
//internal 페이지로 설정한 페이지를 leaf페이지로 바꾸는 함수
pagenum_t make_leaf( void ) {
    pagenum_t leafPNum = make_page();
    page_t tmp;
    file_read_page(leafPNum, &tmp);
    tmp.is_leaf = 1;
    file_write_page(leafPNum, &tmp);
    return leafPNum;
}


/* Helper function used in insert_into_parent
 * to find the index of the parent's pointer to 
 * the node to the left of the key to be inserted.
 */
//왼쪽페이지넘버가져오는함수.
int get_left_index(pagenum_t parent, pagenum_t left) {

    int left_index = 0;
    page_t parent_page, left_page;
    file_read_page(parent, &parent_page);
    file_read_page(left, &left_page);
    //file_read_page(parent->more_page_number, tmp);
    if(left == parent_page.more_page_number) {
        return left_index;
    } else {
        left_index = 1;
        while(left_index < parent_page.number_of_keys) {
            parent = parent_page.keyPage[left_index - 1].pageNum;
            if(left != parent) {
                left_index++;
            } else {
                break;
            }
        }
    }
    return left_index;
}

/* Inserts a new pointer to a record and its corresponding
 * key into a leaf.
 * Returns the altered leaf.
 */
pagenum_t insert_into_leaf(pagenum_t leaf, int key, record * pointer ) {
    page_t tmp;
    file_read_page(leaf, &tmp);
    int i, insertion_point;

    insertion_point = 0;
    while (insertion_point < tmp.number_of_keys && tmp.record[insertion_point].key < key)
        insertion_point++;

    for (i = tmp.number_of_keys; i > insertion_point; i--) {
        tmp.record[i] = tmp.record[i - 1];
    }
    tmp.record[insertion_point] = *pointer;
    tmp.number_of_keys++;
    file_write_page(leaf, &tmp);
    return leaf;
}



pagenum_t insert_into_leaf_after_splitting(pagenum_t leaf, int key, record * pointer) {
    pagenum_t new_leaf;
    page_t left, right;
    int* temp_keys; // key배열담는 변수
    record* temp_pointers; //recrod배열 담는 변수
    int insertion_index, split, new_key, i, j;

    file_read_page(leaf, &left);
    new_leaf = make_leaf();
    file_read_page(new_leaf, &right);

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
    while (insertion_index < 32 - 1 && left.record[insertion_index].key < key)
        insertion_index++;

    for (i = 0, j = 0; i < left.number_of_keys; i++, j++) {
        if (j == insertion_index) j++;
        temp_pointers[j] = left.record[i];
    }

    temp_pointers[insertion_index] = *pointer;

    left.number_of_keys = 0;

    split = cut(32 - 1);

    for (i = 0; i < split; i++) {
        left.record[i] = temp_pointers[i];
        left.number_of_keys++;
    }

    for (i = split, j = 0; i <= 31; i++, j++) {
        right.record[j] = temp_pointers[i];
        right.number_of_keys++;
    }
    free(temp_pointers);
    free(temp_keys);
    temp_pointers = NULL;
    temp_keys = NULL;
    right.more_page_number = left.more_page_number;
    left.more_page_number = new_leaf; 

    right.parent_page_number = left.parent_page_number;
    new_key = right.record[0].key;
    file_write_page(leaf, &left);
    file_write_page(new_leaf, &right);
    
    return insert_into_parent(leaf, new_key, new_leaf);
}


/* Inserts a new key and pointer to a node
 * into a node into which these can fit
 * without violating the B+ tree properties.
 */
pagenum_t insert_into_node(pagenum_t parent, 
        int left_index, int key, pagenum_t right) {
    int i;
    page_t parent_page, right_page;
    file_read_page(parent, &parent_page);
    file_read_page(right, &right_page);
    for (i = parent_page.number_of_keys; i > left_index; i--) {
        parent_page.keyPage[i] = parent_page.keyPage[i - 1];
        //n->keys[i] = n->keys[i - 1];
    }
    parent_page.number_of_keys++;
    parent_page.keyPage[left_index].pageNum = right;
    right_page.parent_page_number = parent;
    parent_page.keyPage[left_index].key = key;
    file_write_page(parent, &parent_page);
    file_write_page(right, &right_page);
    return parent;
}


/* Inserts a new key and pointer to a node
 * into a node, causing the node's size to exceed
 * the order, and causing the node to split into two.
 */
pagenum_t insert_into_node_after_splitting(pagenum_t old, int left_index, 
        int key, pagenum_t right) {
    int i, j, split, k_prime;
    page_t old_page, new_page, right_page, child_page;
    pagenum_t new;
    file_read_page(old, &old_page);
    file_read_page(right, &right_page);
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
    // old node는 parent를 뜻한다.
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

    for (i = 0, j = 0; i < old_page.number_of_keys + 1; i++, j++) {
        if (j == left_index) j++;
        //temp_pointers[j] = &page[old_node->keyPage[i].pageNum];
        temp_pointers[j] = old_page.keyPage[i];
    }

    key_pageNum tmp;
    tmp.key = key;
    tmp.pageNum = right;
    temp_pointers[left_index] = tmp;

    split = cut(249);
    new = make_page();
    file_read_page(new, &new_page);
    old_page.number_of_keys = 0;
    for (i = 0; i < split - 1; i++) {
        old_page.keyPage[i] = temp_pointers[i];
        old_page.number_of_keys++;
    }

    new_page.more_page_number = temp_pointers[split - 1].pageNum;
    k_prime = temp_pointers[split - 1].key;
    for (++i, j = 0; i < 249; i++, j++) {
        new_page.keyPage[j] = temp_pointers[i];
        new_page.number_of_keys++;
    }
    free(temp_pointers);
    free(temp_keys);
    temp_pointers = NULL;
    temp_keys = NULL;
    new_page.parent_page_number = old_page.parent_page_number;
    file_write_page(old, &old_page);
    file_write_page(new, &new_page);
    i = 0;
    
    file_read_page(new_page.more_page_number, &child_page);
    child_page.parent_page_number = new;
    pagenum_t child_pgNum = new_page.more_page_number;
    file_write_page(child_pgNum, &child_page);
    for (i = 1; i <= new_page.number_of_keys; i++) {
        file_read_page(new_page.keyPage[i - 1].pageNum, &child_page);
        child_page.parent_page_number = new;
        child_pgNum = new_page.keyPage[i - 1].pageNum;
        file_write_page(child_pgNum, &child_page);

    }
    
    /* Insert a new key into the parent of the two
     * nodes resulting from the split, with
     * the old node to the left and the new to the right.
     */

    return insert_into_parent(old, k_prime, new);
}



/* Inserts a new node (leaf or internal node) into the B+ tree.
 * Returns the root of the tree after insertion.
 */
pagenum_t insert_into_parent(pagenum_t left, int key, pagenum_t right) {
    int left_index;
    page_t parent_page;
    page_t left_page, right_page;
    pagenum_t parent; // 부모노드의 페이지번호
    file_read_page(left, &left_page);
    file_read_page(right, &right_page);
    parent = left_page.parent_page_number;
    file_read_page(parent, &parent_page);
    /* Case: new root. */

    if (parent == 0)
        //parent = insert_into_new_root(left, key , right);
        //return parent;
        return insert_into_new_root(left, key, right);

    /* Case: leaf or node. (Remainder of
     * function body.)  
     */

    /* Find the parent's pointer to the left 
     * node.
     */
    
    left_index = get_left_index(parent, left);

    /* Simple case: the new key fits into the node. 
     */

    if (parent_page.number_of_keys < 249 - 1)
        return insert_into_node(parent, left_index, key, right);

    /* Harder case:  split a node in order 
     * to preserve the B+ tree properties.
     */

    return insert_into_node_after_splitting(parent, left_index, key, right);
}


/* Creates a new root for two subtrees
 * and inserts the appropriate key into
 * the new root.
 */
pagenum_t insert_into_new_root(pagenum_t left, int key, pagenum_t right) {
    pagenum_t root;
    page_t root_page, left_page, right_page;
    root = make_page();
    file_read_page(HEADER_PAGE_NUMBER, &head);
    file_read_page(root, &root_page);
    file_read_page(left, &left_page);
    file_read_page(right, &right_page);
    head.root_page_number = root;
    root_page.keyPage[0].key = key;
    root_page.more_page_number = left;
    root_page.keyPage[0].pageNum = right;
    root_page.number_of_keys++;
    root_page.parent_page_number = 0;
    left_page.parent_page_number = root;
    right_page.parent_page_number = root;
    file_write_page(HEADER_PAGE_NUMBER, &head);
    file_write_page(root, &root_page);
    file_write_page(left, &left_page);
    file_write_page(right, &right_page);
    return root;
}



/* First insertion:
 * start a new tree.
 */
pagenum_t start_new_tree(int key, record* pointer) {
    file_read_page(HEADER_PAGE_NUMBER, &head);
    printf("start new tree\n");
    pagenum_t root;
    page_t tmp;
    root = make_leaf();
    file_read_page(root, &tmp);
    tmp.parent_page_number = 0; //부모노드가 존재하지 않는다.
    tmp.record[0] = *pointer;
    tmp.number_of_keys = 1;
    tmp.more_page_number = 0; //유일페이지
    file_write_page(root, &tmp);
    file_write_page(0, &head);
    return root;
}



/* Master insertion function.
 * Inserts a key and an associated value into
 * the B+ tree, causing the tree to be adjusted
 * however necessary to maintain the B+ tree
 * properties.
 */
//node * insert( node * root, int key, int value ) {
pagenum_t insert(int key, char* value) {
    record* record;
    pagenum_t leaf;
    page_t tmp;
    /* The current implementation ignores
     * duplicates.
     */
    
    file_read_page(HEADER_PAGE_NUMBER, &head);
    if (find(key) != NULL)
        return 0;

    /* Create a new record for the
     * value.
     */
    record = make_record(key, value);


    /* Case: the tree does not exist yet.
     * Start a new tree.
     */

    if (head.root_page_number == 0)  { 
        return start_new_tree(key, record);
    }


    /* Case: the tree already exists.
     * (Rest of function body.)
     */

    leaf = find_leaf(key);
    /* Case: leaf has room for key and pointer.
     */
    file_read_page(leaf, &tmp);
    if (tmp.number_of_keys < 31) {
        leaf = insert_into_leaf(leaf, key, record);
        return leaf;
    }


    /* Case:  leaf must be split.
     */

    return insert_into_leaf_after_splitting(leaf, key, record);
}


// DELETION.

/* Utility function for deletion.  Retrieves
 * the index of a node's nearest neighbor (sibling)
 * to the left if one exists.  If not (the node
 * is the leftmost child), returns -1 to signify
 * this special case.
 */
int get_neighbor_index(pagenum_t n) {
    int i;
    page_t tmp, parent_page;
    pagenum_t parent;
    file_read_page(n, &tmp);
    parent = tmp.parent_page_number;
    file_read_page(parent, &parent_page);
    /* Return the index of the key to the left
     * of the pointer in the parent pointing
     * to n.  
     * If n is the leftmost child, this means
     * return -1.
     */
    for (i = 0; i <= parent_page.number_of_keys; i++) {
        if(i == 0) {
            if(parent_page.more_page_number == n) {
                return -1;
            }
        } else {
            if(parent_page.keyPage[i - 1].pageNum == n) {
                return i - 1;
            }
        }
    }
    // Error state.
    printf("Search for nonexistent pointer to node in parent.\n");
    exit(EXIT_FAILURE);
}

//remove 번호가 0번보다 크면 페이지를 삭제하는 함수. 0이면 record를 삭제하는 함수

pagenum_t remove_entry_from_node(pagenum_t n, int key, pagenum_t remove) {
    int i, num_pointers;
    page_t tmp;
    page_t remove_page;
    file_read_page(n, &tmp);
    // Remove the key and shift other keys accordingly.
    i = 0;
    if(remove > 0) {
        if(i == 0) {
            if(tmp.more_page_number != remove) i++;
        } 
        if(i == 1) {
            while(tmp.keyPage[i-1].pageNum != remove) {
                i++;
            }
        }
        for(i; i < tmp.number_of_keys; i++) {
            if(i > 0) {
                tmp.keyPage[i - 1] = tmp.keyPage[i];
            }
        }
        //file_free_page(remove);
    } else {
        while (tmp.record[i].key != key) {
            i++;
        }
        for (++i; i < tmp.number_of_keys; i++) {
            tmp.record[i - 1] = tmp.record[i];
        }        
    }

    //안돼면아래꺼로 복귀.
    /*
    if(!tmp.is_leaf) {
        if(i == 0) {
            if(&page[n->more_page_number] != pointer) i++;
        } 
        if(i == 1) {
            while(&page[n->keyPage[i-1].pageNum] != pointer) {
                i++;
            }
        }
        for(i; i < n->number_of_keys; i++) {
            if(i > 0) {
                n->keyPage[i - 1] = n->keyPage[i];
            }
        }
    } else {
        while (n->record[i].key != key) {
            i++;
        }
        for (++i; i < n->number_of_keys; i++) {
            n->record[i - 1] = n->record[i];
        }
    }
    */
    // One key fewer.
    tmp.number_of_keys--;

    // Set the other pointers to NULL for tidiness.
    // A leaf uses the last pointer to point to the next leaf.
    /*
    if (n->is_leaf)
        for (i = n->number_of_keys; i < 32 - 1; i++)
            n->record[i] = NULL;
    else
        for (i = n->number_of_keys + 1; i < 249; i++) {
            n->keyPage[i] = NULL;
        }
*/
    file_write_page(n, &tmp);
    return n;
}


pagenum_t adjust_root() {
    file_read_page(HEADER_PAGE_NUMBER, &head);
    pagenum_t new_root;
    pagenum_t root_number = head.root_page_number;
    page_t root_page, new_root_page;
    file_read_page(root_number, &root_page);

    /* Case: nonempty root.
     * Key and pointer have already been deleted,
     * so nothing to be done.
     */

    if (root_page.number_of_keys > 0) {
        return root_number;
    }

    /* Case: empty root. 
     */

    // If it has a child, promote 
    // the first (only) child
    // as the new root.

    if (!root_page.is_leaf) {
        new_root = root_page.more_page_number;
        file_read_page(new_root,&new_root_page);
        new_root_page.parent_page_number = 0;
        head.root_page_number = new_root; // 루트번호바꾸기
        file_write_page(new_root, &new_root_page);
    }

    // If it is a leaf (has no children),
    // then the whole tree is empty.

    else {
        new_root = 0;
        head.root_page_number = 0;
    }
    file_write_page(new_root, &new_root_page);
    file_write_page(HEADER_PAGE_NUMBER, &head);
    file_free_page(root_number);
    return new_root;
}


/* Coalesces a node that has become
 * too small after deletion
 * with a neighboring node that
 * can accept the additional entries
 * without exceeding the maximum.
 */
pagenum_t coalesce_nodes(pagenum_t n, pagenum_t neighbor, int neighbor_index, int k_prime) {



    int i, j, neighbor_insertion_index, n_end;
    pagenum_t tmp;
    page_t n_page, neighbor_page, tmp_page;
    file_read_page(n, &n_page);
    file_read_page(neighbor, &neighbor_page);
    /* Swap neighbor with node if node is on the
     * extreme left and neighbor is to its right.
     */

    if (neighbor_index == -1) {
        tmp = n;
        n = neighbor;
        neighbor = tmp;
    }
    
    file_read_page(n, &n_page);
    file_read_page(neighbor, &neighbor_page);
    /* Starting point in the neighbor for copying
     * keys and pointers from n.
     * Recall that n and neighbor have swapped places
     * in the special case of n being a leftmost child.
     */

    neighbor_insertion_index = neighbor_page.number_of_keys;

    /* Case:  nonleaf node.
     * Append k_prime and the following pointer.
     * Append all pointers and keys from the neighbor.
     */

    if (!n_page.is_leaf) {
        neighbor_page.more_page_number = n_page.more_page_number;


        n_end = n_page.number_of_keys;

        for (i = neighbor_insertion_index, j = 0; j < n_end; i++, j++) {
            neighbor_page.keyPage[i] = n_page.keyPage[j];
            neighbor_page.number_of_keys++;
            n_page.number_of_keys--;
        }

        /* The number of pointers is always
         * one more than the number of keys.
         */


        /* All children must now point up to the same parent.
         */

        for (i = 0; i <= neighbor_page.number_of_keys; i++) {
            if(i == 0) {
                tmp = neighbor_page.more_page_number;
            } else {
                tmp = neighbor_page.keyPage[i - 1].pageNum;
            }
            file_read_page(tmp, &tmp_page);
            tmp_page.parent_page_number = neighbor;
            file_write_page(tmp, &tmp_page);
        } 
    }

    /* In a leaf, append the keys and pointers of
     * n to the neighbor.
     * Set the neighbor's last pointer to point to
     * what had been n's right neighbor.
     */

    else {
        n_end = n_page.number_of_keys;
        for (i = neighbor_insertion_index, j = 0; j < n_end; i++, j++) {
            neighbor_page.record[i] = n_page.record[j];
            neighbor_page.number_of_keys++;
            n_page.number_of_keys--;
        }
        
        tmp = neighbor_page.parent_page_number;
        file_read_page(tmp, &tmp_page);
        if(get_neighbor_index(neighbor) == -1) {
            tmp_page.more_page_number = neighbor;
        }
        neighbor_page.more_page_number = n_page.more_page_number;
        file_write_page(tmp, &tmp_page);
    }
    file_write_page(neighbor, &neighbor_page);
    pagenum_t ret = delete_entry(n_page.parent_page_number, k_prime, n);




    if(ret == neighbor) {
        file_read_page(neighbor, &neighbor_page);
    }
    file_write_page(neighbor, &neighbor_page);
    file_free_page(n);
    return ret;
}
  
/* Deletes an entry from the B+ tree.
 * Removes the record and its key and pointer
 * from the leaf, and then makes all appropriate
 * changes to preserve the B+ tree properties.
 */

pagenum_t delete_entry(pagenum_t n, int key, pagenum_t remove) {
    // n = key_leaf key = key remove가 0이면 레코드, 0보다 크면 페이지
    int min_keys;
    page_t n_page, neighbor_page, parent_page;
    pagenum_t neighbor, parent;
    int neighbor_index;
    int k_prime_index, k_prime;
    int capacity;

    file_read_page(HEADER_PAGE_NUMBER, &head);
    file_read_page(n, &n_page);
    // Remove key and pointer from node.
    n = remove_entry_from_node(n, key, remove);
    /* Case:  deletion from the root. 
     */

    if (n == head.root_page_number) {
        return adjust_root();
    }

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
    
    file_read_page(n, &n_page);
    if (n_page.number_of_keys >= min_keys) {
        file_write_page(n, &n_page);
        return n;
    }
    /* Case:  node falls below minimum. == 0
     */

    /* Find the appropriate neighbor node with which
     * to coalesce.
     * Also find the key (k_prime) in the parent
     * between the pointer to node n and the pointer
     * to the neighbor.
     */
    
    neighbor_index = get_neighbor_index(n);
    parent = n_page.parent_page_number;
    file_read_page(parent, &parent_page);
    k_prime_index = neighbor_index == -1 ? 0 : neighbor_index;
    k_prime = parent_page.keyPage[k_prime_index].key;
    //k_prime = key;

    if(neighbor_index == -1) {
        neighbor = parent_page.keyPage[0].pageNum;
    } else if(neighbor_index == 0) {
        neighbor = parent_page.more_page_number;
    } else {
        neighbor = parent_page.keyPage[neighbor_index - 1].pageNum;
    }

    /* Coalescence. */
    return coalesce_nodes(n, neighbor, neighbor_index, k_prime);
}



/* Master deletion function.
 */


pagenum_t delete_master(int key) {
    pagenum_t key_leaf;
    record* key_record;
    key_record = find(key);
    key_leaf = find_leaf(key);
    if (key_record != NULL && key_leaf != 0) {
        delete_entry(key_leaf, key, 0);
    } else {
        return 0;
    }
    return 1;
}
