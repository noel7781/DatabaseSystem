// INSERTION ++ index manager
#include "bpt.h"

record_t* make_record(int key, const char* value) {
    record_t* new_record = (record_t*)malloc(sizeof(record_t));
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

    tmp->frame.is_leaf = PAGE_INTERNAL;
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
    tmp->frame.is_leaf = PAGE_LEAF;
    tmp->is_dirty = TRUE;
    tmp->is_pinned = FALSE;
    return leafPNum;
}

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
buffer_control_block* insert_into_leaf(int table_id, pagenum_t leaf, int key, record_t* pointer ) {
    buffer_control_block* p = buffer_write_page(table_id, leaf);
    int i, insertion_point;
    insertion_point = 0;
    while (insertion_point < p->frame.number_of_keys && p->frame.record[insertion_point].key < key)
        insertion_point++;

    for (i = p->frame.number_of_keys; i > insertion_point; i--) {
        p->frame.record[i] = p->frame.record[i - 1];
    }
    memcpy(&p->frame.record[insertion_point], pointer, sizeof(record_t));
    p->frame.number_of_keys++;
    p->is_dirty = TRUE;
    p->is_pinned = FALSE;
    return p;
}



buffer_control_block* insert_into_leaf_after_splitting(int table_id, pagenum_t leaf, int key, record_t* pointer) {
    pagenum_t new_leaf;
    int* temp_keys; 
    record_t* temp_pointers; 
    int insertion_index, split, new_key, i, j;

    buffer_control_block* p = buffer_write_page(table_id, leaf);
    new_leaf = make_leaf(table_id);
    buffer_control_block* q = buffer_write_page(table_id, new_leaf);
    temp_keys = (int*)malloc(32 * sizeof(int));
    if (temp_keys == NULL) {
        perror("Temporary keys array.");
        exit(EXIT_FAILURE);
    }

    temp_pointers = (record_t*)malloc(32 * sizeof(record_t));
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

buffer_control_block* insert_into_node(int table_id, pagenum_t parent, 
        int left_index, int key, pagenum_t right) {
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

buffer_control_block* insert_into_node_after_splitting(int table_id, pagenum_t old, int left_index, 
        int key, pagenum_t right) {
    int i, j, split, k_prime;
    pagenum_t newPageNumber;
    buffer_control_block* old_block = buffer_write_page(table_id, old);
    int * temp_keys;
    key_pageNum* temp_pointers;

    temp_pointers = (key_pageNum*)malloc((249 + 1) * sizeof(key_pageNum));
    if (temp_pointers == NULL) {
        perror("Temporary pointers array for splitting nodes.");
        exit(EXIT_FAILURE);
    }
    temp_keys = (int*)malloc(249 * sizeof(int));
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
    newPageNumber = make_page(table_id);
    buffer_control_block* new_block = buffer_write_page(table_id, newPageNumber);
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
    child_block->frame.parent_page_number = newPageNumber;
    child_block->is_pinned = FALSE;
    for (i = 1; i <= new_block->frame.number_of_keys; i++) {
        child_block = buffer_write_page(table_id, new_block->frame.keyPage[i - 1].pageNum);
        child_block->is_dirty = TRUE;
        child_block->frame.parent_page_number = newPageNumber;
        child_block->is_pinned = FALSE;
    }
    old_block->is_pinned = FALSE;
    new_block->is_pinned = FALSE;

    return insert_into_parent(table_id, old_block->page_num, k_prime, newPageNumber);
}

buffer_control_block* insert_into_parent(int table_id, pagenum_t left, int key, pagenum_t right) {
    int left_index;
    pagenum_t parent; // p - left q - right r -parent
    buffer_control_block* p = buffer_write_page(table_id, left);
    parent = p->frame.parent_page_number;

    if (parent == 0) {
        p->is_pinned = FALSE;
        return insert_into_new_root(table_id, left, key, right);
    }
    
    buffer_control_block* r = buffer_write_page(table_id, parent);
    left_index = get_left_index(table_id, parent, left);
    p->is_pinned = FALSE;

    if (r->frame.number_of_keys < 249 - 1) {
        r->is_dirty = TRUE;
        r->is_pinned = FALSE;
        return insert_into_node(table_id, parent, left_index, key, right);
    }
    r->is_dirty = TRUE;
    r->is_pinned = FALSE;
    return insert_into_node_after_splitting(table_id, parent, left_index, key, right);
}

buffer_control_block* insert_into_new_root(int table_id, pagenum_t left, int key, pagenum_t right) {
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

buffer_control_block* start_new_tree(int table_id, int key, record_t* pointer) {
    buffer_control_block* head_block = buffer_write_page(table_id, HEADER_PAGE_NUMBER);
    pagenum_t root;
    root = make_leaf(table_id);
    buffer_control_block* p = buffer_write_page(table_id, root);
    head_block->frame.root_page_number = root;
    head_block->is_dirty = TRUE;
    p->frame.parent_page_number = 0;
    p->frame.record[0] = *pointer;
    p->frame.number_of_keys = 1;
    p->frame.more_page_number = 0;
    p->is_pinned = FALSE;
    head_block->is_pinned = FALSE;
    p->is_dirty = TRUE;
    return p;
}

buffer_control_block* insert_master(int table_id, int key, const char* value) {
    record_t* record;
    buffer_control_block* leaf_block;
    page_t tmp;

    if (find(table_id, key) != NULL) {
        return 0;
    }

    record = make_record(key, value);

    buffer_control_block* p = buffer_write_page(table_id, HEADER_PAGE_NUMBER);
    if (p->frame.root_page_number == 0)  { 
        p->is_pinned = FALSE;
        return start_new_tree(table_id, key, record);
    }




    leaf_block = find_leaf(table_id, key);

    if (leaf_block->frame.number_of_keys < 31) {
        leaf_block = insert_into_leaf(table_id, leaf_block->page_num, key, record);
        leaf_block->is_dirty = TRUE;
        leaf_block->is_pinned = FALSE;
        p->is_pinned = FALSE;
        return leaf_block;
    }

    p->is_pinned = FALSE;
    leaf_block->is_pinned = FALSE;
    return insert_into_leaf_after_splitting(table_id, leaf_block->page_num, key, record);
}
