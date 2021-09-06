// DELETION. ++ index manager
#include "bpt.h"

int get_neighbor_index(int table_id, pagenum_t n) {
    int i;
    pagenum_t parent;
    buffer_control_block* n_block = buffer_write_page(table_id, n);
    parent = n_block->frame.parent_page_number;
    n_block->is_pinned = FALSE;
    buffer_control_block* parent_block = buffer_write_page(table_id, parent);

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

buffer_control_block*remove_entry_from_node(int table_id, pagenum_t n, int key, pagenum_t remove) {
    int i, num_pointers;
    buffer_control_block* n_block = buffer_write_page(table_id, n);
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

    if (root_block->frame.number_of_keys > 0) {
        root_block->is_pinned = FALSE;
        header_block->is_pinned = FALSE;
        return root_block;
    }

    if (!root_block->frame.is_leaf) {
        new_root = root_block->frame.more_page_number;
        new_root_block = buffer_write_page(table_id, new_root);
        new_root_block->frame.parent_page_number = 0;
        new_root_block->is_dirty = TRUE;
        header_block->frame.root_page_number = new_root;
        header_block->is_dirty = TRUE;
    }

    else {
        new_root = 0;
        header_block->frame.root_page_number = 0;
        header_block->is_dirty = TRUE;
    }
    header_block->is_pinned = FALSE;
    root_block->is_pinned = FALSE;
    new_root_block = buffer_write_page(table_id, new_root);
    new_root_block->is_dirty = TRUE;
    buffer_free_page(table_id, root_block->page_num);
    new_root_block->is_pinned = FALSE;
    return new_root_block;
}

buffer_control_block* coalesce_nodes(int table_id, pagenum_t n, pagenum_t neighbor, int neighbor_index, int k_prime) {

    int i, j, neighbor_insertion_index, n_end;
    pagenum_t tmp;
    buffer_control_block* n_block = buffer_write_page(table_id, n);
    buffer_control_block* neighbor_block = buffer_write_page(table_id, neighbor);

    if (neighbor_index == -1) {
        tmp = n;
        n = neighbor;
        neighbor = tmp;
        n_block = buffer_write_page(table_id, n);
        neighbor_block = buffer_write_page(table_id, neighbor);
        n_block->is_dirty = TRUE;
        neighbor_block->is_dirty = TRUE;
    }

    neighbor_insertion_index = neighbor_block->frame.number_of_keys;


    if (!n_block->frame.is_leaf) {
        neighbor_block->is_dirty = TRUE;


        n_end = n_block->frame.number_of_keys;

        n_block->is_dirty = TRUE;
        for (i = neighbor_insertion_index, j = 0; j < n_end; i++, j++) {
            neighbor_block->frame.keyPage[i] = n_block->frame.keyPage[j];
            neighbor_block->frame.number_of_keys++;
            n_block->frame.number_of_keys--;
        }

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

    }


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




    neighbor_block->is_pinned = FALSE;
    n_block->is_pinned = FALSE;
    buffer_free_page(table_id, n_block->page_num);
    return ret;
}
  
buffer_control_block* delete_entry(int table_id, pagenum_t n, int key, pagenum_t remove) {
    int min_keys;
    pagenum_t neighbor, parent;
    int neighbor_index;
    int k_prime_index, k_prime;
    int capacity;

    buffer_control_block* header_block = buffer_write_page(table_id, HEADER_PAGE_NUMBER);
    buffer_control_block* n_block = buffer_write_page(table_id, n);

    n_block->is_dirty = TRUE;
    n_block = remove_entry_from_node(table_id, n, key, remove);
    if (n_block->page_num == header_block->frame.root_page_number) {
        header_block->is_pinned = FALSE;
        n_block->is_pinned = FALSE;
        return adjust_root(table_id);
    }
    header_block->is_pinned = FALSE;

    min_keys = 1;

    n_block = buffer_write_page(table_id, n);
    if (n_block->frame.number_of_keys >= min_keys) {
        n_block = buffer_write_page(table_id, n);
        n_block->is_dirty = TRUE;
        n_block->is_pinned = FALSE;
        return n_block;
    }
    
    neighbor_index = get_neighbor_index(table_id, n);
    parent = n_block->frame.parent_page_number;
    buffer_control_block* parent_block = buffer_write_page(table_id, parent);
    
    k_prime_index = neighbor_index == -1 ? 0 : neighbor_index;
    if(k_prime_index == 0) {
        k_prime = parent_block->frame.more_page_number;
    } else {
        k_prime = parent_block->frame.keyPage[k_prime_index - 1].key;
    }
    if(neighbor_index == -1) {
        neighbor = parent_block->frame.keyPage[0].pageNum;
    } else if(neighbor_index == 0) {
        neighbor = parent_block->frame.more_page_number;
    } else {
        neighbor = parent_block->frame.keyPage[neighbor_index - 1].pageNum;
    }
    parent_block->is_pinned = FALSE;
    n_block->is_pinned = FALSE;
    return coalesce_nodes(table_id, n, neighbor, neighbor_index, k_prime);
}


buffer_control_block* delete_master(int table_id, int key) {
    buffer_control_block* p;
    pagenum_t key_leaf;
    record_t* key_record;
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
