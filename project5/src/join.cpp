//join
#include "bpt.h"

int join_table(int table_id_1, int table_id_2, char * pathname) {
    if(myFile[table_id_1].is_open == FALSE || myFile[table_id_2].is_open == FALSE) {
        return -1;
    } else {
        int new_fd = open(pathname, O_RDWR | O_CREAT | O_APPEND, 0600);
        buffer_control_block* cur = buffer_write_page(table_id_1, HEADER_PAGE_NUMBER);
        buffer_control_block* cur_2;
        buffer_control_block* right;
        char buf[260];
        if (cur->frame.root_page_number == 0) {
            return -1;
        }
        cur_2 = buffer_write_page(table_id_1, cur->frame.root_page_number);
        cur->is_pinned = FALSE;
        cur = cur_2;
        while (!cur->frame.is_leaf) {
            cur_2 = buffer_write_page(table_id_1, cur->frame.more_page_number);
            cur->is_pinned = FALSE;
            cur = cur_2;
        }
        while(1) {
            for(int i = 0; i < cur->frame.number_of_keys; i++) {
                right = find_leaf(table_id_2, cur->frame.record[i].key);
                if(right == NULL) continue;
                else {
                    int j = 0;
                    j = binary_search(right->frame.record, right->frame.number_of_keys, cur->frame.record[i].key);
                    if(j != -1) {
                        sprintf(buf, "%ld,%s,%ld,%s\n", cur->frame.record[i].key, cur->frame.record[i].value, right->frame.record[j].key, right->frame.record[j].value);
                        pwrite(new_fd, buf, strlen(buf), 0);
                        fsync(new_fd);
                    }
                }
            }
            if(cur->frame.more_page_number == 0) break;
            cur_2 = buffer_write_page(table_id_1, cur->frame.more_page_number);
            cur->is_pinned = FALSE;
            cur = cur_2;
        }
        cur->is_pinned = FALSE;
        close(new_fd);
        return 0;
    }
}