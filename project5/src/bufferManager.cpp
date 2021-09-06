//Buffer Manager
#include "bpt.h"

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