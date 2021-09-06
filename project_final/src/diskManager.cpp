//Disk Manager
#include "bpt.h"

int set_header(int table_id, page_t* header) {
    file_read_page(table_id, HEADER_PAGE_NUMBER, header);
    header->free_page_number = 1;
    header->root_page_number = 0;
    header->number_of_pages = 8000;
    file_write_page(table_id, HEADER_PAGE_NUMBER, header);
}

void file_read_page(int table_id, pagenum_t pagenum, page_t* dest) {
    int size = 0;
    if((size = pread(myFile[table_id].fd, dest, 4096, pagenum * 4096)) == - 1) {
        perror("filemanager pread");
    }
}
void file_write_page(int table_id, pagenum_t pagenum, const page_t* src) {
    int size = 0;
    if((size = pwrite(myFile[table_id].fd, src, 4096, pagenum * 4096)) == -1) {
        perror("pwrite");
    }
    fsync(myFile[table_id].fd);
}