#include "bpt.cpp"

int main( int argc, char ** argv ) {

    char instruction[100];
    char pathname[100];

    int64_t key;
    int tableId = 0;
    char* value = (char*)malloc(sizeof(char) * 120);


    int buffer_size = 0;
    printf("SET BUFFER POOL SIZE \n");
    scanf("%d", &buffer_size);
    printf("BUFFER SIZE : %d\n", buffer_size);
    init_db(buffer_size);
    usage_2();

    extern buffer_control_block** controller;
    extern page_t* header_ptr;
    extern page_t* free_ptr;
    extern record_t* rec;

    printf("> ");

// thread
    
    while (scanf("%s", instruction) != EOF) {
        if(strcmp(instruction,"open") == 0) {
            scanf("%s", pathname);
            tableId = open_table(pathname);
        } else if(strcmp(instruction,"delete") == 0) {
            scanf("%ld", &key);
            if(db_delete(tableId, key) == 0) {
                printf("Delete success\n");
            } else {
                printf("Delete not found\n");
            }
        } else if(strcmp(instruction,"insert") == 0) {
            scanf("%ld %s", &key, value);
            if(db_insert(tableId, key, value) == 0) {
                printf("key insert completed\n");
            }  else {
                printf("duplitcate key\n");
            }
        }/* else if(strcmp(instruction,"find") == 0) {
            scanf("%ld", &key);
            char* ret_val = (char*)malloc(sizeof(char) * 120);
            int res = db_find(tableId, key, ret_val);
            if (res == 0) {
                printf("%s\n", ret_val);
            } else {
                printf("Cannot find data.\n");
            }
            free(ret_val);
        }*/else if(strcmp(instruction, "print") == 0) {
            printf("print leaf keys\n");
            print_leaves(tableId);
            printf("\n");
        }
        else if(strcmp(instruction, "close") == 0) {
            close_table(tableId);
        } else if(strcmp(instruction, "join") == 0) {
            int table_1 = 0;
            int table_2 = 0;
            char* p_name = (char*)malloc(sizeof(char) * 120);
            printf("input two open table number and pathname\n");
            scanf("%d %d %s", &table_1, &table_2, p_name);
            int res = join_table(table_1, table_2, p_name);
            if(res == -1) {
                printf("open two table first\n");
            }
            free(p_name);
        } 
        while (getchar() != (int)'\n');
        usage_2();
        printf("> ");
    }
    
    printf("\n");

    free(value);
    free(rec);
    return EXIT_SUCCESS;
}
