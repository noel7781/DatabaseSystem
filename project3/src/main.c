#include "bpt.c"

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
    extern record* rec;
    printf("> ");
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
        } else if(strcmp(instruction,"find") == 0) {
            scanf("%ld", &key);
            char* ret_val = (char*)malloc(sizeof(char) * 120);
            int res = db_find(tableId, key, ret_val);
            if (res == 0) {
                printf("%s\n", ret_val);
            } else {
                printf("Cannot find data.\n");
            }
            free(ret_val);
        }else if(strcmp(instruction, "print") == 0) {
            printf("print leaf keys\n");
            print_leaves(tableId);
            printf("\n");
        }
        else if(strcmp(instruction, "close") == 0) {
            close_table(tableId);
        }
        /*
        case 'p':
            scanf("%d", &input);
            find_and_print(root, input, instruction == 'p');
            break;
        case 'r':
            scanf("%d %d", &input, &range2);
            if (input > range2) {
                int tmp = range2;
                range2 = input;
                input = tmp;
            }
            find_and_print_range(root, input, range2, instruction == 'p');
            break;
        case 'l':
            print_leaves(root);
            break;
        case 'q':
            while (getchar() != (int)'\n');
            return EXIT_SUCCESS;
            break;
        case 't':
            print_tree(root);
            break;
        case 'v':
            verbose_output = !verbose_output;
            break;
        case 'x':
            if (root)
                root = destroy_tree(root);
            print_tree(root);
            break;*/
        while (getchar() != (int)'\n');
        usage_2();
        printf("> ");
    }
    printf("\n");

    free(value);
    free(rec);
    return EXIT_SUCCESS;
}
