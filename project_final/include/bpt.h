#ifndef __BPT_H__
#define __BPT_H__


#include <list>
#include <set>
#include <map>
using namespace std;

#define FALSE 0
#define TRUE 1
#define SUCCESS 1
#define HEADER_PAGE_NUMBER 0
#define HEADER_PAGE_OFFSET 0
#define PAGE_SIZE 1000
#define MAX_THREAD_NUM 20
#define HASH_SIZE 10000
#define MAX_TXN_COUNT 20
typedef uint64_t pagenum_t;
typedef uint64_t offset_t;
typedef offset_t LSN;
typedef pagenum_t PageId;
typedef int TransactionId;
typedef int TableId;
typedef int64_t Key;
typedef char Value[120];


typedef struct My_File {
    int fd;
    char path[30];
    bool is_open;
} MyFile_t;
typedef struct record_t
{
    int64_t key;
    char value[120];
} record_t;

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

typedef struct
{
    union {
        // internal or leaf page
        struct
        {
            pagenum_t parent_page_number;
            PageType is_leaf;
            int number_of_keys;
            char reserve1[8];
            LSN pageLSN;
            char reserve2[128 - 32];
            pagenum_t more_page_number;
            union {
                record_t record[31];
                key_pageNum keyPage[248];
            };
        };
        // header page
        struct
        {
            pagenum_t free_page_number;
            pagenum_t root_page_number;
            pagenum_t number_of_pages;
            LSN pageLSN2;
            char reserve3[4096 - 32];
        };
        // free page
        struct
        {
            pagenum_t next;
            char reserve4[4096 - 8];
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
    struct buffer_control_block* next;
    struct buffer_control_block* prev;
    pthread_mutex_t buffer_page_latch;
}buffer_control_block;

typedef struct {
    TableId table_id;
    Key key;
    Value old_value;
} UndoLog;

typedef list<UndoLog> UndoLogList;

typedef enum lock_mode {
    NOLOCK= 0,
    SHARED = 1,
    EXCLUSIVE = 2
} LockMode;

typedef struct lock_t {
    TableId table_id;
    struct trx_t* trx;
    PageId page_id;
    Key key;
    bool acquired;
    LockMode lock_mode;
    struct lock_t* prev;
    struct lock_t* next;
} Lock, *LockPtr;

typedef list<Lock*> LockPtrList;
typedef Lock** LockTable;

typedef enum trx_state{
    IDLE = 0, 
    RUNNING = 1, 
    WAITING = 2,
    END = 3
} TransactionState;

typedef struct trx_t {
    TransactionId trx_id;
    TransactionState state; // IDLE, RUNNING, WAITING, END
    LockPtrList acquired_locks; // list of holding locks 
    LockPtr wait_lock; // lock object that trx is waiting
    UndoLogList undo_log_list;
    pthread_mutex_t trx_mutex;
    pthread_cond_t trx_cond;
    bool is_abort;
} Transaction;

typedef Transaction* TransactionPtr;
typedef Transaction** TransactionTable;

typedef struct {
    Transaction** trx_table;
    TransactionId next_trx_id;
    pthread_mutex_t trx_sys_mutex;
} TransactionManager;


typedef struct {
    Lock** lock_table;
    pthread_mutex_t lock_sys_mutex;
} LockManager;


typedef enum trxType{
    BEGIN = 0,
    UPDATE = 1,
    COMMIT = 2,
    ABORT = 3
} TrxType;

typedef struct {
    LSN presentLSN;
    LSN prevLSN;
    int transactionID;
    TrxType type;
    TableId tableId;
    int pageNumber;
    int offset;
    int dataLength;
    char oldImage[120];
    char newImage[120];
} Log;

typedef list<Log> logBufferlist;

typedef struct {
    logBufferlist logPool;
    pthread_mutex_t log_mutex;
} LogManager;


pthread_t thread[MAX_THREAD_NUM];
page_t head;
page_t free_page;
buffer_control_block** controller;
freeNumber* freeNumberNode;
MyFile_t myFile[30];
record_t* rec;
TransactionManager txnMgr;
LockManager lockMgr;
LogManager logMgr;
int gNoTrx = 0;
int trx_count = 0;
int currentLSN = 0;
int log_fd = 0;
pthread_mutex_t buffer_pool_latch;
set<int> allTableId;
multimap<int, Log> loserLog;


void usage_2(void);
void usage_3(void);
/*
int height(node *root);
int path_to_root(node *root, node *child);
*/
int binary_search(record_t*arr, int size, int target);
void print_leaves();
void print_tree();
/*
void find_and_print(node *root, int key, bool verbose);
void find_and_print_range(node *root, int range1, int range2, bool verbose);
int find_range(node *root, int key_start, int key_end, bool verbose,
               int returned_keys[], void *returned_pointers[]);
               */
buffer_control_block* find_leaf(int table_id, int key);
record_t* find(int table_id, int key);
int cut(int length);

// Insertion.

record_t* make_record(int key, const char *value);
pagenum_t make_node(int table_id);
pagenum_t make_leaf(int table_id);
int get_left_index(int table_id, pagenum_t parent, pagenum_t left);
buffer_control_block* insert_into_leaf(int table_id, pagenum_t leaf, int key, record_t* pointer);
buffer_control_block* insert_into_leaf_after_splitting(int table_id, pagenum_t leaf, int key, record_t* pointer);
buffer_control_block*insert_into_node(int table_id, pagenum_t parent,
                         int left_index, int key, pagenum_t right);
buffer_control_block* insert_into_node_after_splitting(int table_id, pagenum_t old_node, int left_index,
                                         int key, pagenum_t right);
buffer_control_block* insert_into_parent(int table_id, pagenum_t left, int key, pagenum_t right);
buffer_control_block* insert_into_new_root(int table_id, pagenum_t left, int key, pagenum_t right);
buffer_control_block* start_new_tree(int table_id, int key, record_t* pointer);
buffer_control_block* insert_master(int table_id, int key, const char *value);

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
int open_table(const char *pathname);
int db_insert(int table_id, int64_t key, const char *value);
int db_find(int table_id, int64_t key, char *ret_val, int trx_id);
int db_update(int table_id, int64_t key, char* values, int trx_id);
int db_delete(int table_id, int64_t key);
int join_table(int table_id_1, int table_id_2, char * pathname);
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
pagenum_t buffer_alloc_page(int table_id);

//txn part
int begin_trx();
int end_trx(int tid);
int abort_trx(int table_id, Key key, int tid);
void init_TxnMgr();
void init_lockMgr();
int lock_hash(int table_id, pagenum_t pageNo);
LockPtr initLockObject(int table_id, TransactionPtr transaction, PageId pageId, Key key, bool acquired, LockMode mode, LockPtr prev, LockPtr next);
Lock* find_matched_last_lock(Lock* head, Lock* target, int table_id, int64_t key);

int find_Id(const char* pathname);
void init_logMgr();
void initLogPtr(Log* retLog, int trx_id, TrxType trx_type, TableId table_id, int pageNumber, int offset, int dataLength, char* old_image, char* new_image);
void flushLog();
void analyzeLog();
void redoLog();
void undoLog();
#endif /* __BPT_H__*/
