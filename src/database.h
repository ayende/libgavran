// #pragma once

// #include <stdint.h>
// #include <stdbool.h>
// #include <stddef.h>




// _Static_assert(PAGE_SIZE > 4096 && PAGE_SIZE % 4096 == 0, "PAGE_SIZE must be of a multiple of 4096 and > 4096");


// #define TX_READ_ONLY    1
// #define TX_READ_WRITE   2

// typedef struct database_handle database_handle_t;



// typedef struct {
//     const char* path;
//     const char* name;
//     uint64_t minimum_size;
// } database_options_t;

// bool create_database(database_options_t* options, database_handle_t* db);

// bool close_database(database_handle_t* db);

// size_t get_txn_size(void);

// uint64_t get_txn_id(txn_t* tx);

// size_t get_database_handle_size(database_options_t* options);

// bool create_transaction(database_handle_t* database, uint32_t flags, txn_t* tx);

// bool close_transaction(txn_t* tx);

// bool allocate_page(txn_t* tx, uint32_t number_of_pages, uint32_t flags, uint64_t* page_number);

// bool modify_page(txn_t* tx, uint64_t page_number, void** page_buffer, uint32_t* number_of_pages_allocated);
