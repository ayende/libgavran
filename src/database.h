#pragma once

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>


#define PAGE_SIZE 8192


#define TX_READ_ONLY    1
#define TX_READ_WRITE   2

typedef struct database_handle database_handle_t;

typedef struct database_transction txn_t;

typedef struct {
    const char* path;
    const char* name;
    uint64_t minimum_size;
} database_options_t;

bool create_database(database_options_t* options, database_handle_t** db, void* mem);

bool close_database(database_handle_t* db);

size_t get_txn_t_size(void);

size_t get_database_handle_size(database_options_t* options);

bool create_transaction(database_handle_t* database, uint32_t flags, txn_t** tx);

void hi(void);

