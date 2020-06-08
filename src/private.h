#pragma once

#include "database.h"
#include "pal.h"

#define MINIMUM_DATABASE_SIZE 1024*128
#define FREE_SPACE_PAGES_BLOCK 4
#define PAGE_ALIGNED __attribute__((aligned(PAGE_SIZE)))

#define STRONG_HASH_SIZE 32

#define PAGE_FLAGS_FREE     0
#define PAGE_FLAGS_SINGLE   1
#define PAGE_FLAGS_OVERFLOW 2

#define FILE_HEADER_MAGIC_CONSTANT 0x73656e6176726167

typedef struct page_map page_map_t;

struct database_transction{
    database_handle_t* db;
    page_map_t* page_map;
    uint64_t txid;
    uint32_t flags;
    char _padding[4];
};


typedef struct {
    union {
        struct{
            // uint64 _page_number; - not required, implicit from the position in the headers array
            uint32_t overflow_size;
            char flags; 
            char _reserved[11];
        };
        char _padding[16];
    };
} page_header_t;

/*
    Structure of a garvan file is as follows:
    * The file is divided into pages (PAGE_SIZE)
    * The first two pages are header pages, and contains file_header_ptr with details
      about the file.
    * then you have the raw file data
    * then you have the free space bitmap:
        * each bit in the buffer points to 4 pages 
        * if *any* of those pages is free, the bit it set to 1
        * if *all* of them are busy, it is set to 0
        * actual page availability is checked using the page headers
        * a single 8kb page can store free bits information for 2GB 
    * then you have the page headers array, which contains the page headers
*/
typedef struct  {

    // indicate that this is a real garvan file, should equal to FILE_HEADER_MAGIC_CONSTANT
    uint64_t magic;     
    uint64_t last_allocated_page;
    uint64_t size_in_pages;
    uint64_t last_txid;
    uint32_t version;   // file version of this file
    uint32_t _padding;
} file_header_t;


// typedef struct  {
//     union{
//         struct{
//             // indicate that this is a real garvan file, should equal to FILE_HEADER_MAGIC_CONSTANT
//             uint64_t magic;     
//             uint64_t last_allocated_page;
//             uint32_t version;   // file version of this file
           
//             // incremented on each header modification
//             uint64_t last_txid; 
//             uint64_t size_in_pages;
//             uint64_t free_space_first_page;
//             uint64_t page_headers_first_page;
//             uint32_t _reserved;
//             char header_hash[STRONG_HASH_SIZE];
//         };
//         char _padding[96];
//     };
// } file_header_t;

struct database_handle {
   file_header_t current_file_header;
   file_handle_t* file_handle;
   void* mapped_memory;
   uint64_t database_size;
}; 

uint64_t _size_of_free_space_bitmap_in_pages(uint64_t number_of_pages);
uint64_t _size_of_page_header_buffer_in_pages(uint64_t number_of_pages);

void _set_free_space_bitmap(char* base PAGE_ALIGNED, file_header_t* header, uint64_t page);

void _clear_free_space_bitmap(char* base PAGE_ALIGNED, file_header_t* header, uint64_t page);

page_header_t* _get_page_header(char* base PAGE_ALIGNED, file_header_t* header, uint64_t page);

void* get_page_pointer(char* base PAGE_ALIGNED, uint64_t page);

bool _create_new_database(database_options_t* options, database_handle_t* database);

page_map_t* create_page_map(txn_t* tx,uint32_t initial_capacity);

bool set_page_map(page_map_t* map, uint64_t page, page_header_t* header, void* val, page_header_t**old_header, void** old_value);

bool get_page_map(page_map_t* map, uint64_t page, page_header_t** header, void** value);

bool del_page_map(page_map_t* map, uint64_t page, page_header_t** old_header, void** old_value);

void destroy_page_map(page_map_t** map, void (*destroyer)(uint64_t page, page_header_t* header, void* value, void* ctx), void* context);

void* allocate_tx_mem(txn_t* tx, uint64_t size);
void  release_tx_mem(txn_t* tx, void* address);
void* allocate_tx_page(txn_t* tx, uint64_t number_of_pages);
void  release_tx_page(txn_t* tx, void* address, uint64_t number_of_pages);


file_header_t* get_file_header(txn_t* tx);
