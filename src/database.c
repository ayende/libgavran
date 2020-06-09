#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "database.h"
#include "private.h"
#include "errors.h"
#include "pal.h"

/*

uint64_t _size_of_free_space_bitmap_in_pages(uint64_t number_of_pages){
    uint64_t number_of_bits_required = (number_of_pages / FREE_SPACE_PAGES_BLOCK);
    uint64_t number_of_bytes_required = number_of_bits_required / 8;
    if(number_of_bits_required % 8)
        number_of_bytes_required++;
    uint64_t number_of_pages_required = number_of_bytes_required / PAGE_SIZE;
    if(number_of_bytes_required % PAGE_SIZE)
        number_of_pages_required++;
    return number_of_pages_required;
}
 
uint64_t _size_of_page_header_buffer_in_pages(uint64_t number_of_pages) {
    uint64_t size_of_page_headers_buffer = number_of_pages * sizeof(page_header_t);
    uint64_t number_of_pages_for_page_headers_buffer = size_of_page_headers_buffer / PAGE_SIZE;
    if(size_of_page_headers_buffer % PAGE_SIZE)
        number_of_pages_for_page_headers_buffer++;
    return number_of_pages_for_page_headers_buffer;
}

void _set_free_space_bitmap( char* base  PAGE_ALIGNED , file_header_t* header, uint64_t page){
     uint64_t* free_space_bitmap = (void*)(base + header->free_space_first_page * PAGE_SIZE);
     free_space_bitmap[ page / 64 ] |= 1ul << ( page % 64 );
} 

void _clear_free_space_bitmap( char* base PAGE_ALIGNED, file_header_t* header, uint64_t page){
     uint64_t* free_space_bitmap = (void*)(base + header->free_space_first_page * PAGE_SIZE);
     free_space_bitmap[ page / 64 ] &= ~(1ul << ( page % 64 ));
}

page_header_t* _get_page_header(char* base PAGE_ALIGNED, file_header_t* header, uint64_t page){
     page_header_t* buffer = (void*)(base + header->page_headers_first_page * PAGE_SIZE);
     return &buffer[page];
}
*/



bool _create_new_database(database_options_t* options, database_handle_t* database){
    file_handle_t* handle = database->file_handle;
    assert(options->minimum_size % PAGE_SIZE == 0);
    if(!ensure_file_minimum_size(handle, options->minimum_size))
        return false;

    void* address;
    if(!map_file(handle, options->minimum_size, &address)){
        push_error(ENODATA, "Unable to map file when creating new database %s", options->name);
        return false;    
    }
    database->mapped_memory = address;
    database->database_size = options->minimum_size;

    // here we can assume that the entire range is zero initialized
    uint64_t number_of_pages = options->minimum_size / PAGE_SIZE;

    file_header_t header = {
        .magic = FILE_HEADER_MAGIC_CONSTANT,
//        .last_txid = 0,
        .size_in_pages = number_of_pages,
        .last_allocated_page = 1, // exluding the header page
        .version = 1,        
    };
    /*
    header.page_headers_first_page = number_of_pages - 
                _size_of_page_header_buffer_in_pages(number_of_pages); 

    header.free_space_first_page = header.page_headers_first_page - 
                _size_of_free_space_bitmap_in_pages(number_of_pages);
    */

    char* base = address;
    // we copy the header twice on the same page, but on different sectors
    _Static_assert(PAGE_SIZE/2 >= 4096,"We need to ensure that this reside on separate sectors, and on SSD, that means 4KB apart");
    memcpy(base, &header, sizeof(header));              // PAGE 0
    memcpy(base + PAGE_SIZE/2, &header, sizeof(header));// PAGE 0.5
    memcpy(&database->current_file_header, &header, sizeof(header));

    // _set_free_space_bitmap(base, &header, 0); 
    // _get_page_header(base, &header, 0)->flags = PAGE_FLAGS_SINGLE;
    
    // _set_free_space_bitmap(base, &header, 1);
    // _get_page_header(base, &header, 1)->flags = PAGE_FLAGS_SINGLE;

    // for(uint64_t i = header.free_space_first_page; i < number_of_pages; i++){
    //     _set_free_space_bitmap(base, &header, i);
    //      _get_page_header(base, &header, i)->flags = PAGE_FLAGS_SINGLE;
    // }

    return true;
}

static bool validate_options(database_options_t* options) {
    if(options->minimum_size < MINIMUM_DATABASE_SIZE)
        options->minimum_size = MINIMUM_DATABASE_SIZE;

    if(options->minimum_size % PAGE_SIZE)
        options->minimum_size += PAGE_SIZE - (options->minimum_size % PAGE_SIZE);

    if(!options->path || !strlen(options->path)){
        push_error(EINVAL, "The database path was not specified or empty, but is required");
        return false; 
    }
    if(!options->name || !strlen(options->name)){
        push_error(EINVAL, "The database name was not specified or empty, but is required");
        return false;
    } 
    return true;
}

size_t get_database_handle_size(database_options_t* options) {
    size_t len = get_file_handle_size(options->path, options->name);
    return len + sizeof(database_handle_t);
}

bool create_database(database_options_t* options, database_handle_t* database){

    if(!validate_options(options))
        return false;

    size_t len = get_file_handle_size(options->path, options->name);
    assert(len); // shouldn't happen, already checked in validate_options

    memset(database, 0, sizeof(database) + len);

    if(!create_file(options->path, options->name, database->file_handle)){
        push_error(EIO, "Could not create database %s", options->name);
        goto exit_error;
    }
 
    if(!get_file_size(database->file_handle, &database->database_size)){
        push_error(errno, "Unable to create database %s", options->name);
        goto exit_error;
    }

    if(database->database_size == 0){
        if(!_create_new_database(options, database))
            goto exit_error;
    }
    else {
        // recover existing db
    }
    
    return true;
exit_error:
    // these can fail, but in this case, we don't care, the error is reported anyway via push_error
    // and there isn't much we can do further to handle this
    close_database(database);
    return false;
}

bool close_database(database_handle_t* database) {
    if(!database)
        return true;
    
    bool success = true;
    if(database->mapped_memory)  
        success &= unmap_file(database->mapped_memory, database->database_size);
    success &= close_file(database->file_handle);
    if(!success)
        push_error(EIO, "Failed to close database %s", get_file_name(database->file_handle));

    return success;
}

bool create_transaction(database_handle_t* database, uint32_t flags, txn_t* txn) {
    memset(txn, 0, sizeof(txn_t));

    // txn->txid = database->current_file_header.last_txid;
    // if (flags & TX_READ_WRITE)
    //     txn->txid++;

    txn->db = database;
    txn->flags = flags;
    txn->page_map = create_page_map(txn, 32);
    if (!txn->page_map) {
        push_error(ENOMEM, "Unable to allocate a page map for transaction");
        return false;
    }
    return true;
}

static void _page_map_value_destroyer(uint64_t page, page_header_t* header, void* value, void* ctx){
    (void)page;
    (void)header;
    (void)value;
    (void)ctx;
}

bool close_transaction(txn_t* tx){
    destroy_page_map(&tx->page_map, _page_map_value_destroyer, 0);
    return true;
}
