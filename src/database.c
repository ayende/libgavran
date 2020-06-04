#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "database.h"
#include "private.h"
#include "errors.h"
#include "pal.h"


uint64_t _size_of_free_space_bitmap_in_pages(uint64_t number_of_pages){
    uint64_t number_of_bits_required = (number_of_pages / FREE_SPACE_PAGES_BLOCK);
    uint64_t number_of_bytes_required = number_of_bits_required / 8;
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

void* _get_page_pointer(char* base PAGE_ALIGNED, uint64_t page) {
    return (void*)(base + page * PAGE_SIZE);
}

bool _create_new_database(database_options_t* options, database_handle_t* database, file_handle_t* handle){
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
        .last_txid = 0,
        .size_in_pages = number_of_pages,
        .version = 1,        
    };
    header.page_headers_first_page = number_of_pages - 
                _size_of_page_header_buffer_in_pages(number_of_pages); 

    header.free_space_first_page = header.page_headers_first_page - 
                _size_of_free_space_bitmap_in_pages(number_of_pages);

    char* base = address;
    memcpy(base, &header, sizeof(header));             // PAGE 0
    memcpy(base + PAGE_SIZE, &header, sizeof(header)); // PAGE 1

    memcpy(&database->current_file_header, &header, sizeof(header));

    _set_free_space_bitmap(base, &header, 0); 
    _get_page_header(base, &header, 0)->flags = PAGE_FLAGS_SINGLE;
    
    _set_free_space_bitmap(base, &header, 1);
    _get_page_header(base, &header, 1)->flags = PAGE_FLAGS_SINGLE;

    for(uint64_t i = header.free_space_first_page; i < number_of_pages; i++){
        _set_free_space_bitmap(base, &header, i);
         _get_page_header(base, &header, i)->flags = PAGE_FLAGS_SINGLE;
    }

    return true;
}

bool create_database(database_options_t* options, database_handle_t** database){
    if(options->minimum_size < MINIMUM_DATABASE_SIZE)
        options->minimum_size = MINIMUM_DATABASE_SIZE;

    if(options->minimum_size % PAGE_SIZE)
        options->minimum_size += PAGE_SIZE - (options->minimum_size % PAGE_SIZE);

    if(!options->path){
        push_error(EINVAL, "The database path was not specified, but is required");
        return false; 
    }
    if(!options->name){
        push_error(EINVAL, "The database name was not specified, but is required");
        return false;
    } 

    *database = 0;
    file_handle_t* handle = 0;
    if(!create_file(options->path, &handle)){
        push_error(EIO, "Could not create database %s", options->name);
        return false;
    }
    *database = malloc(sizeof(struct database_handle));
    if(!*database){
        push_error(ENOMEM, "Unable to allocate memory for database handle for %s", options->name);
        close_file(&handle);
        return false;
    }
    
 
    if(!get_file_size(handle, &(*database)->database_size)){
        push_error(errno, "Unable to create database %s", options->name);
        goto exit_error;
    }

    if((*database)->database_size == 0){
        if(!_create_new_database(options, *database, handle))
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

bool close_database(database_handle_t** database) {
    if(!database || !*database)
        return true;

    bool success = true;
    if((*database)->mapped_memory)  
        success &= unmap_file((*database)->mapped_memory, (*database)->database_size);
    success &= close_file(&(*database)->file_handle);
    free(*database);
    *database = 0;

    return success;
}

txn_t* create_transaction(database_handle_t* database, uint32_t flags){
    txn_t* txn = malloc(sizeof(txn_t));
    if(!txn){
        push_error(ENOMEM, "Unable to allocate a new transaction");
        return 0;
    }

    txn->txid = database->current_file_header.last_txid;
    if (flags & TX_READ_WRITE)
        txn->txid++;

    txn->db = database;
    txn->flags = flags;
    txn->page_map = create_page_map(32);
    if (!txn->page_map) {
        push_error(ENOMEM, "Unable to allocate a page map for transaction");
        free(txn);
        return false;
    }
    return txn;
}

bool modify_page(txn_t* tx, uint64_t page, page_header_t** header, void** page_data) {
    if(get_page_map(tx->page_map, page, header, page_data))
        return true;

    if(page > tx->db->current_file_header.size_in_pages){
        push_error(EINVAL, "Attempted to read page %lu but the max page is %ul", page, tx->db->current_file_header.size_in_pages);
        return false;
    }

    page_header_t* read_header = _get_page_header(tx->db->mapped_memory, &(tx->db->current_file_header), page);
    void* read_data = _get_page_pointer(tx->db->mapped_memory, page);

    int number_of_pages = 1;

    if(read_header->flags & PAGE_FLAGS_OVERFLOW) {
        number_of_pages = read_header->overflow_size / PAGE_SIZE + (read_header->overflow_size % PAGE_SIZE ? 1 : 0);
    }

    int rc = posix_memalign(page_data, PAGE_SIZE, PAGE_SIZE * number_of_pages);
    if(!rc){
        push_error(rc, "Unable to allocate memory for modifed copy of page %lu (%ul pages)", page, number_of_pages);
        return false;
    }
    *header = malloc(sizeof(page_header_t));
    if (!*header){
        push_error(ENOMEM, "Unable to allocate memory for modified page header %lu", page);
        free(*page_data);
        *page_data = 0;
        return false;
    } 

    memcpy(*header, read_header, sizeof(page_header_t));
    memcpy(*page_data, read_data, PAGE_SIZE * number_of_pages);

    if(!set_page_map(tx->page_map, page, *header, *page_data, &read_header, &read_data)){
        push_error(ENOMEM, "Unable to allocate memory for transaction's page map");
        free(*page_data);
        free(*header);
        *header = 0;
        *page_data = 0;
        return false;
    }
    return true;
}

