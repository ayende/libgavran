#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "database.h"
#include "private.h"
#include "errors.h"
#include "pal.h"

/*
bool modify_page(txn_t* tx, uint64_t page, page_header_t** header, void** page_data) {
    if(get_page_map(tx->page_map, page, header, page_data))
        return true;

    if(page > tx->db->current_file_header.size_in_pages){
        push_error(EINVAL, "Attempted to read page %lu but the max page is %lu", page, tx->db->current_file_header.size_in_pages);
        return false;
    }

    page_header_t* read_header = _get_page_header(tx->db->mapped_memory, &(tx->db->current_file_header), page);
    void* read_data = _get_page_pointer(tx->db->mapped_memory, page);

    uint64_t number_of_pages = 1;

    if(read_header->flags & PAGE_FLAGS_OVERFLOW) {
        number_of_pages = read_header->overflow_size / PAGE_SIZE + (read_header->overflow_size % PAGE_SIZE ? 1 : 0);
    }

    *page_data = allocate_tx_page(tx, number_of_pages);
    if(!*page_data){
        push_error(errno, "Unable to allocate memory for modifed copy of page %lu (%lu pages)", page, number_of_pages);
        return false;
    }
    *header = allocate_tx_mem(tx, sizeof(page_header_t));
    if (!*header){
        push_error(ENOMEM, "Unable to allocate memory for modified page header %lu", page);
        release_tx_page(tx, *page_data, number_of_pages);
        *page_data = 0;
        return false;
    } 

    memcpy(*header, read_header, sizeof(page_header_t));
    memcpy(*page_data, read_data, PAGE_SIZE * number_of_pages);

    if(!set_page_map(tx->page_map, page, *header, *page_data, &read_header, &read_data)){
        push_error(ENOMEM, "Unable to allocate memory for transaction's page map");
        release_tx_page(tx, *page_data, number_of_pages);
        release_tx_mem(tx, *header);
        *header = 0;
        *page_data = 0;
        return false;
    }
    return true;
}

file_header_t* get_current_file_header(txn_t* tx){
    void* first_page_buffer = get_page_pointer(tx->db->mapped_memory, 0);
    file_header_t* fst = (file_header_t*)first_page_buffer;
    file_header_t* snd = (file_header_t*)((char*)first_page_buffer + PAGE_SIZE/2);
    if (fst->last_txid > snd->last_txid)
        return fst;
    return snd;
}

*/


void* get_page_pointer(char* base PAGE_ALIGNED, uint64_t page) {
    return (void*)(base + page * PAGE_SIZE);
}


size_t get_txn_size(void) { 
    return sizeof(txn_t);
}

uint64_t get_txn_id(txn_t* tx) {
    return tx->txid;
}

bool modify_page(txn_t* tx, uint64_t page_number, void**page_buffer, uint32_t* number_of_pages_allocated){
    file_header_t* header = &tx->db->current_file_header;
    if(page_number >= header->last_allocated_page){
        push_error(EINVAL, "Page %lu was not allocated in %s", page_number, get_file_name(tx->db->file_handle));
        return false;
    }
    *page_buffer = get_page_pointer(tx->db->mapped_memory, page_number);
    *number_of_pages_allocated = 1;
    return true;
}

bool allocate_page(txn_t* tx, uint32_t number_of_pages, uint32_t flags, uint64_t* page_number){
    (void)flags; // currently unused
    assert(number_of_pages == 1);
    file_header_t* header = &tx->db->current_file_header;
    if(header->last_allocated_page + number_of_pages >= header->size_in_pages){
        push_error(ENOSPC, "Unable to allocate %u page(s) for %s because %lu pages are allocated", number_of_pages,
            get_file_name(tx->db->file_handle), header->last_allocated_page);
        return false;
    }
    *page_number  = header->last_allocated_page ;
    header->last_allocated_page += number_of_pages;
    return true;
}


// TEMP impl

void* allocate_tx_mem(txn_t* tx, uint64_t size){
    (void)tx;
    return malloc(size);
}

void* allocate_tx_page(txn_t* tx, uint64_t number_of_pages){
    (void)tx;
    void*ptr; 
    int rc = posix_memalign(&ptr, PAGE_SIZE, number_of_pages * PAGE_SIZE);
    if(!rc)
        return ptr;
    errno = rc;
    return 0;
}

void release_tx_mem(txn_t* tx, void* address){
    (void)tx;
    free(address);
}

void  release_tx_page(txn_t* tx, void* address, uint64_t number_of_pages){
    (void)tx;
    (void)number_of_pages;
    free(address);
}


