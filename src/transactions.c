#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "transactions.h"
#include "errors.h"
#include "pal.h"


typedef struct page_hash_entry {
    uint64_t page_num;
    void* address;
} page_hash_entry_t;

struct transaction_state {
    file_handle_t* handle;
    void* address;
    size_t allocated_size;
    uint64_t file_size;
    uint32_t flags;
    uint32_t modified_pages;
    page_hash_entry_t entries[];
};

#define get_number_of_buckets(state) ((state->allocated_size - sizeof(txn_state_t)) / sizeof(page_hash_entry_t))

bool commit_transaction(txn_t* tx){
    txn_state_t* state = tx->state;
    size_t number_of_buckets = get_number_of_buckets(state);
    
    for(size_t i = 0; i< number_of_buckets; i++){
        if(!state->entries[i].address)
            continue;

        if(!write_file(state->handle, 
            state->entries[i].page_num * PAGE_SIZE, 
            state->entries[i].address, PAGE_SIZE)) {
                push_error(EIO, "Unable to write page %lu", state->entries[i].page_num);
            return false;
        }
        free(state->entries[i].address);
        state->entries[i].address = 0;
    }
    return true;
}

 bool close_transaction(txn_t* tx) {
     if(!tx->state)
        return true; // probably double close?
    txn_state_t* state = tx->state;
    size_t number_of_buckets = get_number_of_buckets(state);
    bool result = true;
    for(size_t i = 0; i< number_of_buckets; i++){
        if(!state->entries[i].address)
            continue;

        free(state->entries[i].address);
        state->entries[i].address = 0;
    }

   if(!unmap_file(state->address, state->file_size)){
        mark_error();
        result = false;
    }

    free(tx->state);

    tx->state = 0;
    return result;
 }

bool create_transaction(file_handle_t* handle, uint32_t flags, txn_t* tx){
    
    assert_no_existing_errors();
    
    uint64_t size;
    if(!get_file_size(handle, &size)){
        mark_error();
        return false;
    }

    void* addr;
    if(!map_file(handle, 0, size, &addr)){
        mark_error();
        return false;
    }

    size_t initial_size = sizeof(txn_state_t) + sizeof(page_hash_entry_t) * 8;
    txn_state_t* state = calloc(1, initial_size);
    if (!state){
       if(!unmap_file(addr, size)){
           mark_error();
       }
       push_error(ENOMEM, "Unable to allocate memory for transaction state");
       return false;
    }
    memset(state, 0, initial_size);
    state->allocated_size = initial_size;
    state->handle = handle;
    state->flags = flags;
    state->file_size = size;
    state->address = addr;

    tx->state = state;
    return true;
}


static bool lookup_entry_in_tx(txn_state_t* state, uint64_t page_num, page_hash_entry_t** entry){
    size_t number_of_buckets = get_number_of_buckets(state);
    size_t starting_pos = (size_t)(page_num % number_of_buckets);
    // we use linear probing to find a value in case of collisions
    for(size_t i = 0; i < number_of_buckets; i++){
        size_t index = (i + starting_pos) % number_of_buckets;
        if(!state->entries[index].address){
            // empty value, so there is no match
            return false;
        }
        if(state->entries[index].page_num == page_num){
            *entry = &state->entries[index];
            return true;
        }
    }
    return false;
}

enum hash_resize_status {
    hash_resize_success,
    hash_resize_err_no_mem,
    hash_resize_err_failure,
};

static enum hash_resize_status expand_hash_table(txn_state_t** state_ptr, size_t number_of_buckets){
    size_t new_number_of_buckets = number_of_buckets*2;
    size_t new_size = sizeof(txn_state_t) + (new_number_of_buckets*sizeof(page_hash_entry_t));
    txn_state_t* state  = *state_ptr;
    txn_state_t* new_state = calloc(1, new_size);
    if (!new_state){
        // we are OOM, but we'll accept that and let the hash
        // table fill to higher capacity, caller may decide to 
        // error
        return hash_resize_err_no_mem;
    }
    memcpy(new_state, state, sizeof(txn_state_t));
    new_state->allocated_size = new_size;

    for(size_t i = 0; i < number_of_buckets; i++){
        if(state->entries[i].address){
            size_t starting_pos = state->entries[i].page_num % new_number_of_buckets;
            bool located = false;
            for(size_t j = 0; j < new_number_of_buckets; j++){
                size_t index = (j + starting_pos) % new_number_of_buckets;
                if(!new_state->entries[index].address){ // empty
                    new_state->entries[index] = state->entries[i];
                    located = true;
                    break;
                }
            }
            if(!located){
                push_error(EINVAL, "Failed to find spot for %lu after hash table resize", state->entries[i].page_num);
                free(new_state);
                return hash_resize_err_failure;
            }
        }
    }

    *state_ptr = new_state;// update caller's reference
    free(state);
    return hash_resize_success;
}

static bool allocate_entry_in_tx(txn_state_t** state_ptr, uint64_t page_num, page_hash_entry_t** entry){
    txn_state_t* state = *state_ptr;
    size_t number_of_buckets = get_number_of_buckets(state);
    size_t starting_pos = (size_t)(page_num % number_of_buckets);
    // we use linear probing to find a value in case of collisions
    for(size_t i = 0; i < number_of_buckets; i++){
        size_t index = (i + starting_pos) % number_of_buckets;
        if(state->entries[index].page_num == page_num){
            *entry = &state->entries[index];
            return true;
        }

        if(!state->entries[index].address){
            size_t max_pages = (number_of_buckets * 3/4);
            if(state->modified_pages+1 < max_pages){
                state->modified_pages++;
                state->entries[index].page_num = page_num;
                *entry = &state->entries[index];
                return true;    
            }
            switch(expand_hash_table(state_ptr, number_of_buckets)){
                case hash_resize_success:
                    // try again, now we'll have enough room
                    return allocate_entry_in_tx(state_ptr, page_num, entry);
                case hash_resize_err_no_mem:
                    // we'll accept it here and just have higher
                    // load factor
                    break; 
                case hash_resize_err_failure:
                    push_error(EINVAL, "Failed to add page %lu to the transaction hash table", page_num);
                    return false;
            }
        }
    }

     switch(expand_hash_table(state_ptr, number_of_buckets)){
        case hash_resize_success:
            // try again, now we'll have enough room
            return allocate_entry_in_tx(state_ptr, page_num, entry);
        case hash_resize_err_no_mem: 
            // we are at 100% capacity, can't recover, will error now
            push_error(ENOMEM, "Can't allocate to add page %lu to the transaction hash table", page_num);
            return false;
        case hash_resize_err_failure:
            push_error(EINVAL, "Failed to add page %lu to the transaction hash table", page_num);
            return false;
    }
}

bool get_page(txn_t* tx, page_t* page){
    assert_no_existing_errors();

    page_hash_entry_t* entry;
    if(lookup_entry_in_tx(tx->state, page->page_num, &entry)) {
        page->address = entry->address;
        return true;
    }
    uint64_t offset = page->page_num * PAGE_SIZE;
    if(offset + PAGE_SIZE > tx->state->file_size){
        push_error(ERANGE, "Requests page %lu is outside of the bounds of the file (%lu)", 
            page->page_num, tx->state->file_size);
        return false;
    }

    page->address = ((char*)tx->state->address + offset);
    return true;
}

bool modify_page(txn_t* tx, page_t* page) {
    assert_no_existing_errors();

    page_hash_entry_t* entry;
    if(lookup_entry_in_tx(tx->state, page->page_num, &entry)) {
        page->address = entry->address;
        return true;
    }

    uint64_t offset = page->page_num * PAGE_SIZE;
    if(offset + PAGE_SIZE > tx->state->file_size){
        push_error(ERANGE, "Requests page %lu is outside of the bounds of the file (%lu)", 
            page->page_num, tx->state->file_size);
        return false;
    }
    void* original = ((char*)tx->state->address + offset);
    void* modified;
    int rc = posix_memalign(&modified, PAGE_ALIGNMENT, PAGE_SIZE);
    if (rc){
        push_error(rc, "Unable to allocate memory for a COW page %lu", page->page_num);
        return false;
    }
    memcpy(modified, original, PAGE_SIZE);
    if(!allocate_entry_in_tx(&tx->state, page->page_num, &entry)){
        mark_error();
        free(modified);
        return false;
    }
    entry->address = modified;
    page->address = modified;
    return true;
}



// /*
// bool modify_page(txn_t* tx, uint64_t page, page_header_t** header, void** page_data) {
//     if(get_page_map(tx->page_map, page, header, page_data))
//         return true;

//     if(page > tx->db->current_file_header.size_in_pages){
//         push_error(EINVAL, "Attempted to read page %lu but the max page is %lu", page, tx->db->current_file_header.size_in_pages);
//         return false;
//     }

//     page_header_t* read_header = _get_page_header(tx->db->mapped_memory, &(tx->db->current_file_header), page);
//     void* read_data = _get_page_pointer(tx->db->mapped_memory, page);

//     uint64_t number_of_pages = 1;

//     if(read_header->flags & PAGE_FLAGS_OVERFLOW) {
//         number_of_pages = read_header->overflow_size / PAGE_SIZE + (read_header->overflow_size % PAGE_SIZE ? 1 : 0);
//     }

//     *page_data = allocate_tx_page(tx, number_of_pages);
//     if(!*page_data){
//         push_error(errno, "Unable to allocate memory for modifed copy of page %lu (%lu pages)", page, number_of_pages);
//         return false;
//     }
//     *header = allocate_tx_mem(tx, sizeof(page_header_t));
//     if (!*header){
//         push_error(ENOMEM, "Unable to allocate memory for modified page header %lu", page);
//         release_tx_page(tx, *page_data, number_of_pages);
//         *page_data = 0;
//         return false;
//     } 

//     memcpy(*header, read_header, sizeof(page_header_t));
//     memcpy(*page_data, read_data, PAGE_SIZE * number_of_pages);

//     if(!set_page_map(tx->page_map, page, *header, *page_data, &read_header, &read_data)){
//         push_error(ENOMEM, "Unable to allocate memory for transaction's page map");
//         release_tx_page(tx, *page_data, number_of_pages);
//         release_tx_mem(tx, *header);
//         *header = 0;
//         *page_data = 0;
//         return false;
//     }
//     return true;
// }

// file_header_t* get_current_file_header(txn_t* tx){
//     void* first_page_buffer = get_page_pointer(tx->db->mapped_memory, 0);
//     file_header_t* fst = (file_header_t*)first_page_buffer;
//     file_header_t* snd = (file_header_t*)((char*)first_page_buffer + PAGE_SIZE/2);
//     if (fst->last_txid > snd->last_txid)
//         return fst;
//     return snd;
// }

// */


// void* get_page_pointer(char* base PAGE_ALIGNED, uint64_t page) {
//     return (void*)(base + page * PAGE_SIZE);
// }


// size_t get_txn_size(void) { 
//     return sizeof(txn_t);
// }

// uint64_t get_txn_id(txn_t* tx) {
//     return tx->txid;
// }

// bool modify_page(txn_t* tx, uint64_t page_number, void**page_buffer, uint32_t* number_of_pages_allocated){
//     file_header_t* header = &tx->db->current_file_header;
//     if(page_number >= header->last_allocated_page){
//         push_error(EINVAL, "Page %lu was not allocated in %s", page_number, get_file_name(tx->db->file_handle));
//         return false;
//     }
//     *page_buffer = get_page_pointer(tx->db->mapped_memory, page_number);
//     *number_of_pages_allocated = 1;
//     return true;
// }

// bool allocate_page(txn_t* tx, uint32_t number_of_pages, uint32_t flags, uint64_t* page_number){
//     (void)flags; // currently unused
//     assert(number_of_pages == 1);
//     file_header_t* header = &tx->db->current_file_header;
//     if(header->last_allocated_page + number_of_pages >= header->size_in_pages){
//         push_error(ENOSPC, "Unable to allocate %u page(s) for %s because %lu pages are allocated", number_of_pages,
//             get_file_name(tx->db->file_handle), header->last_allocated_page);
//         return false;
//     }
//     *page_number  = header->last_allocated_page ;
//     header->last_allocated_page += number_of_pages;
//     return true;
// }


// // TEMP impl

// void* allocate_tx_mem(txn_t* tx, uint64_t size){
//     (void)tx;
//     return malloc(size);
// }

// void* allocate_tx_page(txn_t* tx, uint64_t number_of_pages){
//     (void)tx;
//     void*ptr; 
//     int rc = posix_memalign(&ptr, PAGE_SIZE, number_of_pages * PAGE_SIZE);
//     if(!rc)
//         return ptr;
//     errno = rc;
//     return 0;
// }

// void release_tx_mem(txn_t* tx, void* address){
//     (void)tx;
//     free(address);
// }

// void  release_tx_page(txn_t* tx, void* address, uint64_t number_of_pages){
//     (void)tx;
//     (void)number_of_pages;
//     free(address);
// }


