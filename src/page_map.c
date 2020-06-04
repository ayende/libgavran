#include <errno.h>
#include <assert.h>
#include <stdlib.h>
#include "private.h"
#include "errors.h"

// partially based on: https://github.com/CyanogenMod/android_system_core/blob/ics/libcutils/hashmap.c
// this is a trivial hash, because we have to have a page map to be able to work
// assumption, value can never be NULL

typedef struct map_entry {
    uint64_t page;
    page_header_t* header;
    void* value;
    struct map_entry* next;
} map_entry_t;

struct page_map {
    uint64_t count_of_items;
    uint64_t number_of_buckets;
    map_entry_t** buckets; 
};

static inline uint64_t get_page_bucket(uint64_t number_of_buckets, uint64_t page) {
    return page & (number_of_buckets - 1);
}

page_map_t* create_page_map(uint32_t initial_capacity){
    
    page_map_t* map = malloc(sizeof(page_map_t));
    if (map == 0) {
        push_error(ENOMEM, "Unable to allocate page map");
        return 0;
    }
    
    // 0.75 load factor.
    uint32_t minimum_bucket_count = initial_capacity * 4 / 3;
    map->number_of_buckets = 1;
    while (map->number_of_buckets <= minimum_bucket_count) {
        // Bucket count must be power of 2.
        map->number_of_buckets <<= 1; 
    }

    map->buckets = calloc(map->number_of_buckets, sizeof(map_entry_t*));
    if (!map->buckets ) {
        push_error(ENOMEM, "Unable to allocate page map %lu buckets", map->number_of_buckets);
        free(map);
        return 0;
    }
    
    map->count_of_items = 0;
    
    return map;
}

static map_entry_t* create_map_entry(uint64_t page, page_header_t* header, void* value) {
    map_entry_t* entry = malloc(sizeof(map_entry_t));
    if (!entry) {
        push_error(ENOMEM, "Unable to allocate page map entry for page: %lu", page);
        return 0;
    }
    entry->page = page;
    entry->value = value;
    entry->header = header; 
    entry->next = NULL;
    return entry;
}

static void maybe_expand_page_map(page_map_t* map) {
    // If the load factor exceeds 0.75...
    if (map->count_of_items > (map->number_of_buckets * 3 / 4)) {
        // Start off with a 0.33 load factor.
        uint64_t new_number_of_buckets = map->number_of_buckets << 1;
        map_entry_t** new_buckets = calloc(new_number_of_buckets, sizeof(map_entry_t*));
        if ( !new_buckets ) {
            // Abort expansion, not an error, because we can proceed
            return;
        }
        
        // Move over existing entries.
        for (uint64_t i = 0; i < map->number_of_buckets; i++) {
            map_entry_t* entry = map->buckets[i];
            while (entry) {
                map_entry_t* next = entry->next;
                uint64_t index = get_page_bucket(new_number_of_buckets, entry->page);
                entry->next = new_buckets[index];
                new_buckets[index] = entry;
                entry = next;
            }
        }

        // Copy over internals.
        free(map->buckets);
        map->buckets = new_buckets;
        map->number_of_buckets = new_number_of_buckets;
    }
}


bool set_page_map(page_map_t* map, uint64_t page, page_header_t* header, void* value, page_header_t**old_header, void** old_value) {
    assert( value && old_value );

    uint64_t index = get_page_bucket(map->number_of_buckets, page);

    map_entry_t** p = &(map->buckets[index]);
    while (true) {
        map_entry_t* current = *p;

        // Add a new entry.
        if (current == NULL) {
            *p = create_map_entry(page, header, value);
            if (!*p ) {
                return false;
            }
            map->count_of_items++;
            *old_value= 0;
            *old_header = 0;
            maybe_expand_page_map(map);
            return true;
        }

        // Replace existing entry.
        if (current->page == page) {
            *old_value = current->value;
            *old_header = current->header;
            current->value = value;
            current->header = header;
            return true;
        }

        // Move to next entry.
        p = &current->next;
    }
}

bool get_page_map(page_map_t* map, uint64_t page, page_header_t** header, void** value){
    uint64_t index = get_page_bucket(map->number_of_buckets, page);

    map_entry_t* entry = map->buckets[index];
    while (entry != NULL) {
        if (entry->page == page) {
            *value = entry->value;
            *header = entry->header;
            return true;
        }
        entry = entry->next;
    }

    return false;
}

bool del_page_map(page_map_t* map, uint64_t page, page_header_t** old_header, void** old_value){
    uint64_t index = get_page_bucket(map->number_of_buckets, page);

    map_entry_t** p = &(map->buckets[index]);

    // Pointer to the current entry.
    
    map_entry_t* current;
    while ((current = *p) != NULL) {
        if (current->page == page) {
            *old_value = current->value;
            *old_header = current->header;
            *p = current->next;
            free(current);
            map->count_of_items--;
            return true;
        }

        p = &current->next;
    }

    return NULL;
}

void destroy_page_map(page_map_t** map, void (*destroyer)(uint64_t page, page_header_t* header, void* value, void* ctx), void* context) {
     for (uint64_t i = 0; i < (*map)->number_of_buckets; i++) {
        map_entry_t* entry = (*map)->buckets[i];
        while (entry != NULL) {
            map_entry_t *next = entry->next;
            destroyer(entry->page, entry->header, entry->value, context);
            free(entry);
            entry = next;
        }
    }
    free((*map)->buckets);
    free(*map);
    *map = 0;
}
