#include <stdlib.h> 
#include <errno.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include "pal.h"
#include "errors.h"

const char* _get_file_name(file_handle_t* handle){
    return ((char*)handle + sizeof(file_handle_t)); 
}

bool create_file(const char* file, file_handle_t** handle) { 
    size_t len = strlen(file) + 1 /* null terminating*/;
    void* mem = malloc(sizeof(file_handle_t) + len);
    if(!mem){
        push_error(ENOMEM, "Unable to allocate memory for file handle's for: %s", file);
        return false; 
    }
    *handle = (file_handle_t*)mem;
    memcpy((char*)mem + sizeof(file_handle_t), file, len);     
    
    int fd = open(file, O_CLOEXEC  | O_CREAT | O_RDWR , S_IRUSR | S_IWUSR);
    if (fd== -1){
        push_error(errno, "Unable to open file %s", file);
        free(handle);
        return false;
    }
    (*handle)->fd = fd;
    return true;
}

bool get_file_size(file_handle_t* handle, uint64_t* size){
    struct stat st;
    int res = fstat(handle->fd, &st);
    if(res != -1){
        *size = (uint64_t)st.st_size;
        return true;
    }
    push_error(errno, "Unable to stat(%s)", _get_file_name(handle));
    return false;
}

bool map_file(file_handle_t* handle, uint64_t size, void** address){
    void* addr = mmap(0, size, PROT_READ | PROT_WRITE, MAP_SHARED, handle->fd, 0);
    if(addr == MAP_FAILED){
        push_error(errno, "Unable to map file %s with size %lu", _get_file_name(handle), size);
        *address = 0;
        return false;
    }
    return true;
}

bool unmap_file(void* address, uint64_t size){
    if(munmap(address, size) == -1){
        push_error(EINVAL, "Unable to unmap!");
        return false;
    }
    return true;
}

bool close_file(file_handle_t** handle){
    if(handle == NULL)
        return true;

    int res = close((*handle)->fd);
    bool result = true;
    if(res != -1){
        goto exit;
    }

    push_error(errno, "Failed to close file %s", _get_file_name(*handle));
    result = false;

exit:
    free(*handle);
    *handle = 0;
    return result;
        
}

bool ensure_file_minimum_size(file_handle_t* handle, uint64_t minimum_size){
    int res = fallocate (handle->fd, 0, 0, (int64_t)minimum_size);
    if(res != -1)
        return true;
    
    push_error(errno, "Unable to extend file to size %s to %lu", _get_file_name(handle), minimum_size);
    return false;
}
 
