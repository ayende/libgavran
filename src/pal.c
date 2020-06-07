#include <stdlib.h> 
#include <errno.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <stddef.h>

#include "pal.h"
#include "errors.h"

const char* _get_file_name(file_handle_t* handle){
    return ((char*)handle + sizeof(file_handle_t)); 
}

size_t get_file_handle_size(const char* path, const char* name) { 
    if(!path || !name)
        return 0;

    size_t path_len = strlen(path);
    size_t name_len = strlen(name);

    if(!path_len || !name_len)
        return 0;

    return sizeof(file_handle_t) + path_len + 1 /* slash */ + name_len + 1 /* null terminating*/;
}

bool create_file(const char* path, const char* name, file_handle_t** handle, void* mem) { 
    *handle = (file_handle_t*)mem;

    // we rely on the fact that foo/bar == foo//bar on linux
    uint64_t path_len = strlen(path);
    uint64_t name_len = strlen(name);
    memcpy((char*)mem + sizeof(file_handle_t), path, path_len); 
    *((char*)mem + sizeof(file_handle_t) + path_len) = '/';
    memcpy((char*)mem + sizeof(file_handle_t) + path_len + 1, name, name_len); 
    
    char* file = (char*)mem + sizeof(file_handle_t);

    if(mkdir(path, S_IRWXU) == -1 ){
        if(errno != EEXIST){
            push_error(errno, "Unable to create directory %s", path);
            return false;
        }
    }

    int fd = open(file, O_CLOEXEC  | O_CREAT | O_RDWR , S_IRUSR | S_IWUSR);
    if (fd == -1){
        push_error(errno, "Unable to open file %s", file);
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
    *address = addr;
    return true;
}

bool unmap_file(void* address, uint64_t size){
    if(munmap(address, size) == -1){
        push_error(EINVAL, "Unable to unmap!");
        return false;
    }
    return true;
}

bool close_file(file_handle_t* handle){
    if(!handle)
        return true;

    if(close(handle->fd) == -1){
        push_error(errno, "Failed to close file %s", _get_file_name(handle));
        return false;
    }

    return true;
}

bool ensure_file_minimum_size(file_handle_t* handle, uint64_t minimum_size){
    int res = fallocate (handle->fd, 0, 0, (int64_t)minimum_size);
    if(res != -1)
        return true;
    
    push_error(errno, "Unable to extend file to size %s to %lu", _get_file_name(handle), minimum_size);
    return false;
}
 
