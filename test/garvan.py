from ctypes import *

libgarvan = cdll.LoadLibrary("../build/garvan.so")  

class DatabaseOptions(Structure):
     _fields_ = [
        ("path", c_char_p), 
        ("name", c_char_p),
        ("minimum_size", c_ulonglong)]

libgarvan.get_database_handle_size.restype = c_size_t
libgarvan.get_database_handle_size.argtypes = [POINTER(DatabaseOptions)]

libgarvan.create_database.restype = c_bool
libgarvan.create_database.argtypes  = [POINTER(DatabaseOptions), POINTER(c_void_p), c_void_p]


class DatabaseScope:

    def __init__(self, options):
        options_ptr = pointer(options)
        size = libgarvan.get_database_handle_size(options_ptr)
        self.mem = create_string_buffer(size)
        self.db = c_void_p(0)
        result = libgarvan.create_database(options_ptr, pointer(self.db), self.mem)
        
        if not result:
            raise "Unable to crete db"

    def __def__(self):
        libgarvan.close_database(self.db) 

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        libgarvan.close_database(self.db) 




with DatabaseScope(DatabaseOptions(b"db", b"orev", 128*1024)) as db:
    print ("here")

print ("done")