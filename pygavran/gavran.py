import os
import errno
from ctypes import *


class DatabaseOptions(Structure):
     _fields_ = [
        ("path", c_char_p), 
        ("name", c_char_p),
        ("minimum_size", c_ulonglong)]


gvn = cdll.LoadLibrary("../build/garvan.so")  

gvn.get_errors_messages.restype = POINTER(c_char_p)
gvn.get_errors_messages.argtypes = [POINTER(c_size_t)]

gvn.get_errors_codes.restype = POINTER(c_int)
gvn.get_errors_codes.argtypes = [POINTER(c_size_t)]


gvn.clear_errors.restype = None
gvn.clear_errors.argtypes = []

# gvn.get_txn_size.restype = c_size_t

# gvn.get_txn_id.restype = c_ulonglong

# gvn.create_transaction.argtypes = [c_void_p, c_int, c_void_p]
# gvn.create_transaction.restype = c_bool

# gvn.close_transaction.argtypes = [c_void_p]
# gvn.close_transaction.restype = c_bool

# gvn.get_database_handle_size.restype = c_size_t
# gvn.get_database_handle_size.argtypes = [POINTER(DatabaseOptions)]

# gvn.create_database.restype = c_bool
# gvn.create_database.argtypes  = [POINTER(DatabaseOptions), c_void_p]

# gvn.allocate_page.restype = c_bool
# gvn.allocate_page.argtypes = [c_void_p, c_uint, c_uint, POINTER(c_ulonglong)]

# gvn.modify_page.restype = c_bool
# gvn.modify_page.argtypes = [c_void_p, c_ulonglong, POINTER(c_void_p), POINTER(c_uint)]

gvn.get_file_handle_size.argtypes = [c_char_p, POINTER(c_size_t)]
gvn.get_file_handle_size.restype = c_bool

gvn.create_file.argtypes = [c_char_p,  c_void_p]
gvn.create_file.restype = c_bool

gvn.close_file.argtypes = [c_void_p]
gvn.close_file.restype = c_bool

class Pal:

    @staticmethod
    def get_file_handle_size(path):
        size = c_size_t()
        if not gvn.get_file_handle_size(path, byref(size)):
            GarvanError.Raise()

        return size.value

    @staticmethod
    def create_file(path):
        size = Pal.get_file_handle_size(path)
        buffer = create_string_buffer(size)
        if not gvn.create_file(path, buffer):
            GarvanError.Raise()
        return buffer

    @staticmethod
    def close_file(handle):
        if not gvn.close_file(handle):
            GarvanError.Raise()


class GarvanError(Exception):
    def __init__(self, message, codes):
        self.message = message
        self.codes = codes

    def code(self, i = -1):
        if i < 0:
            i = len(self.codes) -1
        return self.codes[i]

    def __str__(self):
        return self.message


    @staticmethod
    def Raise():
        size = c_size_t()
        errs = gvn.get_errors_messages(pointer(size))
        codes = gvn.get_errors_codes(pointer(size))
        
        if size == 0:
            return

        msg = "\n"
        codes_arr = []
        for i in reversed(range(size.value)):
            msg+= errs[i].decode("utf-8")  + "\n"
            codes_arr.append(codes[i])


        gvn.clear_errors()

        raise GarvanError(msg, codes_arr)


class TransactionScope:
    READ_ONLY  = 1
    READ_WRITE = 2

    SINGLE_PAGE = 1

    def __init__(self, tx):
        self.tx = tx

    def id(self):
        return gvn.get_txn_id(tx)

    def modify_page(self, page_number):
        page_buffer = c_void_p()
        number_of_pages = c_uint()
        if not gvn.modify_page(self.tx, c_ulonglong(page_number), pointer(page_buffer), pointer(number_of_pages)):
            GarvanError.Raise()
        return (page_buffer, number_of_pages)

    def allocate_page(self, number_of_pages, flags = SINGLE_PAGE):
        page_number = c_ulonglong()
        if not gvn.allocate_page(self.tx, c_uint(number_of_pages), c_uint(flags), pointer(page_number)):
            GarvanError.Raise()
        return page_number.value

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def close(self):
        if self.tx is None:
            return
        tx = self.tx
        self.tx = None
        if not gvn.close_transaction(tx):
            GarvanError.Raise()
        

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class DatabaseScope:
    PAGE_SIZE = 8192

    def __init__(self, options):
        options_ptr = pointer(options)
        size = gvn.get_database_handle_size(options_ptr)
        self.db = create_string_buffer(size)
        result = gvn.create_database(options_ptr, self.db)
        
        if not result:
            GarvanError.Raise()

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        
    def close(self):
        if self.db is None:
            return
        db = self.db
        self.db = None
        if not gvn.close_database(db):
            GarvanError.Raise()

    def write_txn(self):
        size = gvn.get_txn_size()
        tx = create_string_buffer(size)
        result = gvn.create_transaction(self.db, TransactionScope.READ_WRITE, tx)
        if not result:
            GarvanError.Raise()

        return TransactionScope(tx)


if __name__ == "__main__":
    os.remove("db/orev")
    with DatabaseScope(DatabaseOptions(b"db", b"orev", 128*1024)) as db:
        page_num = 0
        with db.write_txn() as tx:
            tx.allocate_page(1)
            (page, allocated_pages) = tx.modify_page(page_num)
            page_buffer = cast(page, POINTER(c_byte))
            page_buffer[0] = 1
            print(page_buffer[0])
            page_buffer[0] += 1

        with db.write_txn() as tx:
            (page, allocated_pages) = tx.modify_page(page_num)
            page_buffer = cast(page, POINTER(c_byte))
            print(page_buffer[0])
            page_buffer[0] = 3
            print(page_buffer[0])

