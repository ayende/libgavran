import os
from ctypes import *


class DatabaseOptions(Structure):
     _fields_ = [
        ("path", c_char_p), 
        ("name", c_char_p),
        ("minimum_size", c_ulonglong)]

class ErrorMessage(Structure):
    _fields_ = [
        ("msg",  c_char_p),
        ("code", c_size_t)
    ]

libgarvan = cdll.LoadLibrary("../build/garvan.so")  

libgarvan.get_errors.restype = POINTER(ErrorMessage)
libgarvan.get_errors.argtypes = [POINTER(c_size_t)]

libgarvan.clear_errors.restype = None
libgarvan.clear_errors.argtypes = []

libgarvan.get_txn_size.restype = c_size_t

libgarvan.get_txn_id.restype = c_ulonglong

libgarvan.create_transaction.argtypes = [c_void_p, c_int, c_void_p]
libgarvan.create_transaction.restype = c_bool

libgarvan.close_transaction.argtypes = [c_void_p]
libgarvan.close_transaction.restype = c_bool

libgarvan.get_database_handle_size.restype = c_size_t
libgarvan.get_database_handle_size.argtypes = [POINTER(DatabaseOptions)]

libgarvan.create_database.restype = c_bool
libgarvan.create_database.argtypes  = [POINTER(DatabaseOptions), c_void_p]

libgarvan.allocate_page.restype = c_bool
libgarvan.allocate_page.argtypes = [c_void_p, c_uint, c_uint, POINTER(c_ulonglong)]

libgarvan.modify_page.restype = c_bool
libgarvan.modify_page.argtypes = [c_void_p, c_ulonglong, POINTER(c_void_p), POINTER(c_uint)]

class GarvanError(Exception):
    def __init__(self, message):
        self.message = message

    @staticmethod
    def Raise():
        size = c_size_t()
        errs = libgarvan.get_errors(pointer(size))
        
        if size == 0:
            return

        msg = "\n"
        for i in reversed(range(size.value)):
            msg+= errs[i].msg.decode("utf-8")  + "\n"

        raise GarvanError(msg)


class TransactionScope:
    READ_ONLY  = 1
    READ_WRITE = 2

    SINGLE_PAGE = 1

    def __init__(self, tx):
        self.tx = tx

    def id(self):
        return libgarvan.get_txn_id(tx)

    def modify_page(self, page_number):
        page_buffer = c_void_p()
        number_of_pages = c_uint()
        if not libgarvan.modify_page(self.tx, c_ulonglong(page_number), pointer(page_buffer), pointer(number_of_pages)):
            GarvanError.Raise()
        return (page_buffer, number_of_pages)

    def allocate_page(self, number_of_pages, flags = SINGLE_PAGE):
        page_number = c_ulonglong()
        if not libgarvan.allocate_page(self.tx, c_uint(number_of_pages), c_uint(flags), pointer(page_number)):
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
        if not libgarvan.close_transaction(tx):
            GarvanError.Raise()
        

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class DatabaseScope:
    PAGE_SIZE = 8192

    def __init__(self, options):
        options_ptr = pointer(options)
        size = libgarvan.get_database_handle_size(options_ptr)
        self.db = create_string_buffer(size)
        result = libgarvan.create_database(options_ptr, self.db)
        
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
        if not libgarvan.close_database(db):
            GarvanError.Raise()

    def write_txn(self):
        size = libgarvan.get_txn_size()
        tx = create_string_buffer(size)
        result = libgarvan.create_transaction(self.db, TransactionScope.READ_WRITE, tx)
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

