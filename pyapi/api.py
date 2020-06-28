import os
import uuid
import errno
from ctypes import *

class mmap_args(Structure):
     _fields_ = [("address", c_void_p),("size", c_size_t)]


class GavranError(Exception):
    def __init__(self, message, codes):
        self.message = message
        self.codes = codes

    def code(self, i = -1):
        if i < 0:
            i = len(self.codes) -1
        return self.codes[i]

    def __str__(self):
        return self.message

class Errors:

    @staticmethod
    def Raise():
        if Errors.get_count() != 0:
            err = GavranError(", ".join(Errors.get_messages()), Errors.get_codes())
            Errors.clear()
            raise err

    @staticmethod
    def print_all():
        gvn.errors_print_all()

    @staticmethod
    def get_count():
        return gvn.errors_get_count()
    
    @staticmethod
    def clear():
            gvn.errors_clear()

    @staticmethod
    def get_errors():
        size = c_size_t()
        codes_ptr = gvn.errors_get_codes(pointer(size))
        msgs_ptr = gvn.errors_get_messages(pointer(size))
        return [(codes_ptr[i], None if msgs_ptr[i] is None else msgs_ptr[i].decode('utf-8')) for i in range(size.value)]

    @staticmethod
    def get_codes():
        size = c_size_t()
        codes_ptr = gvn.errors_get_codes(pointer(size))
        codes = [codes_ptr[i] for i in range(size.value)]
        return codes

    @staticmethod
    def get_messages():
        size = c_size_t()
        msgs_ptr = gvn.errors_get_messages(pointer(size))
        msgs = [msgs_ptr[i].decode('utf-8') for i in range(size.value)]
        return msgs

    @staticmethod
    def push(file, line, func, code):
        gvn.errors_push_new(file, line, func, code)

    @staticmethod
    def append(format):
        gvn.errors_append_message(format)

class PalFS:
    def __init__(self, path):
        self.handle = None
        file = path.encode('utf-8')
        size = c_size_t()
        gvn.palfs_compute_handle_size(file, pointer(size))
        h = create_string_buffer(size.value)
        gvn.palfs_create_file(file, h, 0)
        Errors.Raise()
        self.handle = h

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        
    def close(self):
        if self.handle is None:
            return
        gvn.palfs_close_file(self.handle)
        Errors.Raise()
        self.handle = None

    def size(self):
        size = c_size_t()
        gvn.palfs_get_filesize(self.handle, pointer(size))
        Errors.Raise()
        return size.value

    def name(self):
        return gvn.palfs_get_filename(self.handle).decode('utf-8')

    def set_size(self, len):
        gvn.palfs_set_file_minsize(self.handle, c_long(len))
        Errors.Raise()

    def map(self, offset, size):
        args = mmap_args()
        args.size = size
        gvn.palfs_mmap(self.handle,c_long(offset), pointer(args))
        Errors.Raise()
        return MapRange(args)

    def write(self, offset, val):
        gvn.palfs_write_file(self.handle, c_long(offset), val, len(val))
        Errors.Raise()

class MapRange:
    def __init__(self, args):
        self.args = args

    def __del__(self):
        self.close()

    def __enter__(self):
        return self.args.address

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        
    def close(self):
        if self.args is None:
            return
        gvn.palfs_unmap(pointer(self.args))
        Errors.Raise()
        self.args = None

class DatabaseOptions(Structure):
    _fields_ = [("minimum_size", c_long)]

class DbOrTx(Structure):
    _fields_ = [("state", c_void_p)]

class Page(Structure):
    _fields_ =[("address", c_void_p), ("page_num", c_long), ("overflow_size", c_int), ("_padding", c_int), ("previous",c_void_p)]

class Transaction:
    def __init__(self, s):
        self.s = s

    def allocate(self, size = 8192, nearby=0):
        p = Page()
        p.overflow_size = size
        gvn.txn_allocate_page(pointer(self.s), pointer(p), c_long(nearby))
        Errors.Raise()
        return p

    def free(self, page):
        gvn.txn_free_page(pointer(self.s), pointer(page))
        Errors.Raise()
        
    def get(self, num):
        p = Page()
        p.page_num = num
        gvn.txn_get_page(pointer(self.s), pointer(p))
        Errors.Raise()
        return p

    def modify(self, num):
        p = Page()
        p.page_num = num
        gvn.txn_modify_page(pointer(self.s), pointer(p))
        Errors.Raise()
        return p

    def commit(self):
        gvn.txn_commit(pointer(self.s))
        Errors.Raise()
        pass

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        
    def abandon(self):
        self.s = None

    def close(self):
        if self.s is None:
            return
        gvn.txn_close(pointer(self.s))
        Errors.Raise()
        self.s = None

class Database:
    def __init__(self, path, options):
        self.s = None
        s = DbOrTx()
        gvn.db_create(path.encode('utf-8'), pointer(options), pointer(s))
        Errors.Raise()
        self.s = s

    def test_get_map_at(self, page):
        address = c_void_p()
        gvn.TEST_db_get_map_at(pointer(self.s), c_long(page), pointer(address))
        Errors.Raise()
        return address
    
    def test_wal_last_pos(self):
        pos = gvn.TEST_wal_get_last_write_position(pointer(self.s))
        Errors.Raise()
        return pos
    

    def txn(self, flags):
        t = DbOrTx()
        gvn.txn_create(pointer(self.s), flags, pointer(t))
        Errors.Raise()
        return Transaction(t)

    def write_txn(self):
        return self.txn(2)
    
    def read_txn(self):
        return self.txn(4)

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        
    def close(self):
        if self.s is None:
            return
        gvn.db_close(pointer(self.s))
        Errors.Raise()
        self.s = None


def setup_palfs(gvn):
    methods = [
        ("palfs_compute_handle_size", [c_char_p, POINTER(c_size_t)], c_void_p),
        ("palfs_create_file", [c_char_p, c_void_p], c_void_p, c_int),
        ("palfs_get_filename", [c_void_p], c_char_p),
        ("palfs_set_file_minsize", [c_void_p, c_long], c_void_p),
        ("palfs_close_file", [c_void_p], c_void_p),
        ("palfs_get_filesize", [c_void_p, POINTER(c_size_t)], c_void_p),
        ("palfs_mmap", [c_void_p, c_long, POINTER(mmap_args)], c_void_p),
        ("palfs_unmap", [POINTER(mmap_args)], c_void_p),
        ("palfs_write_file", [c_void_p, c_long, c_void_p, c_size_t], c_void_p)
    ]

    for method in methods:
        m = None
        try:
            m = getattr(gvn, method[0])
        except AttributeError:
            continue
            
        setattr(m, "argtypes", method[1])
        setattr(m, "restype", method[2])

def setup_db(gvn):
    methods = [
        ("db_create", [c_char_p, POINTER(DatabaseOptions), POINTER(DbOrTx)], c_void_p),
        ("db_close", [ POINTER(DbOrTx)], c_void_p),
        ("txn_create", [POINTER(DbOrTx), c_int, POINTER(DbOrTx)], c_void_p),
        ("txn_close", [POINTER(DbOrTx)], c_void_p),
        ("txn_commit",[POINTER(DbOrTx)], c_void_p),
        ("txn_get_page", [POINTER(DbOrTx), POINTER(Page)], c_void_p),
        ("txn_modify_page", [POINTER(DbOrTx), POINTER(Page)], c_void_p),
        ("txn_free_page", [POINTER(DbOrTx), POINTER(Page)], c_void_p),
        ("txn_allocate_page", [POINTER(DbOrTx), POINTER(Page), c_long], c_void_p),
        ("TEST_db_get_map_at", [POINTER(DbOrTx), c_long, POINTER(mmap_args)], c_void_p),
        ("TEST_wal_get_last_write_position", [POINTER(DbOrTx)], c_long),
    ]

    for method in methods:
        m = None
        try:
            m = getattr(gvn, method[0])
        except AttributeError:
            continue
            
        setattr(m, "argtypes", method[1])
        setattr(m, "restype", method[2])


def setup_errors(gvn):
    methods = [
        ("errors_get_count", [], c_size_t),
        ("errors_get_codes", [POINTER(c_size_t)], POINTER(c_int)),
        ("errors_get_messages", [POINTER(c_size_t)], POINTER(c_char_p)),
        ("errors_clear", [], None),
        ("errors_print_all", [], None),
        ("errors_append_message", [c_char_p] , c_void_p),
        ("errors_push_new", [c_char_p,c_int, c_char_p, c_int], c_void_p)
    ]

    for method in methods:
        m = getattr(gvn, method[0])
        setattr(m, "argtypes", method[1])
        setattr(m, "restype", method[2])

gvn = cdll.LoadLibrary("./build/gavran.so")  

setup_errors(gvn)
setup_palfs(gvn)
