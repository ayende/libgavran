from gavran import *
import os
import errno
from ctypes import *

class mmap_args(Structure):
     _fields_ = [("address", c_void_p),("size", c_size_t)]

methods = [
    ("palfs_compute_handle_size", [c_char_p, POINTER(c_size_t)], c_void_p),
    ("palfs_create_file", [c_char_p, c_void_p], c_void_p),
    ("palfs_get_filename", [c_void_p], c_char_p),
    ("palfs_set_file_minsize", [c_void_p, c_long], c_void_p),
    ("palfs_close_file", [c_void_p], c_void_p),
    ("palfs_get_filesize", [c_void_p, POINTER(c_size_t)], c_void_p),
    ("palfs_mmap", [c_void_p, c_long, POINTER(mmap_args)], c_void_p),
    ("palfs_unmap", [POINTER(mmap_args)], c_void_p),
    ("palfs_write_file", [c_void_p, c_long, c_void_p, c_size_t], c_void_p)
]



for method in methods:
    m = getattr(gvn, method[0])
    setattr(m, "argtypes", method[1])
    setattr(m, "restype", method[2])

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
        return [(codes_ptr[i], None if msgs_ptr[i] is None else str(msgs_ptr[i])) for i in range(size.value)]

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
        msgs = [str(msgs_ptr[i]) for i in range(size.value)]
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
        if not gvn.palfs_compute_handle_size(file, pointer(size)):
            Errors.Raise()
        self.handle = create_string_buffer(size.value)
        if not gvn.palfs_create_file(file, self.handle):
            self.handle = None
            Errors.Raise()

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        
    def close(self):
        if self.handle is None:
            return
        if not gvn.palfs_close_file(self.handle):
            Errors.Raise()
        self.handle = None

    def size(self):
        size = c_size_t()
        if not gvn.palfs_get_filesize(self.handle, pointer(size)):
            Errors.Raise()
        return size.value

    def name(self):
        return str(gvn.palfs_get_filename(self.handle))

    def set_size(self, len):
        if not gvn.palfs_set_file_minsize(self.handle, c_long(len)):
            Errors.Raise()

    def map(self, offset, size):
        args = mmap_args()
        args.size = size
        if not gvn.palfs_mmap(self.handle,c_long(offset), pointer(args)):
            Errors.Raise()
        return MapRange(args)

    def write(self, offset, val):
        if not gvn.palfs_write_file(self.handle, c_long(offset), val, len(val)):
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
        if not gvn.palfs_unmap(pointer(self.args)):
            Errors.Raise()
        self.args = None