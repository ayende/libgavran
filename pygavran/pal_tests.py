import unittest
import tempfile
from gavran import *

class TestPal(unittest.TestCase):

    def test_get_file_size(self):
        size_of_file_handle = 4
        size = Pal.get_file_handle_size(b"/db/orev")
        self.assertEqual(size, 5 + 2 + 1 + 4 + 1)

    def test_get_bad_file_name_throws(self):
        with self.assertRaises(GarvanError):
            Pal.get_file_handle_size(b"")

    def test_require_db_name_to_not_be_directory(self):
        f = tempfile.NamedTemporaryFile()
        with self.assertRaises(GarvanError) as ge:
            Pal.create_file(f.name + "/db")
        
        self.assertEqual(ge.exception.code(), errno.ENOTDIR)

    def test_works_on_existing_file(self):
        f = tempfile.NamedTemporaryFile()
        h =  Pal.create_file(f.name)
        Pal.close_file(h)

    def test_fail_on_readonly_location(self):
        with self.assertRaises(GarvanError) as ge:
            Pal.create_file("/proc/db")

        self.assertEqual(ge.exception.code(), errno.EPERM)

    def test_fail_on_readonly_dir(self):
        with self.assertRaises(GarvanError) as ge:
            Pal.create_file("/proc/db/orev")
            
        self.assertEqual(ge.exception.code(), errno.EACCES)


if __name__ == '__main__':
    unittest.main()