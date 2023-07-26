#!/usr/bin/env python3
import unittest


class TestEq(unittest.TestCase):
    def test_list_int(self):
        """
        Basic unit test.
        """
        data1 = 1
        data2 = 1
        self.assertEqual(data1, data2)


if __name__ == '__main__':
    unittest.main()
