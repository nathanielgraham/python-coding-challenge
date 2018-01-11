
import unittest
import filecmp
import os


class TestOutput(unittest.TestCase):

    def test_equal(self):
        os.system("python route.py test_requests.csv test_vlans.csv")
        self.assertTrue(filecmp.cmp("test_output.csv", "output.csv", shallow=True))


if __name__ == '__main__':
    unittest.main()

