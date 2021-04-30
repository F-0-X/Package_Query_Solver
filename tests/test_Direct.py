from unittest import TestCase
# import unittest
from src.Direct import direct
from src.utils import processInputFile


class TestDirect(TestCase):


    def test_2(self):
        path_to_dataset = "data/"

        path_to_input_file = "input/Q2.json"
        query = processInputFile(path_to_input_file)

        direct(query, path_to_dataset)
