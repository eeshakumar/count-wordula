import filecmp
import os
from unittest import TestCase
from collections import Counter
from pathlib import Path
import unittest

from src.utils import do_map, do_reduce


class MapReduceTests(TestCase):

    def setUp(self):
        self.input_dir = os.path.join(os.getcwd(), "test/data/test_inputs")
        self.intermediate_dir = os.path.join(os.getcwd(), "test/data/test_intermediate")
        self.output_dir = os.path.join(os.getcwd(), "test/data/test_outputs")

    def test_map_operation(self):
        # test consistency of map operation
        input_file = "test.txt"
        map_id = 0
        another_map_id = 1
        M = 2
        do_map(input_file, M, map_id, self.input_dir, self.intermediate_dir)
        do_map(input_file, M, another_map_id, self.input_dir, self.intermediate_dir)
        for m in range(M):
            words = Path(self.intermediate_dir, f"mr-{map_id}-{m}").read_text()
            other_words = Path(self.intermediate_dir, f"mr-{another_map_id}-{m}").read_text()
            self.assertEqual(sorted(words), sorted(other_words))
    

    def test_reduce_operation(self):
        # test accuracy of reduce operation
        N = 2
        map_files = []
        another_map_files = []
        reduce_id = 0
        another_reduce_id = 1
        for n in range(N):
            map_files.append(f"mr-{n}-{reduce_id}")
            another_map_files.append(f"mr-{n}-{another_reduce_id}")
        do_reduce(reduce_id, N, self.intermediate_dir, self.output_dir)
        do_reduce(another_reduce_id, N, self.intermediate_dir, self.output_dir)
        reduced_file = os.path.join(self.output_dir, f"out-{reduce_id}")
        another_reduce_file = os.path.join(self.output_dir, f"out-{another_reduce_id}")
        filecmp.cmp(reduced_file, another_reduce_file, shallow=True)


if __name__ == "__main__":
    unittest.main()
