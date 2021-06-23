import filecmp
import os
from unittest import TestCase
from collections import Counter, defaultdict
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
        same_input_another_file = "same_test.txt"
        map_id = 0
        another_map_id = 1
        M = 2
        do_map(input_file, M, map_id, self.input_dir, self.intermediate_dir)
        do_map(same_input_another_file, M, another_map_id, self.input_dir, self.intermediate_dir)
        for m in range(M):
            words = Path(self.intermediate_dir, f"mr-{map_id}-{m}").read_text()
            other_words = Path(self.intermediate_dir, f"mr-{another_map_id}-{m}").read_text()
            self.assertEqual(sorted(words), sorted(other_words))
    
    def reduce_operation(self, reduce_id):
        # evaluate accuract of reduce operation
        N = 2
        map_files = []
        words = []
        for n in range(N):
            map_files.append(f"mr-{n}-{reduce_id}")
            words += Path(self.intermediate_dir, f"mr-{n}-{reduce_id}").read_text().split()
        do_reduce(reduce_id, N, self.intermediate_dir, self.output_dir)
        expected_word_count = Counter(words)
        reduced_result = Path(self.output_dir, f"out-{reduce_id}").read_text()
        reduced_word_count = {}
        for line in reduced_result.splitlines():
            if line:
                word, count = line.split()
                reduced_word_count[word] = int(count)
        self.assertEqual(expected_word_count, reduced_word_count)

    def test_reduce_operation_general(self):
        self.reduce_operation(0)

    def test_reduce_operation_special(self):
        self.reduce_operation(1)


if __name__ == "__main__":
    unittest.main()
