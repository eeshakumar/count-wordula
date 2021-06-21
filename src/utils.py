import re
import os
from multiprocessing import Pool
from pathlib import Path
from collections import Counter, defaultdict
from itertools import chain
import time


INPUT_DIR = os.path.join(os.getcwd(), "data/inputs")
OUTPUT_DIR = os.path.join(os.getcwd(), "data/outputs")
INTERMEDIATE_DIR = os.path.join(os.getcwd(), "data/intermediate")


def collect_map_tasks():
    return os.listdir(os.path.join(os.getcwd(), "data/inputs"))


def collect_reduce_tasks():
    return os.listdir(os.path.join(os.getcwd(), "data/intermediate"))


def read_file_linebyline(f):
    return Path(f).read_text().splitlines()


def do_map(input_file, M, map_id):
    print(f"Working on {input_file} with map id {map_id} and {M} reduceable buckets")
    buckets = defaultdict([])
    text = Path(input_file).read_text()
    all_words = list(re.findall(r"([\w]+['][\w]+)", text))
    for word in all_words:
        print(word)
        buckets[ord(word[0]) % M].append(word)
    for key, value in buckets.items():
        text = "\n".join(value)
        Path(INTERMEDIATE_DIR, f"mr-{map_id}-{key}").write_text(text)
    print("Map Task completed for {input_file}")
    return


def do_reduce(reduce_id, N):
    files_to_reduce = []
    for n in range(N):
        files_to_reduce.append(os.path.join(INTERMEDIATE_DIR, f"mr-{n}-{reduce_id}"))
    # all_words = []
    # start = time.time()
    # with Pool(5) as p:
    #     all_words = list(chain(*p.map(read_file_linebyline, files_to_reduce)))
    all_words = []
    for f in files_to_reduce:
        all_words += Path(f).read_text().splitlines()
    word_counts = Counter(all_words)
    output_file = os.path.join(OUTPUT_DIR, f"out-{reduce_id}")
    with open(output_file, 'w+') as f:
        for word, count in word_counts.items():
            f.write(f"{word} {count}\n")
    print("Reduce task completed for {reduce_id}")
    return
