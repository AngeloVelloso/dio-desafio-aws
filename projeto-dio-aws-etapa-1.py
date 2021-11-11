from mrjob.job import MRJob

# from mrjob.step import MRStep
import re
import os

REGEX_ONLY_WORDS = "[\w']+"
REGEX_ONLY_FILE = "(?=data/).+\.txt$"

# Referência dos passos:
# https://towardsdatascience.com/tf-idf-calculation-using-map-reduce-algorithm-in-pyspark-e89b5758e64c


class MRDataMining(MRJob):
    def mapper(self, _, line):
        INPUT_FILE = os.environ["mapreduce_map_input_file"]
        file = re.findall(REGEX_ONLY_FILE, INPUT_FILE)[0][5:]
        words = re.findall(REGEX_ONLY_WORDS, line)
        for word in words:
            yield (file, word.lower()), 1

    def reducer(self, word, values):
        yield word, sum(values)


if __name__ == "__main__":
    MRDataMining.run()
