from mrjob.job import MRJob

# from mrjob.step import MRStep
import re
import sys

REGEX_ONLY_WORDS = '(?<=\")[\w\'\.]+(?=\")'

# Referência dos passos:
# https://towardsdatascience.com/tf-idf-calculation-using-map-reduce-algorithm-in-pyspark-e89b5758e64c


class MRDataMining(MRJob):

    def mapper(self, _, line):
        try:
            key, value = line.split('\t')
            file, token = re.findall(REGEX_ONLY_WORDS, key)
            yield '{:04d}'.format(int(value)), (token, file)
        except:
            sys.stderr.write("Erro na linha - conteudo:  " + line)

    def reducer(self, count, words):
        for word in words:
            yield count, word

if __name__ == "__main__":
    MRDataMining.run()


