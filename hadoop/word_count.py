from mrjob.job import MRJob
from mrjob.step import MRStep
import re


class WordCount(MRJob):

    def steps(self):
        return [
            MRStep(
                mapper= self.map_words,
                reducer= self.reduce_words
            )
        ]

    def map_words(self,_ , line):
        words = re.findall(r"\w+",line.lower())
        for word in words:
            yield word, 1

    def reduce_words(self,words, values):
        yield words, sum(values)

if __name__ == '__main__':
    WordCount.run()