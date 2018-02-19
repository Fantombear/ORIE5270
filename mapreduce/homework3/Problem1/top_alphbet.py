# -*- coding: utf-8 -*-
"""
Created on Thu Jan 18 10:20:25 2018

@author: Yuan Xiong
"""
from mrjob.job import MRJob
from mrjob.step import MRStep
import re
WORD_RE = re.compile(r"[A-Za-z]+")
class MRMostUsedWord(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   #combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_find_top_word)
        ]

    def mapper_get_words(self, _, line):
        # yield each word in the line
        for word in WORD_RE.findall(line):
            yield word.lower(), 1

    def reducer_count_words(self, word, counts):
        yield None , (sum(counts), word)

    # discard the key; it is just None
    def reducer_find_top_word(self,_, word_count_pairs):
        for w in sorted(word_count_pairs,reverse=True)[:100]:
            yield w


if __name__ == '__main__':
    MRMostUsedWord.run()
