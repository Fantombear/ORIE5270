from mrjob.job import MRJob
import time

import operator

# questions about yield?
# See https://stackoverflow.com/questions/231767/what-does-the-yield-keyword-do


class MRWordCounter(MRJob):
    def mapper(self, key, line):
        for word in line.split():
            yield word, 1

    def reducer(self, word, counts):
        total = sum(counts)
        yield word, total

if __name__ == '__main__':
    #run: python count_word.py -o 'output_dir' --no-output 'location_input_file or files'
    #e.g. python count_word.py -o 'results_count_word' --no-output 'data_count_word/*.txt'
    st = time.time()
    MRWordCounter.run()
    end = time.time()
    print (end - st)
