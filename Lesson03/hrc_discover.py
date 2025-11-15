# -*- coding: utf-8 -*-
"""
Created on Tue Oct  5 12:45:37 2021

@author: ThinkPad
"""

from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import os

WORD_RE = re.compile(r"[\w']+")
choice = ""

class MRPrepositionsFinder(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words),
            MRStep(reducer=self.reducer_find_prep_word)
        ]

    def mapper_get_words(self, _, line):
        # set word_list to indicators, convert to lowercase, and strip whitespace
        word_list = set(line.lower().strip() for line in open("/hdfs/user/user/indicators.txt"))

        # set filename to map_input_file
        fileName = os.environ['map_input_file']
        # itterate through each word in line
        for word in WORD_RE.findall(line):
            # if word is in indicators, yield chocie as filename
            if word.lower() in word_list:
                choice = fileName.split('/')[5]
                yield (choice, 1)

    def reducer_find_prep_word(self, choice, counts):
        # each item of choice is (choice, count),
        # so yielding results in value=choice, key=count
        yield (choice, sum(counts))


if __name__ == '__main__':
    MRPrepositionsFinder.run()