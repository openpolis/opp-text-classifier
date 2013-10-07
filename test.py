__author__ = 'guglielmo'
import nltk
from nltk.corpus.util import LazyCorpusLoader
from nltk.corpus.reader import *

interrogazioni_macro = LazyCorpusLoader(
    'opp_interrogazioni_macro',
    CategorizedPlaintextCorpusReader,
    '(training|test).*',
    cat_file='cats.txt', cat_delimiter=','
)

print "computing total number of words"
print len(interrogazioni_macro.words())

print "computing FreqDist over all words"
all_words = nltk.FreqDist(w.lower() for w in interrogazioni_macro.words())
print all_words.keys()[:100]
