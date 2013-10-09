import random

__author__ = 'guglielmo'

import nltk
from nltk.corpus.util import LazyCorpusLoader
from nltk.corpus.reader import *

def document_features(doc_words, top_words):
    """
    Basic features extraction from a list of words in a document (doc_words).
    For all words in top_words (most used in all corpus),
    set True/False if the word is in the document's words set.
    """
    # transform the list of words into a set to optimize search
    doc_words_set = set(doc_words)

    # build features dictionary
    features = {}
    for word in top_words:
        features['contains(%s)' % word] = (word in doc_words_set)
    return features


interrogazioni = LazyCorpusLoader(
    'opp_interrogazioni_macro',
    CategorizedPlaintextCorpusReader,
    r'\d*', cat_file='cats.txt', cat_delimiter=','
)

print "computing FreqDist over all words"
all_words = nltk.FreqDist(w.lower() for w in interrogazioni.words())
top_words = all_words.keys()[:2000]


print "generating list of documents for each category"
documents = [
    (list(interrogazioni.words(fileid)), category)
    for category in interrogazioni.categories()
    for fileid in interrogazioni.fileids(category)
]
random.shuffle(documents)

print "building the classifier"
featuresets = [(document_features(d, top_words), c) for (d,c) in documents]
train_set, test_set = featuresets[1000:], featuresets[:1000]
classifier = nltk.NaiveBayesClassifier.train(train_set)

print "classifier accuracy: ", nltk.classify.accuracy(classifier, test_set)






