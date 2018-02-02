# NLTK libraries
import nltk
from nltk import *
try:
    import Queue as queue
except ImportError:
    import queue

def extract_np(des):
    tokens = nltk.word_tokenize(des)
    tokens_tag = nltk.pos_tag(tokens)

    # define chunking regex expression: opt det + adj + noun
    grammar = "NP: {<DT>?<JJ.*>*<NN.*>+}"
    cp = nltk.RegexpParser(grammar)
    np_ary = []
    root_tree = cp.parse(tokens_tag)

    #st = root_tree.subtrees()
    for i in list( root_tree.subtrees(filter=lambda x: x.label() == 'NP')):
        st = ""
        for t in i.leaves():
            st += t[0] + " "
        print(st)
        np_ary.append(st)
    return np_ary


if __name__ == '__main__':
    #extract_np("this is a yellow dog, and it a fat one. it's funny the white house. the house?")
    q = queue.PriorityQueue()