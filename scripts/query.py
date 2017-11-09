import os
import sys
import time

from pysearch import index, query


def print_results(topk, titles=None):
    for doc, score in topk:
        if titles:
            print(doc, score, titles[doc], sep='\t')
        else:
            print(doc, score, sep='\t')


def load_titles(indexdir):
    with open(os.path.join(indexdir, 'titles'), 'r') as t:
        return [title[:-1] for title in t]


def main():
    indexdir = sys.argv[1]
    inputfile = sys.argv[2]

    idx = index.load(indexdir)
    titles = load_titles(indexdir)
    start = time.perf_counter()
    numqueries = 0
    with open(inputfile, 'r') as qfile:
        for qid, qline in enumerate(qfile):
            numqueries += 1
            #print(f'{qid} ({titles[qid]})')
            weighted_terms = [
                tuple([int(s) for s in termweight.split(':')])
                for termweight in qline.strip().split(' ')]
            docstreams = [
                query.weighted(idx.postinglist(term), weight)
                for term, weight
                in weighted_terms]
            accumulator = idx.accumulator()
            query.taat(docstreams, accumulator, weighted_terms)
            print_results(query.topk(accumulator, 30), titles)
    end = time.perf_counter()
    avg = (end - start) * 1000 / numqueries
    print(avg)


if __name__ == '__main__':
    main()
