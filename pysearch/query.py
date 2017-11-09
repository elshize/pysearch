"""Query processing"""

import heapq
from collections import namedtuple
from pysearch import index


def weighted(docstream, weight):
    """Returns a docstream with scores multipied by weight."""
    for doc, score in docstream:
        yield index.Posting(doc, score * weight)


def taat(docstreams, accumulator, terms=None):
    """Traverses all documents and accumulates scores."""
    if terms is not None and len(terms) != len(docstreams):
        raise ValueError(
            f'supplied term list with different length ({len(terms)}) from '
            f'docstreams ({len(docstreams)})')
    for docstream in docstreams:
        for doc, score in docstream:
            accumulator[doc] += score


def daatstream(docstreams):
    """Returns a merged doc stream in order of increasing docIDs."""
    entry = namedtuple('Entry', 'head term tail')
    postinglists = []
    for term, docstream in enumerate(docstreams):
        head, *tail = docstream
        if head is not None:
            heapq.heappush(postinglists, entry(head, term, tail))

    while postinglists:
        mindoc = postinglists[0].head.doc
        score = 0
        while postinglists and postinglists[0].head.doc == mindoc:
            top = postinglists[0]
            score += top.head.score
            if postinglists[0].tail:
                head, *tail = postinglists[0].tail
                postinglist = entry(head, top.term, tail)
                heapq.heapreplace(postinglists, postinglist)
            else:
                heapq.heappop(postinglists)
        yield index.Posting(mindoc, score)


def daat(docstreams, k=10):
    return heapq.nlargest(k, daatstream(docstreams), key=lambda p: p.score)


# TODO: must implement more efficient top-k
def topk(accumulator, k):
    """Extract k highest ranked documents from accumulator."""
    return sorted(
        [(doc, score) for doc, score in enumerate(accumulator) if score > 0],
        key=lambda entry: entry[1],
        reverse=True)[:k]
