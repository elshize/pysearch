"""Tests query processing."""

import array
import struct
import pytest
from pysearch import index, query


def _index():
    postings = array.array('B')
    lexicon = {}

    # term 0
    lexicon[0] = len(postings)
    header = index.PostingHeader(mask=0, count=3, scoreoffset=36)
    postings.frombytes(header.pack())
    postings.frombytes(struct.pack('<III', 0, 1, 2))  # docs
    postings.frombytes(struct.pack('<III', 3, 3, 3))  # scores

    # term 1
    lexicon[1] = len(postings)
    header = index.PostingHeader(mask=0, count=2, scoreoffset=32)
    postings.frombytes(header.pack())
    postings.frombytes(struct.pack('<II', 0, 2))  # docs
    postings.frombytes(struct.pack('<II', 3, 3))  # scores

    # term 2
    lexicon[2] = len(postings)
    header = index.PostingHeader(mask=index.SHORTMASK, count=1, scoreoffset=3)
    postings.frombytes(header.pack())

    return index.Index(postings, lexicon, 3)


def test_weighted():
    idx = _index()
    docstream = idx.postinglist(1)
    assert list(query.weighted(docstream, 2)) == [(0, 6), (2, 6)]


def test_daatstream():
    idx = _index()
    docstreams = [idx.postinglist(term) for term in [0, 1, 2]]
    assert list(query.daatstream(docstreams)) == [(0, 6), (1, 6), (2, 6)]


def test_daat():
    idx = _index()
    docstreams = [idx.postinglist(term) for term in [0, 1, 2]]
    docstreams = [query.weighted(d, idx) for idx, d in enumerate(docstreams)]
    assert query.daat(docstreams, k=2) == [(1, 6), (0, 3)]


def test_taat():
    idx = _index()
    docstreams = [idx.postinglist(term) for term in [0, 1, 2]]
    acc = idx.accumulator()
    with pytest.raises(ValueError):
        query.taat(docstreams, acc, [0, 3])
    query.taat(docstreams, acc)
    assert acc == [6, 6, 6]


def test_taat_weighted():
    idx = _index()
    docstreams = [idx.postinglist(term) for term in [0, 1, 2]]
    docstreams = [query.weighted(d, idx) for idx, d in enumerate(docstreams)]
    acc = idx.accumulator()
    query.taat(docstreams, acc)
    assert acc == [3, 6, 3]


def test_topk():
    accumulator = [4, 0, 2, 1, 9]
    assert query.topk(accumulator, 1) == [(4, 9)]
    assert query.topk(accumulator, 3) == [(4, 9), (0, 4), (2, 2)]
    assert query.topk(accumulator, 10) == [(4, 9), (0, 4), (2, 2), (3, 1)]
