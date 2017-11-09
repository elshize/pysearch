"""Tests index-related code."""

import array
import struct
from pysearch import index


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
    header = index.PostingHeader(mask=28, count=1, scoreoffset=3)
    postings.frombytes(header.pack())

    return index.Index(postings, lexicon, numdocs=3)


def test_postings():
    idx = _index()
    assert [(0, 3), (1, 3), (2, 3)] == list(idx.postinglist(0))
    assert [(0, 3), (2, 3)] == list(idx.postinglist(1))
    assert [(1, 3)] == list(idx.postinglist(2))


def test_accumulator():
    idx = _index()
    assert idx.accumulator() == [0, 0, 0]
