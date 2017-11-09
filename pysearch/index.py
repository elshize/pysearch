"""Index reading and handling."""


import json
import struct
from collections import namedtuple

import attr
from py import path  # pylint: disable=no-name-in-module


Posting = namedtuple('Posting', 'doc score')


@attr.s
class PostingHeader(object):
    """A container for a posting list header."""
    mask = attr.ib()
    count = attr.ib()
    scoreoffset = attr.ib()
    docoffset = attr.ib(24, init=False)

    fmt = struct.Struct('<IIIIII')

    def pack(self):
        """Packs the header to bytes."""
        return self.fmt.pack(self.mask, self.count, 0, self.scoreoffset, 0, 0)

    def is_short(self):
        """Returns true if the following posting list is short.

        Short means that there is only one posting and it is included in the
        header."""
        return self.mask == 268435456

    def short_postinglist(self):
        """Returns the short posting list included in the header."""
        assert self.is_short(), \
            'asked for the short posting list but it is not'
        return [Posting(doc=self.count, score=self.scoreoffset)]

    @staticmethod
    def unpack_from(buf, offset):
        """Creates a header from a binary input."""
        mask, count, _, scoreoffset, _, _ = PostingHeader.fmt.unpack_from(
            buf, offset)
        return PostingHeader(mask, count, scoreoffset)


@attr.s
class Index(object):
    """In-memory index representation."""

    postings = attr.ib()
    lexicon = attr.ib()
    numdocs = attr.ib()

    def _postinggen(self, listoffset, header):
        posting = 0
        while posting < header.count:
            doc, = struct.unpack_from(
                '<I', self.postings,
                listoffset + header.docoffset + posting * 4)
            score, = struct.unpack_from(
                '<I', self.postings,
                listoffset + header.scoreoffset + posting * 4)
            yield Posting(doc, score)
            posting += 1

    def postinglist(self, term):
        """Retrieve the posting list generator of term."""
        listoffset = self.lexicon[term]
        assert listoffset < len(self.postings), 'offset larger than data size'
        header = PostingHeader.unpack_from(self.postings, listoffset)
        if header.is_short():
            return header.short_postinglist()
        return self._postinggen(listoffset, header)

    def accumulator(self):
        """Returns an initialized empty accumulator array."""
        return [0 for _ in range(self.numdocs)]


def load(indexdir):
    """Loads index from disk to memory."""
    indexdir = path.local(indexdir)
    manifestfile = (indexdir / 'manifest.json')
    postingfile = (indexdir / 'postings.dat')
    lexiconfile = (indexdir / 'dictionary.dat')
    assert manifestfile.exists(), 'missing lexicon file'
    assert postingfile.exists(), 'missing postings file'
    assert lexiconfile.exists(), 'missing lexicon file'

    manifest = json.loads(manifestfile.read())
    numdocs = int(manifest['collection_size'])
    postings = postingfile.read_binary()
    postings_size = len(postings)
    lexicon = dict()
    with lexiconfile.open('br') as lexfile:
        lexfile.read(24)  # Skip header
        for _ in range(int(manifest['dictionary_size'])):
            term, = struct.unpack('<Q', lexfile.read(8))
            offset, = struct.unpack('<Q', lexfile.read(8))
            assert offset < postings_size, 'offset larger than data size'
            lexicon[term] = offset
    return Index(postings, lexicon, numdocs)
