"""Query processing"""


def weighted(docstream, weight):
    """Returns a docstream with scores multipied by weight."""
    for doc, score in docstream:
        yield doc, score * weight


def taat(docstreams, accumulator, terms=None):
    """Traverses all documents and accumulates scores."""
    if terms is not None and len(terms) != len(docstreams):
        raise ValueError(
            f'supplied term list with different length ({len(terms)}) from '
            f'docstreams ({len(docstreams)})')
    for termind, docstream in enumerate(docstreams):
        #if terms:
        #    print('Processing term: ', terms[termind])
        for doc, score in docstream:
            #if doc >= len(accumulator):
            #    print(doc, len(accumulator))
            #    break
            accumulator[doc] += score


# TODO: must implement more efficient top-k
def topk(accumulator, k):
    """Extract k highest ranked documents from accumulator."""
    return sorted(
        [(doc, score) for doc, score in enumerate(accumulator) if score > 0],
        key=lambda entry: entry[1],
        reverse=True)[:k]
