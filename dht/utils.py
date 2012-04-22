import hashlib

from math import ceil


def md5(value):
    return hashlib.md5(str(value)).hexdigest()


def last(iterator):
    """Returns the last item in an iterator."""
    for item in iterator:
        last = item

    try:
        return last
    except NameError:
        raise ValueError('Cannot fetch the last item from an empty iterator')


def rehash(key, count, hash=md5):
    hashed = hash(key)
    if count > 1:
        return [hashed] + rehash(hashed, count - 1, hash)
    else:
        return [hashed]


def quorum(n):
    """The number of nodes that must respond to a query for quorum to occur."""
    return int(ceil(n / 2.0))
