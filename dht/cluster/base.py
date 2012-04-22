from collections import OrderedDict
from itertools import takewhile

from dht.utils import last


class Cluster(object):
    def __init__(self, members):
        self.hash = hash
        self.members = OrderedDict(((self.hash(node), node) for node in members))

    def __len__(self):
        return sum((len(node) for node in self.members.values()))

    def __getitem__(self, key):
        return self.location(key)[key]

    def __setitem__(self, key, value):
        self.location(key)[key] = value

    def __delitem__(self, key):
        del self.location(key)[key]

    def location(self, key):
        """
        Returns where a given key should be stored.
        """
        hashed = self.hash(key)
        try:
            return last(takewhile(lambda pair: pair[0] <= hashed,
                self.members.items()))[1]
        except ValueError:
            # "wrap around" the ring of nodes to the last node if no nodes
            # have a hashed value that is lower than or equal to the hashed
            # value of the key
            return self.members.values()[-1]
