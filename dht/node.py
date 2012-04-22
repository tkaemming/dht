class Node(object):
    pass


class SimpleNode(Node):
    def __init__(self, id=None):
        self.id = id
        self.values = {}

    def __repr__(self):
        return '<%s: %s>' % (self.__class__.__name__, hash(self))

    def __hash__(self):
        return hash(self.id or id(self))

    def __len__(self):
        return len(self.values)

    def __getitem__(self, key):
        return self.values[key]

    def __setitem__(self, key, value):
        self.values[key] = value

    def __delitem__(self, key):
        del self.values[key]
