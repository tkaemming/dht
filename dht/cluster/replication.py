from contextlib import contextmanager

from dht.cluster.base import Cluster
from dht.utils import rehash, quorum


class ReplicatedClusterMixin(object):
    def __init__(self, members, replica_count=3, read_durability=None, write_durability=None, *args, **kwargs):
        self.replica_count = 3
        self.read_durability = read_durability
        self.write_durability = write_durability
        super(ReplicatedClusterMixin, self).__init__(members, *args, **kwargs)

    @contextmanager
    def __call__(self, read_durability=None, write_durability=None):
        # We need to retrieve the original durability values -- not the
        # property values -- to allow for dynamic quorum calculation to
        # continue after the context manager exits.
        original_read_durability = self._read_durability
        if read_durability is not None:
            self.read_durability = read_durability

        original_write_durability = self._write_durability
        if write_durability is not None:
            self.write_durability = write_durability

        yield

        self.read_durability = original_read_durability
        self.write_durability = original_write_durability

    # TODO: Actually implement tunable durability/quorums.

    def __getitem__(self, key):
        keys = self.rehash(key)
        values = []
        for key in keys:
            values.append(super(ReplicatedClusterMixin, self).__getitem__(key))

        # TODO: Add conflict resolution logic.
        assert len(set(values)) == 1
        return values[0]

    def __setitem__(self, key, value):
        keys = self.rehash(key)
        for key in keys:
            super(ReplicatedClusterMixin, self).__setitem__(key, value)

    def __delitem__(self, key):
        keys = self.rehash(key)
        for key in keys:
            super(ReplicatedClusterMixin, self).__delitem__(key)

    def rehash(self, key):
        return map(lambda hash: '%s:%s' % (key, hash),
            rehash(key, self.replica_count))

    def get_read_durability(self):
        return getattr(self, '_read_durability', None) or quorum(self.replica_count)

    def set_read_durability(self, value):
        if value is not None and value < 1:
            raise ValueError('Read durability must be greater than zero')
        elif value > self.replica_count:
            raise ValueError('Read durability may not be greater than the cluster replica count')

        self._read_durability = value

    read_durability = property(get_read_durability, set_read_durability)

    def get_write_durability(self):
        value = getattr(self, '_write_durability', None)
        if value is not None:
            return value
        else:
            return quorum(self.replica_count)

    def set_write_durability(self, value):
        if value > self.replica_count:
            raise ValueError('Write durability may not be greater than the cluster replica count')

        self._write_durability = value

    write_durability = property(get_write_durability, set_write_durability)


class ReplicatedCluster(ReplicatedClusterMixin, Cluster):
    pass
