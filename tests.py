import sys

from dht.cluster import Cluster, ReplicatedCluster
from dht.node import SimpleNode
from dht.utils import rehash, quorum


def test_single_simple_node():
    """
    Test basic interface with only a single node.
    """
    node = SimpleNode(id=1)
    cluster = Cluster((node,))

    # hashing lookup
    assert cluster.location(0) == node
    assert cluster.location('hello') == node
    assert cluster.location(sys.maxint) == node

    # assignment and retrieval
    cluster['hello'] = 'world'
    assert cluster['hello'] == 'world'
    assert len(cluster) == 1

    # deletion
    del cluster['hello']
    try:
        cluster['hello']
        assert False
    except KeyError:
        assert True

    assert len(cluster) == 0


def test_multiple_simple_nodes():
    """
    Test basic interface with multiple nodes.
    """
    nodes = (SimpleNode(id=1), SimpleNode(id=2))
    cluster = Cluster(nodes)

    assert cluster.location(0) == nodes[-1]
    assert cluster.location(1) == nodes[0]
    assert cluster.location(2) == nodes[1]


def test_simple_node_id_generation():
    """
    Test simple node automatic ID generation.
    """
    cluster = Cluster((SimpleNode(),))
    cluster[0] = 0
    cluster['hello'] = 'world'


def test_rehashing():
    """
    Tests key rehashing for replication.
    """
    count = 3
    rehashed = rehash('example', count)
    assert len(rehashed) == count
    assert len(set(rehashed)) == count


def test_replicated_cluster():
    """
    Test cluster replication.
    """
    replicas = 3
    node = SimpleNode()
    cluster = ReplicatedCluster((node,), replica_count=replicas)
    assert cluster.read_durability == 2
    assert cluster.write_durability == 2

    cluster.read_durability = 1
    assert cluster.read_durability == 1

    try:
        cluster.read_durability = 0
        assert False
    except ValueError:
        assert True

    try:
        cluster.read_durability = 100
        assert False
    except ValueError:
        assert True

    cluster.read_durability = None
    assert cluster.read_durability == quorum(cluster.replica_count)

    cluster.write_durability = 0
    assert cluster.write_durability == 0

    cluster.write_durability = 1
    assert cluster.write_durability == 1

    try:
        cluster.write_durability = 100
        assert False
    except ValueError:
        assert True

    cluster.write_durability = None
    assert cluster.write_durability == quorum(cluster.replica_count)

    cluster['hello'] = 'world'
    assert len(node) == replicas

    del cluster['hello']
    assert len(node) == 0


def test_replicated_conflicts():
    """
    Test conflict resolution with conflicting replicas.
    """
    raise NotImplementedError
