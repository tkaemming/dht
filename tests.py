import sys

from dht.cluster import Cluster
from dht.node import SimpleNode


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
