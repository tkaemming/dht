from dht.cluster.base import Cluster
from dht.cluster.replication import ReplicatedCluster

# Trick pyflakes into thinking we actually use the above imports
Cluster, ReplicatedCluster
