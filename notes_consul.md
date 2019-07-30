- on startup, will acquire a session, with a TTL that we will periodically refresh
- will enable listener only after we have initialized (if in cluster mode)



## Leadership
The leader will be responsible for assigning partitions to nodes. Once a node acquires leadership, it will need to check the topology for required assignments. That also includes doing it on startup

# Misc
- Create the new session only iff there is not one created earlier -- stored in a file locally, or if when attempting to renew it (on startup) it fails because it no longer exists
- https://kafka.apache.org/documentation/#semantics
	- Under non-failure conditions, each partition has 1 leader and 0+ followers



