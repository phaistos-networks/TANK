Tank is a high performance distributed log, inspired in part by Kafka, and other similar services and technologies. 

It is a work in progress; it began 4 days ago, and has been moved to github so that I will be motivated to work on it.
It won't compile as-is, for it requires our Switch library, but you can expect a near-future release to include whatever Switch bits are needed to accomplish it.
It compiles and works perfectly find here though.


This is our first major open source release as a company, and we plan to accelerate our O/S efforts in the future.

It will eventually support, among other features:
- clusters via leader/followers arrangement using etcd, similar in semantics to Kafka (but no single controller, and simpler configuration and operation)
- higher level clients, based on Kafka's current client design (depending on the needs of our developers)
- hooks into other Phaistos infrastructure
- a Kafka like streams abstraction
- encryption (wire transfers and bundle serialization)
- improved client and extended API
- HTTP/1 and HTTP/2 REST APIs


Features include:
- Very high performance
- Very tight encoding of messages(into bundles)
- simple design
