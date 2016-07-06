Tank is a high performance distributed log, inspired in part by Kafka, and other similar services and technologies.

You should begin by [reading about the core concepts](https://github.com/phaistos-networks/TANK/wiki/Core-Concepts) and the [client API](https://github.com/phaistos-networks/TANK/wiki/Client-API).

It is a work in progress. The core functionality is implemented and tested, and new features will be implemented soon.  
It depends on our Switch library, so a lean/stripped-down Switch is included in the repo. 
Please see [building instructions](https://github.com/phaistos-networks/TANK/wiki/Building-Tank). You may also want to [run Tank using its Docker image](https://github.com/phaistos-networks/TANK/wiki/Docker).

This is our first major open source release as a company, and we plan to accelerate our OSS release efforts in the future.

It will eventually support, among other features:
- [clusters via leader/followers arrangement](https://github.com/phaistos-networks/TANK/wiki/Operation-Modes) using etcd, similar in semantics to Kafka (but no single controller, and simpler configuration and operation)
- higher level clients, based on Kafka's current client design (depending on the needs of our developers, but PRs will be welcome)
- hooks into other Phaistos infrastructure
- a Kafka/DataFlow like streams topologies abstraction/framework
- encryption (wire transfers and bundle serialization)
- improved client and extended API
- HTTP/1 and HTTP/2 REST APIs
- compactions based on message key (will retain the last published message for each key)

Features include:
- Very high performance
- Very tight encoding of messages(into bundles)
- Simple design (Simple is Beautiful; [Convenience is Key](https://medium.com/@markpapadakis/convenience-is-key-2aad97d531cd#.47eyjv6xt))
 
You should probably use Kafka (the Confluent folk are particularly great ), or Google Pub/Sub, or any other open source broker/queue instead of Tank - they are all perfectly fine, some more than other. 
Tank's goal is highest performance and simplicity. If you need very high performance, operation simplicity and no reliance on other services (when running Tank in standalonemode), consider Tank.

Please see the [wiki](https://github.com/phaistos-networks/TANK/wiki) for more information.

We chose the name Tank because its a storage chamber, suitable for liquids and gas - which we think is analogous to a storage container for data for data that flow, from and to other containers and other systems via 'pipes' (connections).
