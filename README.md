Tank is [a very high performance distributed log](https://github.com/phaistos-networks/TANK/wiki/Why-Tank-and-Tank-vs-X), inspired in part by Kafka, and other similar services and technologies.

You should begin by [reading about the core concepts](https://github.com/phaistos-networks/TANK/wiki/Core-Concepts) and the [client API](https://github.com/phaistos-networks/TANK/wiki/Client-API) (A new [Java Tank Client](https://github.com/phaistos-networks/TANK-JavaClient) is now available).

It depends on our Switch library, so a lean/stripped-down Switch is included in the repo. 
Please see [building instructions](https://github.com/phaistos-networks/TANK/wiki/Building-Tank). You may also want to [run Tank using its Docker image](https://github.com/phaistos-networks/TANK/wiki/Docker).

This is [our](http://phaistosnetworks.gr/) first major open source release as a company, and we plan to accelerate our OSS release efforts in the future.

It will eventually support, among other features:
- [clusters via leader/followers arrangement](https://github.com/phaistos-networks/TANK/wiki/Operation-Modes) using etcd, similar in semantics to Kafka (but no single controller, and simpler configuration and operation)
- higher level clients, based on Kafka's current client design (depending on the needs of our developers, but PRs will be welcome)
- hooks into other Phaistos infrastructure
- a Kafka/DataFlow like streams topologies abstraction/framework
- encryption (wire transfers and bundle serialization)
- improved client and extended API
- HTTP/1 and HTTP/2 REST APIs

Features include:
- [Very high performance](https://github.com/phaistos-networks/TANK/wiki/Why-Tank-and-Tank-vs-X)
- [Very tight encoding](https://github.com/phaistos-networks/TANK/blob/master/tank_encoding.md) of messages(into bundles)
- Simple design (Simple is Beautiful; [Convenience is Key](https://medium.com/@markpapadakis/convenience-is-key-2aad97d531cd#.47eyjv6xt))
- [compactions](https://github.com/phaistos-networks/TANK/wiki/Compactions) based on message keys and in the future based on programmable logic
- A [powerful CLI tool](https://github.com/phaistos-networks/TANK/wiki/Tank-CLI) for managing, querying, setting messages, mirroring across brokets, etc.
 
You should probably use Kafka (the Confluent folk are particularly great), or Google Pub/Sub, or any other open source broker/queue instead of Tank - they are all perfectly fine, some more than other, if support for cluster-aware setups is crucial to you(this feature is in the works).

Tank's goal is highest performance and simplicity. If you need very high performance, operation simplicity and no reliance on other services (when running Tank in stand-alone mode), consider Tank.

Please see the [wiki](https://github.com/phaistos-networks/TANK/wiki) for more information.

We chose the name Tank because its a storage chamber, suitable for liquids and gas - which we think is analogous to a storage container for data for data that flow, from and to other containers and other systems via 'pipes' (connections).
