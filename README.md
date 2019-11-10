Tank is [a very high performance distributed log](https://github.com/phaistos-networks/TANK/wiki/Why-Tank-and-Tank-vs-X), inspired in part by Kafka, and other similar services and technologies.

#### Some Benchmarks
##### Single Producer
```bash
$> bin/kafka-run-class.sh org.apache.kafka.tools.ProducerPerformance  \
 --topic test --num-records 10000000 --record-size 100 \
 --throughput -1 --producer-props acks=1 bootstrap.servers=127.0.0.1:9092 
10000000 records sent, 913158.615652 records/sec (87.09 MB/sec), 2.44 ms avg latency, 167.00 ms max latency, 1 ms 50th, 12 ms 95th, 34 ms 99th, 39 ms 99.9th.
```
```bash
$> tank-cli  -b 127.0.0.1:11011 -t test  bm p2b  -s 100 -c 1000000 -B 8196 -R
Will publish 1,000,000 messages, in batches of 8,196 messages, each message content is 100b (compression disabled)
Go ACK after publishing 1,000,000 message(s) of size 100b (95.37mb), took 0.296s
```

For 1 million messages, without enabling compression(if you do, it will take upto 30% less time for Tank to complete this benchmark), it takes 1 second for Kafka, vs <0.3s for Tank.

##### Multiple Producers (3x)
```
1000000 records sent, 177967.609895 records/sec (16.97 MB/sec), 1029.78 ms avg latency, 2008.00 ms max latency, 989 ms 50th, 1902 ms 95th, 1930 ms 99th, 2007 ms 99.9th.
1000000 records sent, 176118.351532 records/sec (16.80 MB/sec), 1026.09 ms avg latency, 2003.00 ms max latency, 888 ms 50th, 1912 ms 95th, 1990 ms 99th, 2002 ms 99.9th.
1000000 records sent, 173550.850399 records/sec (16.55 MB/sec), 1096.54 ms avg latency, 1953.00 ms max latency, 1023 ms 50th, 1883 ms 95th, 1935 ms 99th, 1952 ms 99.9th.
```

```
Go ACK after publishing 1,000,000 message(s) of size 100b (95.37mb), took 0.413s
Go ACK after publishing 1,000,000 message(s) of size 100b (95.37mb), took 0.474s
Go ACK after publishing 1,000,000 message(s) of size 100b (95.37mb), took 0.519s
```

Produced with
```bash
#!/bin/bash
bin/kafka-run-class.sh org.apache.kafka.tools.ProducerPerformance --topic test --num-records 1000000 --record-size 100 --throughput -1 --producer-props acks=1 bootstrap.servers=127.0.0.1:9092 &
bin/kafka-run-class.sh org.apache.kafka.tools.ProducerPerformance --topic test --num-records 1000000 --record-size 100 --throughput -1 --producer-props acks=1 bootstrap.servers=127.0.0.1:9092 &
bin/kafka-run-class.sh org.apache.kafka.tools.ProducerPerformance --topic test --num-records 1000000 --record-size 100 --throughput -1 --producer-props acks=1 bootstrap.servers=127.0.0.1:9092 &
```
and

```bash
#!/bin/bash
tank-cli  -b 127.0.0.1:11011 -t test  bm p2b  -s 100 -c 1000000 -B 8196 -R &
tank-cli  -b 127.0.0.1:11011 -t test  bm p2b  -s 100 -c 1000000 -B 8196 -R &
tank-cli  -b 127.0.0.1:11011 -t test  bm p2b  -s 100 -c 1000000 -B 8196 -R &
```

For Kafka, we get 177k messages/second/producer. For Tank, we get about 2mil messages/second/producer.


##### Benchmark environment details:
```
Ubuntu 16.04 LTS
Dell Poweredge R630
1x Intel(R) Xeon(R) CPU E5-2620 v3 @ 2.40GHz (6c/12th)
24G ram
Consumer grade SSD (samsung 850 PRO 512GB)
PERC H730 1GB flash cache controller
OpenJDK-8
```

Consumer benchmarks coming soon.



#### Introduction
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
- A [powerful CLI tool](https://github.com/phaistos-networks/TANK/wiki/Tank-CLI) for managing, querying, setting messages, mirroring across brokers, etc.
- [Prometheus Support](https://github.com/phaistos-networks/TANK/wiki/Prometheus-Support)
 
You should probably use Kafka (the Confluent folk are particularly great), or Google Pub/Sub, or any other open source broker/queue instead of Tank - they are all perfectly fine, some more than others, if support for cluster-aware setups is crucial to you(this feature is in the works).

Tank's goal is highest performance and simplicity. If you need very high performance, operation simplicity and no reliance on other services (when running Tank in stand-alone mode), consider Tank.

Please see the [wiki](https://github.com/phaistos-networks/TANK/wiki) for more information.

We chose the name Tank because its a storage chamber, suitable for liquids and gas - which we think is analogous to a storage container for data that flows, from and to other containers and other systems via 'pipes' (connections).
