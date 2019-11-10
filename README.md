TANK is [a very high performance distributed log](https://github.com/phaistos-networks/TANK/wiki/Why-Tank-and-Tank-vs-X), inspired in part by Kafka, and other similar services and technologies. This is the second major public release, **TANK 2**. Read about this new major public release [here](https://medium.com/@markpapadakis/tank-2-initial-public-release-b6d88edab07f).


#### Introduction
You should begin by [reading about the core concepts](https://github.com/phaistos-networks/TANK/wiki/Core-Concepts) and the [client API](https://github.com/phaistos-networks/TANK/wiki/Client-API) (A new [Java Tank Client](https://github.com/phaistos-networks/TANK-JavaClient) is now available).

Please see [building instructions](https://github.com/phaistos-networks/TANK/wiki/Building-Tank). You may also want to [run Tank using its Docker image](https://github.com/phaistos-networks/TANK/wiki/Docker).


Features include:
- [Very high performance](https://github.com/phaistos-networks/TANK/wiki/Why-Tank-and-Tank-vs-X)
- [Very tight encoding](https://github.com/phaistos-networks/TANK/blob/master/tank_encoding.md) of messages(into bundles)
- Simple design (Simple is Beautiful; [Convenience is Key](https://medium.com/@markpapadakis/convenience-is-key-2aad97d531cd#.47eyjv6xt))
- [compactions](https://github.com/phaistos-networks/TANK/wiki/Compactions) based on message keys and in the future based on programmable logic
- A [powerful CLI tool](https://github.com/phaistos-networks/TANK/wiki/Tank-CLI) for managing, querying, setting messages, mirroring across brokers, etc.
- [Prometheus Support](https://github.com/phaistos-networks/TANK/wiki/Prometheus-Support)
 
TANK's goal is highest performance and simplicity. 
If you need very high performance, operation simplicity and no reliance on other services (when running TANK in stand-alone mode), consider TANK.

Please see the [wiki](https://github.com/phaistos-networks/TANK/wiki) for more information.

We chose the name TANK because its a storage chamber, suitable for liquids and gas - which we think is analogous to a storage container for data that flows, from and to other containers and other systems via 'pipes' (connections).
