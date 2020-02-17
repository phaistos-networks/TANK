# IMPORTANT
This is slightly out of date. Revised documentation should become available soon. What's here here is still correct, but new information
is missing.



This document describes the encoding and semantics of requests and responds exchanged between clients and brokers. 

### Important Notes
- integers are little-endian encoded
- u8 represents an unsigned 8bit integer, u32 an unsigned 32bit integer, etc in the following encoding description
- varint encoding and decoding implementation is available in Switch/compress.h. This is based on Jeff Dean's varint encoding scheme
	so there are many different implementations for all kinds of languages on GH and elsewhere if you are interested
- the various flag bits are defined in common.h, in TankFlags namespace
- str8 represents a { length:u8, string:... }. A string of length encoded in 1 byte followed by the string characters. A st32 represents a { length:u32, string, ...} 




### BASICS
All requests and responses begin with

```
{
	msgId:u8
	payload size:u32
}
```

where msgId the type of request or response, and payload size is the size of the content for the request/response.
The payload is described below, for each different request or response supported.
See common.h for the message IDs of all support requests/responses.



### FetchReq
msgId is `0x2`

```
{
	client version:u16 			A client version, for versioning. Use 0 for current version
	request id:u32 				Every request is assigned a request id, and used by the initiator for tracking. The broker will always return that request in the response
	client id:str8 				This is used for debugging and tracing. You may omit it or set it to some dummy value.
	max wait(ms):u64 			Please see below for wait and min bytes semantics
	min bytes:u32 				Please see below for wait and min bytes semantics
	topics count:u8 			How many distinct topics are requested

		topic
		{
			name:str8 			The name of the topic
			partitions count:u8 		Number of distinct partitions we are requesting data for

			partition 		
			{
				partition id:u16 		The partition ID
				abs. sequence number: u64 	The absolute sequence number of the first message in this (topic, partition) we are interested in
									See Wait and MinBytes semantics for more.
				fetch size:u32 			Please see below for fetch size semantics

		} ..
}
```

#### Wait and MinBytes semantics
When we request data for a (topic, partition), we provide the absolute sequence number of the first message we are interested in.
That value has two reserved special-purprose possible values.  
`0` : This asks the broker to begin from the first message available in the topic, regardless of its sequence number.  
Because of various retention and (in the future, compaction) semantics, older messages may be dropped(configurable), so
by asking for 0, you always get to begin from the very first available message, without having to know what its message id is.  
`18446744073709551615`: This is the maximum value an unsigned integer can represent, and that meeans this is the highest possible message sequence number (it's almost impossible you will get to that value). When this sequence number is requested, the broker will translate that value  to
the `last assigned sequence number + 1`. 

When the absolute sequence number requested is equal to `last assigned sequence number + 1`, the broker will wait until new messages are published from the moment the request has been received and will send that data back to the client. It will wait for `max wait` time(see above) until any new messages are published. If no messages are published within that time, a response is returned but holds no bundles in the chunk.
Furthermore, if "min bytes" is specified, the broker will not respond immediately after the first bundle is published. Instead, it will only respond if the total bytes of all bundles published since the request  >= min bytes.  
The client may want to deal with lots of data at a time instead, and cares more for throughput than latency. Those two options are useful for tuning the behavior of 'tail' semantics to that end.


#### FetchSize semantics
For each new request for (topic, partition) data, the client specifies a `fetch size` value, which is the largest amount of data the broker should send in chunks (i.e encoded bundles) from that partition. Because of indexing, encoding and storage semantics, and for performance and simplicity, the broker does not adjust the fetch size to only return full bundles. That is to say, if fetch size is 80, and two bundles are to be returned, one of length 40, and another 100, then the first bundle in the response chunk will be full, while the second will be partial (last 90 - 80 bytes(10) bytes will be missing).  Please refer to `TankClient::process_consume()` implementation for how this works in practice.
The client should account for that. This is also how Kafka works.



#### FetchResp
msgReq is `0x2`  

```
{
	header length:u32 		The length of the header that follows

	header 			    	The header encodes information for all requested (topic, partition)s. 
	{
		request id:u32 		When clients issue requests, they specify a request id for them. 
		                    	The broker encodes that here so that the client will know what this is for
		topics count:u8 	How many topics are encoded below, based on the original request

		topic 	
		{
			name:str8 			Name of the topic
			total partitions:u8 		Total partitions encoded for this topic


			{
				if a u16 follows == UINT16_MAX, then this topic wasn't found and
				no partitions are encoded in this response for this topic
			}

			partition
			{
				partitionid:u16 		Partition ID
				errorOrFlags:u8 		Error or flags(0 means no error or any special flags are set)

				if (errorOrFlags == 0xff)
				{
 					//the specified partition is unknown
					//and following fields are not encoded in this response/topic/partition
				}

				if (errorOrFlags != 0xfe)
				{
					base absolute sequence number of the first message in the first bundle returned:u64
				}
				else
				{
					//The first bundle has the SPARSE bit set so
					//instead of storing the abs.seqNum of the first bundle's message here and in the bundle header
					//we will only deserialize from the bundle header in order to save 8 bytes
				}

				high water mark:u64 		absolute sequence number of the latest committed message for this (topic, partition):u64
				chunk length:u32 		See later for chunk encoding semantics


				{
					if (errorOrFlags == 0x1) 
					{
						//this is a boundary check failure, and  chunk length and base abs.seqn number encoded earlier as 0
						firstAvailSeqNum:u64 is serialized here
					}
				}

			} ..
		} ..
	}

	Right past all headers, we encode all the chunks, one chunk at a time, for every (topic, response) that
	we have data for (specified in the respective header).
	Each chunk just holds a sequence of bundles, but for semantics reasons, the last bundled included in the chunk may be partial, so the
	consumer implementation needs to account for that. Please see FetchReq description for more. 
	I highly recommend you check TankClient::process_consume() for how in practice. See "FetchSize semantics" for more information.

	chunk
	{
		This chunk length is specified in the respective header as `chunk length`. So we store however many bundles
		possible in this chunk of that length. Again, please note that the last encoded bundle may be partial. See "FetchSize semantics"

		bundle
		{
			length:varint 	The lenght of the bundle
			bundle:... 	See tank_encoding.md for bundle encoding scheme
		}...
	}...
}
```






### Publish Req   
msgId `0x1`  or `0x5`


```
{
	client version:u16
	request id:u32
	client id:str8
	required acks:u8				This will be considered in clustered mode setups. For standalone setup, this is ignored.
	ack. timeout:u32 				This will be considered in clustered mode setups. For standalone setup, this is ignored.
	topics cnt:u8			 		How many distinct topics to publish to

		topic
		{
			topic name:str8 		Name of the topic
			partitions cnt:u8		Total distinct partitions from this topic to publish to

			partition 					
			{
				partition id:u16 		
				bundle length:varint  	The length of the bundle

				if (msg == TankAPIMsgType::ProduceWithBaseSeqNum)
				{
					// this is for specific tools and broker interchange
					// clients should never issue this request
					baseSeqNum:u64
				}

				bundle:... 				See "tank_encoding.md" for bundle semantics
			} ..

		} ..
}
```


### Publish Resp
msgId `0x1`  

You are expected to kep-track/remember the original publish request's list of (topic, partition), so that when you get back a publish response  
you can lookup that list. 
The publish response contains essentially an error code for every partition in that request list (of type u8), in order, except when the
topic requested is unknown, in which case, the first parition for that topic's error code is 0xff, and the remaining matching error codes for
that requested topic are not included in the response.

Errors:
- 0x0: No Error
- 0xff: topic unknown
- 0x02: invalid request
- any other: system error

```
{
	request id:u32
	For each topic specified in the matching publish request:
		{
			error:u8 	// error for the first partition for this topic, as specified in the orginal req.

			if (error == 0xff)
			{
				// Unknown topic
				//
				// The error code for the first partition for this requested topic == 0xff, so
				// we should not expect any other partition error codes for the remaining partitions of this
				// requested topic.
			}
			else
			{
				// otherwise error is the error for the first partition of the topic specified in the matching publish request
				// and we will also consider the errors for the remaining partitions of this topic
				for (int i{1}; i != reqPartitionsCont; ++i)
				{
					error for the next partition in topic for this request:u8
				}
			}
		}

}
```
Note that the client should store the sequence of (topic, partions...) for the publish request, so that processing the publish response can use that information. This is so that we wouldn't have to encode (topic, partition) again in the publish response.



### ReplicaIDReq
msgId `0x4`

```
{
	replicaId:u16
}
```

Immediately upon connecting, the first request from a replica(follower) should be a replica ID req, where the replica ID is advertised to the broker. This way, the broker will know that the connection originated from a known broker in a cluster. This is currently not used, because clustered mode support has not been implemented yet.




### Ping
msgId `0x3`  

This message has no payload. The broker is expected to immediately ping any client or broker that connects to it, and periodically do so as a hearbeat. The client should consider the connection to a broker successful only as soon as it has received a ping from the broker.
