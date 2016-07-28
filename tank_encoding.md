## Bundle Encoding Semantics
This document describes the encoding semantics of bundles. 
This encoding is used for both wire-transfers between apps and brokers, and for storing data in segment log files.
It is still a work in progress, albeit we do not foresee any significant changes in the future, except the use of
extra flag bits. Please see later for more.

You may want to consider this schema to build your own consumer clients, or tooling that accesses segment logs directly, or
just to gain a deeper understanding of Tank internals.


### Important Notes
- integers are little-endian encoded
- u8 represents an unsigned 8bit integer, u32 an unsigned 32bit integer, etc in the following encoding description
- varint encoding and decoding implementation is available in Switch/compress.h. This is based on Jeff Dean's varint encoding scheme
	so there are many different implementations for all kinds of languages on GH and elsewhere if you are interested
- the various flag bits are defined in common.h, in TankFlags namespace




### BUNDLE
Messages are always packed into **bundles**, so bundles contain 1+ messages. 
When a client publishes 1+ messages for a specific (topic, partition), even it is a single message, those messages are
all held together in a bundle. 

The client libary will consider the size and messages in the bundle, and may select a compression
codec to compress the bundle messages(called a "message set").  See `TankClient::choose_compression_codec()` for the current implementation.
The more messages you publish, the higher the chance they will 
be compressed (they are always compressed together), and because messages for the same (partition, topic) are usually similar, the compression
codec will often yield a great compression ratio. Furthermore, publishing multiple messages in a single request, will require far fewer network RTs, 
and resources and time to transfer from client to the broker. 

So, a bundle is just a set of messages(1 or more), with a special bundle header, and messages in that message set can be compressed as a whole, or not.
See below for the encoding:


```
bundle
{
	flags:u8		       The bundle flags. Currently, 6 out of 8 bits are used. See below
				       (6, 8]: those 2 bits encode compression codec. 0 is for no compression, 1 is for Snappy compression. Other codecs may be supported in the future
				       (2, 6): those 4 bits encode the total messages in message set, iff total number of messages in the message <= 15. If not, see below
				       (1): unused bit 	(maybe when set means that bundle encodes an absolute seq.num:u64 and msgs and each msg a delta:u32)
				       (0): unused bit	(maybe when set means that each msg or bundle has a crc32 encoded in its header)

	if total messages in message set > 15
	{
		total messages in messages set:varint 	Very often, clients will publish no more than 15 messages/time and encoding that number in
							some bits in flags means we won't need to encode that value individually, and even when we do, because
							we will be encoding it as a varint, it will only take up as many bits as needed.
	}


	We now store message set, one message after the other. The message set as
	a whole may be compressed using the codec encoded in flags.
	See below for encoding of messages. 
	
	msg
	{
		flags:u8 								// The individual message flags

		if ((flags & TankFlags::BundleMsgFlags::UseLastSpecifiedTS) == 0)
		{
			creation ts in milliseconds:u64 				// See later for message timestamp assignment semantics
		}
		else
		{
			use the timestamp for the last message that had a timestamp set in this bundle
		}


		if (flags & TankFlags::BundleMsgFlags::HaveKey) 			// Most messages don't have a key. When they do, this bit is set
		{
			keyLen:u8 							// and we encode the length of the key as a u8 followed by the key in however many bytes long it is
			key:...
		}

		content length:varint 							// The payload(content) length of the message
		data: ... 								// The payload, in however many bytes long it is (sequnece of characters)
	}
}
```


#### Timestamp assignment semantics
Because very often all messages in a bundle have the same creation timestamp, we take that into consideration because it means we can usually save the 8bytes used for
encoding the creation timestamp for the majority of the messages in the bundle. 
We do that by reserving 1 bit from the message flags. If that bit is set, it means
that the message's creation timestamp is the same as the last message in the bundle that had an explictly specified timestamp. If that bit is not set, then
it means the message has an explicitly specified timestamp, and that timestamp is encoded right after the flags. 
That means that in practice we will be encoding the timestamp only for the first message in the bundle, and every message in the bundle which has a different timestamp than the one before it.

#### Messages Sequence Numbers
You will notice that the sequence number for the invidual bundle messages are not encoded in bundle message set.
This is because for the common case where each message's sequence number is set to the last assigned sequence number + 1, we don't need to do that.
We consider the `base sequence number` of the segment log that holds the bundle, as well as the sparse segment index which maps from sequence numbers to segment log file offsets, and from
there we can compute the sequence numbers of all messages indirectly. See tank_protocol.md for more.
By not having to encode that value in the bundle we can safe 4 or 8 bytes we 'd otherwise need to spend to store it.

However, please note, that in the future, we may extend the encoding semantics so that we will either encode a base sequence number for the bundle, or the absolute sequence number for each message, or
a combination of deltas and absolute sequence numbers, in order to support compaction semantics. This will not complicate further the encoding scheme, except that it will likely require the use of one bit
of the bundle and/or messages flags, and the encoding of another value. 
We will ammend this document to reflect those requirements. This will likely not happen soon because there is no immediate need for it, and
from an informal survey, we found out that users of Kafka and other such systems overwhelmingly do not need nor take advantage of compactions. It will be support though eventually.


#### Design Goals
We wanted to come up with a very tight encoding schem in order to minimize both the wire-transfer costs and time(between clients and brokers), and also reduce the storage requirements when persisting bundles to
segment log files. Those savings will become more important and evident as the data set becomes larger, as they will reduce the I/O access costs, transfer codes, processing and transfer costs.
The price to pay for this is a slightly more complicated encoding scheme, but is nonetheless simple.
