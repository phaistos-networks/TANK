## Bundle Encoding Semantics
This document describes the encoding semantics of bundles. 
This encoding is used for both wire-transfers between apps and brokers, and for storing data in segment log files.
The encoding scheme has been crystalized, although one remaining unused bit in the bundle header(see later) may be used in the future
to signify that another flag is present, that in turn may encode other information(encyrption, etc)

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
	flags:u8		       The bundle flags. Currently, 7 out of 8 bits are used. See below
				       (7) 	: extra flags set in the header 			WAS: unused bit	(future:when set, means another flag:u8 is defined in the bunde header, for encryption/CRC etc)
				       (6) 	: SPARSE bundle bit - see later
				       (2, 6] 	: those 4 bits encode the total messages in message set, iff total number of messages in the message <= 15. If not, see below
				       (0, 2] 	: compression codec. 0 for no compression, 1 for Snappy compression. Other codecs may be supported in the future

	if (extra flags)
	{
		extra_flags:u8 		Extra flags. See bits below
			(0) 	: rich producer info available
	}

	if (rich producer info bit set in extra flags)
	{
		partition_leader_eoch:u32 	set by the broker upon receipt by a produce request and is used to
						  ensure no loss of data when there are leader changes /w log trancations. 
		producer_id:u64 		broker assigned producer epoch, received by the 'InitProducerId'
						  clients that wish to support idempotent message delivery and transactiuons must set this field
		producer_epoch:u16  		broker assigned producer epoch received by the InitProducerId request. Clients that wish to support
						  idenmpotent message delivery must set this field
	}


	if (total messages in message set > 15)
	{
		total messages in messages set:varint 	Very often, clients will publish no more than 15 messages/time and encoding that number in
							some bits in flags means we won't need to encode that value individually, and even when we do, because
							we will be encoding it as a varint, it will only take up as many bits as needed.
	}


	if (SPARSE bit is set)
	{
		first message absolute sequence number:u64

		if (total messages in message set > 1)
		{
			// -1 because we want to maximize varint gains
			// we do the same for deltas in the message set
			// so for example, if the first message's sequence number is 10 and the last's is 15, 
			// we 'll encode 15 - 10 - 1 = 4 as varint here
			last messsage in this message set seqNumber - first message seqNum - 1:varint
		}
	}




	We now store message set, one message after the other. The message set as
	a whole may be compressed using the codec encoded in flags.
	See below for encoding of messages. 
	
	msg
	{
		flags:u8 									// The individual message flags

		if (SPARSE bit is set)
		{
			if (flags & TankFlags::BundleMsgFlags::SeqNumPrevPlusOne)
			{
				// message.seqNum = prevMessage.seqNum + 1
			}
			else if (this is neither the first NOR the last message in the message set)
			{
				// So for example, if the previous message in this bundle's message set seq.num was 10
				// and this message's seq num is 11, then we encode (11 - 10 - 1) = 0 here
				(message.seqNum - prevMessage.seqNum - 1):varint
			}
			else
			{
				// first and last message sequence numbers of this message set
				// are encoded in the bundle header
			}
		}

		if ((flags & TankFlags::BundleMsgFlags::UseLastSpecifiedTS) == 0)
		{
			creation ts:u64 					// See later for message timestamp assignment semantics
										// You should use ms, but you may want to use us instead
		}
		else
		{
			// use the timestamp for the last message that had a timestamp set in this bundle
		}


		if (flags & TankFlags::BundleMsgFlags::HaveKey) 				// Most messages don't have a key. When they do, this bit is set
		{
			keyLen:u8 								// and we encode the length of the key as a u8 followed by the key in however many bytes long it is
			key:...
		}

		content length:varint 								// The payload(content) length of the message
		data: ... 									// The payload, in however many bytes long it is (sequnece of characters)
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
You will notice that the sequence number for the invidual bundle messages are not encoded in bundle message set - although there is now support for 'sparse' bundles. See later.
This is because for the common case where each message's sequence number is set to the last assigned sequence number + 1, we don't need to do that.
We consider the `base sequence number` of the segment log that holds the bundle, as well as the sparse segment index which maps from sequence numbers to segment log file offsets, and from
there we can compute the sequence numbers of all messages indirectly. See tank_protocol.md for more.
By not having to encode that value in the bundle we can safe 4 or 8 bytes we 'd otherwise need to spend to store it.

##### Sparse Bundles
We now support 'sparse bundles', where we encode information in the bundle header and the bundle message headers(if necessary) so that messages in the set are explicitly identified
with a sequence number, as opposed to being implicitly identified based on a relative base sequence number(see above).
This is required in order to support compactions and mirroring (a tank-cli feature). Regular applications should never produce sparse bundles (via TankClient::produce_with_base() method)
and sparse bundles are properly handled by the client.
Support for sparse bundles increased the complexity of the encoding and the client, but I think it's worth it, considering one only need to write the client one(and make sure
she's got it right), while reaping the benefits of the tight encoding forever.


#### Design Goals
We wanted to come up with a very tight encoding schem in order to minimize both the wire-transfer costs and time(between clients and brokers), and also reduce the storage requirements when persisting bundles to
segment log files. Those savings will become more important and evident as the data set becomes larger, as they will reduce the I/O access costs, transfer codes, processing and transfer costs.
The price to pay for this is a slightly more complicated encoding scheme, but is nonetheless simple.
