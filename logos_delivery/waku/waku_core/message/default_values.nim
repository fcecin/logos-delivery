import ../../common/utils/parse_size_units

const
  ## https://rfc.vac.dev/spec/64/#message-size
  DefaultMaxWakuMessageSizeStr* = "150KiB" # Remember that 1 MiB is the PubSub default
  DefaultMaxWakuMessageSize* = parseCorrectMsgSize(DefaultMaxWakuMessageSizeStr)

  DefaultSafetyBufferProtocolOverhead* = 64 * 1024 # overhead measured in bytes

  MaxMessageTimestampSkewPastSeconds* = 300
    ## The time the timestamp of a message may lie behind the clock of a node
    ## that receives it: the clock difference between peers, plus the network
    ## latency, plus the time a sender keeps retrying one message with one
    ## timestamp. The archive, the RLN validator and the receive backfill apply
    ## it. The send service renews a message before it reaches it.
  MaxMessageTimestampSkewFutureSeconds* = 20
    ## The time the timestamp of a message may lie ahead of the clock of a node
    ## that receives it: the clock difference between peers plus the network
    ## latency. No retry adds to it.
