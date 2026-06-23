import chronos, results
import logos_delivery/api/types
import logos_delivery/channels/types
import logos_delivery/channels/reliable_channel

type IReliableChannelManager* = ref object of RootObj

method createReliableChannel*(
    self: IReliableChannelManager,
    channelId: ChannelId,
    contentTopic: ContentTopic,
    senderId: SdsParticipantID,
    sendHandler: SendHandler = nil,
): Result[ChannelId, string] {.base.} =
  return err("Interface IReliableChannelManager.createReliableChannel not implemented")

method closeChannel*(
    self: IReliableChannelManager, channelId: ChannelId
): Future[Result[void, string]] {.async: (raises: []), base.} =
  return err("Interface IReliableChannelManager.closeChannel not implemented")

method send*(
    self: IReliableChannelManager,
    channelId: ChannelId,
    appPayload: seq[byte],
    ephemeral: bool = false,
): Future[Result[RequestId, string]] {.async: (raises: []), base.} =
  return err("Interface IReliableChannelManager.send not implemented")
