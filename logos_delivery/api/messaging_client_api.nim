import chronos, results
import logos_delivery/api/types

type IMessagingClient* = ref object of RootObj

method subscribe*(
    self: IMessagingClient, contentTopic: ContentTopic
): Future[Result[void, string]] {.async: (raises: []), base.} =
  return err("Interface IMessagingClient.subscribe not implemented")

method unsubscribe*(
    self: IMessagingClient, contentTopic: ContentTopic
): Result[void, string] {.base, raises: [].} =
  return err("Interface IMessagingClient.unsubscribe not implemented")

method send*(
    self: IMessagingClient, envelope: MessageEnvelope
): Future[Result[RequestId, string]] {.async: (raises: []), base.} =
  return err("Interface IMessagingClient.send not implemented")
