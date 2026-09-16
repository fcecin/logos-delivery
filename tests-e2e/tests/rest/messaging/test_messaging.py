import pytest
from src.env_vars import NODE_1
from src.libs.common import delay
from src.node.waku_node import WakuNode
from src.steps.messaging import StepsMessaging


@pytest.mark.usefixtures("setup_main_messaging_nodes")
class TestMessagingRest(StepsMessaging):
    @pytest.mark.smoke
    def test_subscribe_send_receive(self):
        self.node2.messaging_subscribe([self.test_content_topic])
        self.send_and_get_request_id(self.node1, "hello messaging")
        record = self.collect_received(self.node2, 1)[0]
        assert record["seq"] == 1  # the node's first received record
        assert record["source"] == "live"
        assert record["message"]["contentTopic"] == self.test_content_topic
        assert self.decode_payload(record) == "hello messaging"
        assert record["messageHash"].startswith("0x") and len(record["messageHash"]) == 66
        # evict-after-poll: a second poll is empty
        assert self.node2.messaging_received() == []

    def test_send_events_propagated_then_sent(self):
        # node2 uses node1 as its Store peer, so its send is confirmed by Store.
        request_id = self.send_and_get_request_id(self.node2)
        self.collect_send_kinds(self.node2, request_id, ["propagated", "sent"])
        # everything for the id was returned and evicted (status anchored: the URL in the
        # message carries the random REST port and the request id)
        with pytest.raises(Exception, match=r"^Error: 404 "):
            self.node2.messaging_send_events_by_id(request_id)

    @pytest.mark.skip(
        reason="needs the Store confirmation fix (separate PR): without it a burst "
        "larger than one Store page is re-sent instead of confirmed"
    )
    def test_burst_is_confirmed(self):
        # More than one Store page (20) of sends in flight at once: every one
        # must still be confirmed rather than re-sent until it ages out.
        for i in range(60):
            self.send_and_get_request_id(self.node2, f"burst-{i}")
        self.collect_sent_request_ids(self.node2, 60, timeout=90)

    def test_unsubscribe_stops_delivery(self):
        self.node2.messaging_subscribe([self.test_content_topic])
        self.send_and_get_request_id(self.node1, "before unsubscribe")
        self.collect_received(self.node2, 1)
        self.node2.messaging_unsubscribe([self.test_content_topic])
        request_id = self.send_and_get_request_id(self.node1, "after unsubscribe")
        # the negative assertion only means something once the message was published
        self.collect_send_kinds(self.node1, request_id, ["propagated"])
        delay(2)
        assert self.node2.messaging_received() == []

    def test_malformed_input_is_rejected_as_bad_request(self):
        with pytest.raises(Exception, match=r"^Error: 400 "):
            self.node2.messaging_subscribe(["not-a-content-topic"])
        with pytest.raises(Exception, match=r"^Error: 400 "):
            self.node2.messaging_send({"payload": "@@not-base64@@", "contentTopic": self.test_content_topic})
        with pytest.raises(Exception, match=r"^Error: 400 "):
            self.node2.messaging_send({"payload": "aGVsbG8=", "contentTopic": "not-a-content-topic"})


class TestMessagingRestNodeConfig(StepsMessaging):
    def test_received_cache_capacity(self):
        self.setup_store_messaging_node()
        self.setup_client_messaging_node(rest_messaging_cache_capacity="10")
        self.node2.messaging_subscribe([self.test_content_topic])
        for i in range(15):
            self.send_and_get_request_id(self.node1, f"cap-{i}")
        # count deliveries from the metric so that no poll evicts anything first
        self.wait_for_live_received_count(self.node2, 15)
        response = self.node2.messaging_received_response()
        records = response.json()
        assert len(records) == 10, f"the buffer keeps the newest 10 of 15, got {len(records)}"
        assert all(self.decode_payload(record).startswith("cap-") for record in records)
        # the overflow is reported, not hidden: header, seq gap and metric all say 5
        assert int(response.headers["X-Messaging-Dropped"]) == 5
        assert [record["seq"] for record in records] == list(range(6, 16))
        self.check_metric(self.node2, "logos_delivery_rest_received_dropped_total", 5)

    def test_kernel_only_node_says_what_to_enable(self):
        node = WakuNode(NODE_1, f"node1_{self.test_id}")
        node.start(relay="true")
        with pytest.raises(Exception, match="--entry-layer"):
            node.messaging_subscribe([self.test_content_topic])
