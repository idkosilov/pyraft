import time


def test_message_delivery(message_bridge):
    delivered_messages = []

    def deliver_to_node_callback(message):
        delivered_messages.append(('app', message))

    def deliver_to_app_callback(message):
        delivered_messages.append(('node', message))

    message_bridge.register_deliver_to_node_callback(deliver_to_node_callback)
    message_bridge.register_deliver_to_app_callback(deliver_to_app_callback)
    message_bridge.start()

    message_bridge.handle_message_from_node('Hello, app!')
    message_bridge.handle_message_from_app('Hello, node!')
    time.sleep(0.1)  # Wait for messages to be delivered

    assert delivered_messages == [('node', 'Hello, app!'), ('app', 'Hello, node!')]


def test_message_delivery_exception(message_bridge, caplog):
    def deliver_to_node_callback(message):
        raise ValueError('Unable to deliver message to node')

    message_bridge.register_deliver_to_node_callback(deliver_to_node_callback)
    message_bridge.start()

    message_bridge.handle_message_from_app('Hello, app!')
    time.sleep(0.1)  # Wait for message to be delivered

    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == 'ERROR'
    assert 'Exception occurred on deliver to node:' in caplog.records[0].msg


def test_stop(message_bridge):
    message_bridge.start()
    message_bridge.stop()
    assert not message_bridge.is_running


def test_multiple_stops(message_bridge):
    message_bridge.start()
    message_bridge.stop()
    message_bridge.stop()
    assert not message_bridge.is_running


def test_clear_queue_on_stop(message_bridge):
    message_bridge.handle_message_from_node('Hello, node!')
    message_bridge.handle_message_from_app('Hello, app!')
    message_bridge.start()
    message_bridge.stop()
    assert message_bridge.messages_queue_to_node.empty()
    assert message_bridge.messages_queue_to_app.empty()
