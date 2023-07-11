import os
from typing import Any, List, Optional

import stomp

from evaluation_plan import Statement

ACTIVEMQ_HOST = os.environ.get("ACTIVEMQ_HOST", "localhost")
ACTIVEMQ_PORT = os.environ.get("ACTIVEMQ_PORT", 61613)
STATEMENTS = os.environ.get("STATEMENTS", [])


class LogListenerActiveMQ(stomp.ConnectionListener):
    def on_error(self, message):
        print('LogListener received an error "%s"' % message)

    def on_message(self, message):
        print('LogListener received a message "%s"' % message)


def make_connection(listener: Any = LogListenerActiveMQ()):
    hosts = [(ACTIVEMQ_HOST, ACTIVEMQ_PORT)]
    conn = stomp.Connection(host_and_ports=hosts)
    conn.set_listener("", listener)
    conn.connect("admin", "admin", wait=True)
    return conn


class ActiveMQNode(stomp.ConnectionListener):
    def __init__(
        self,
        id_: int,
        statements: Optional[List[Statement]] = None,
    ):
        super().__init__()

        self.id: int = int(id_)
        self.activemq_connection: stomp.Connection = make_connection(listener=self)
        self.statements: list[Statement] = statements or []

    def start(self):
        self.subscribe_to_topics()

    def stop(self):
        self.unsubscribe_from_topics()
        self.disconnect()

    def on_message(self, message):
        print(f"ActiveMQNodeListener - Received message {message}")

    def on_error(self, error):
        print(f"ActiveMQNodeListener - Received error {error}")

    @property
    def topic_subscriptions(self):
        return [
            f"/topic/{topic}"
            for statement in self.statements
            for topic in statement.input_topics
        ]

    @property
    def topic_advertisements(self):
        return [f"/topic/{statement.query.topic}" for statement in self.statements]

    def subscribe(self, topic, ack="auto"):
        subscription_id = f"sub-{self.id}-{topic}"
        self.activemq_connection.subscribe(
            destination=topic, id=subscription_id, ack=ack
        )

    def unsubscribe(self, topic):
        subscription_id = f"sub-{self.id}-{topic}"
        self.activemq_connection.unsubscribe(id=subscription_id)

    def subscribe_to_topics(self, ack="auto"):
        for topic in self.topic_subscriptions:
            self.subscribe(topic, ack=ack)

    def unsubscribe_from_topics(self):
        for topic in self.topic_subscriptions:
            self.unsubscribe(topic)

    def send(self, message, topic):
        self.activemq_connection.send(body=message, destination=topic)

    def disconnect(self):
        self.activemq_connection.disconnect()

    def __str__(self):
        return f"Connection(id='{self.id}', activemq_connection={self.activemq_connection}, statements='{self.statements}')"

    def __eq__(self, other) -> bool:
        if not isinstance(other, ActiveMQNode):
            return False

        return self.id == other.id

    def __hash__(self) -> int:
        return self.id
