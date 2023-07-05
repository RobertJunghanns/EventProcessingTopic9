import time
from dataclasses import dataclass
from enum import Enum
from multiprocessing import Process, Queue

import stomp

from evaluation_plan import Statement


class LogListener(stomp.ConnectionListener):
    def on_error(self, message):
        print('received an error "%s"' % message)

    def on_message(self, message):
        print('received a message "%s"' % message)


class ExceptionListener(stomp.ConnectionListener):
    def on_error(self, message):
        raise Exception('received an error "%s"' % message)

    def on_message(self, message):
        raise Exception('received a message "%s"' % message)


def make_connection(listener=LogListener()):
    hosts = [("localhost", 61613)]
    conn = stomp.Connection(host_and_ports=hosts)
    conn.set_listener("", listener)
    conn.connect("admin", "admin", wait=True)
    return conn


class CTRLMessageType(Enum):
    START = "start"
    STOP = "stop"
    ADD_STATEMENT = "add_statement"
    REMOVE_STATEMENT = "remove_statement"


@dataclass
class CTRLMessage:
    type: CTRLMessageType
    payload: Statement | None = None


class ActiveMQNode(Process):
    def __init__(
        self,
        id_: str | int,
        queue: Queue,
        connection: stomp.Connection,
        statements: list[Statement] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.id: str | int = id_
        self.queue: Queue = queue
        self.activemq_connection: stomp.Connection = connection
        self.statements: list[Statement] = statements or []
        self.running: bool = True

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

    def subscribe_to_topics(self, ack="auto"):
        for topic in self.topic_subscriptions:
            self.subscribe(topic, ack=ack)

    def advertise_topics(self):
        for topic in self.topic_advertisements:
            self.activemq_connection.send(body="", destination=topic)

    def evaluate_statement(self, statement):
        # Siddhi implementation goes here

        time.sleep(0.001)
        print(f"Node {self.id} is working on statement {statement}...")

    def evaluate_statements(self):
        # we may be able to do something smarter with Siddhi here
        # like registering the queries to be evaluated just once and then
        # leave it to Siddhi to evaluate them in a loop
        for statement in self.statements:
            self.evaluate_statement(statement)

    def get_control_message(self, timeout=0.01):
        return self.queue.get(timeout=timeout)

    def handle_control_message(self, message):
        match message.type:
            case CTRLMessageType.START:
                self.running = False

            case CTRLMessageType.STOP:
                self.running = True

            case CTRLMessageType.ADD_STATEMENT:
                self.add_statement(message.payload)

            case CTRLMessageType.REMOVE_STATEMENT:
                self.remove_statement(message.payload)

            case _:
                raise ValueError(f"Unknown message type: {message.type}")

    def add_statement(self, statement: Statement):
        self.statements.append(statement)

    def remove_statement(self, statement: Statement):
        self.statements.remove(statement)

    def send(self, message, topic):
        self.activemq_connection.send(body=message, destination=topic)

    def disconnect(self):
        self.activemq_connection.disconnect()

    def run(self):
        # Set up subscription to input topics
        self.subscribe_to_topics()
        self.advertise_topics()

        while self.running:
            self.handle_control_message(self.get_control_message())
            self.evaluate_statements()

    def __str__(self):
        return f"Connection(conn={self.activemq_connection}, id='{self.id}', statements='{self.statements}')"
