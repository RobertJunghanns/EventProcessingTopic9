import time
from dataclasses import dataclass, field
from enum import Enum
from multiprocessing import Process, Queue
from queue import Empty
from typing import Callable, Optional

import stomp

from evaluation_plan import NodeEnum, Statement

# from PySiddhi.core.SiddhiManager import SiddhiManager
# from PySiddhi.core.query.output.callback.QueryCallback import QueryCallback
# from PySiddhi.core.util.EventPrinter import PrintEvent


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
        id_: int,
        queue: Queue = Queue(),
        statements: Optional[list[Statement]] = None,
        **kwargs,
    ):
        self.id: int = int(id_)
        self.queue: Queue = queue
        self.activemq_connection: stomp.Connection = None  # don't initialize here!
        self.statements: list[Statement] = statements or []
        self.running: bool = True

        super().__init__(**kwargs)

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

    def createActiveMqConnection(self):
        hosts = [("localhost", 61613)]
        self.activemq_connection = stomp.Connection(host_and_ports=hosts)
        # self.activemq_connection.set_listener("", PySiddhiListener(self))
        self.activemq_connection.connect("admin", "admin", wait=True)

    def unsubscribe(self, topic):
        subscription_id = f"sub-{self.id}-{topic}"
        # self.activemq_connection.unsubscribe(id=subscription_id)

    def subscribe_to_topics(self, ack="auto"):
        for topic in self.topic_subscriptions:
            self.subscribe(topic, ack=ack)

    def advertise(self, topic):
        self.activemq_connection.send(body="", destination=topic)

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
        try:
            return self.queue.get(timeout=timeout)
        except Empty:
            return None

    def handle_control_message(self, message):
        match message.type:
            case CTRLMessageType.START:
                self.running = True

            case CTRLMessageType.STOP:
                self.running = False
                self.close()

            case CTRLMessageType.ADD_STATEMENT:
                self.add_statement(message.payload)

            case CTRLMessageType.REMOVE_STATEMENT:
                self.remove_statement(message.payload)

            case _:
                raise ValueError(f"Unknown message type: {message.type}")

    def add_statement(self, statement: Statement):
        self.statements.append(statement)

        for topic in statement.input_topics:
            self.subscribe(topic)

        self.advertise(statement.query.topic)

        # if self.activemq_connection is not None:
        #     self.activemq_connection.set_listener("", PySiddhiListener(self))

    def remove_statement(self, statement: Statement):
        self.statements.remove(statement)

        for topic in statement.input_topics:
            self.unsubscribe(topic)

    def send(self, message, topic):
        self.activemq_connection.send(body=message, destination=topic)

    def disconnect(self):
        self.activemq_connection.disconnect()

    def run(self):
        # Set up connection to ActiveMQ,
        # we can not do this in __init__ because the connection is not serializable
        # self.activemq_connection = self.activemq_connection_factory()
        self.createActiveMqConnection()

        # Set up subscription to input topics
        self.subscribe_to_topics()
        self.advertise_topics()  # isn't this already interpreted as an acutal sending of an composite event?

        while self.running:
            if control_message := self.get_control_message(timeout=0.01):
                self.handle_control_message(control_message)
            self.evaluate_statements()

    def __str__(self):
        return f"Connection(id='{self.id}', activemq_connection={self.activemq_connection}, statements='{self.statements}')"

    def __eq__(self, other) -> bool:
        if not isinstance(other, ActiveMQNode):
            return False

        return self.id == other.id

    def __hash__(self) -> int:
        return self.id


@dataclass
class NodeManager:
    _nodes: dict[int, ActiveMQNode] = field(default_factory=dict)

    @property
    def nodes(self):
        return list(self._nodes.values())

    def add_node(self, node: ActiveMQNode):
        self._nodes[node.id] = node

    def remove_node(self, node: ActiveMQNode):
        node.queue.put(CTRLMessage(type=CTRLMessageType.STOP))
        self._nodes.pop(node.id, None)

    def get_node_by_id(self, node_id: int | NodeEnum):
        if isinstance(node_id, NodeEnum):
            node_id = node_id.value

        return self._nodes.get(node_id, None)

    def start_node(self, node: int | NodeEnum | ActiveMQNode):
        if isinstance(node, NodeEnum):
            node = ActiveMQNode(id_=node.value)

        if isinstance(node, int):
            node = ActiveMQNode(id_=node)

        self.add_node(node)
        node.start()

    def stop_node(
        self,
        node_id: int | NodeEnum | None = None,
        node: ActiveMQNode | None = None,
    ):
        assert (
            node_id is not None or node is not None
        ), "Either node_id or node must be provided"

        if node_id is not None:
            node = self.get_node_by_id(node_id)

        if node is None:
            raise ValueError(f"Node with id {node_id} not found")

        node.queue.put(CTRLMessage(type=CTRLMessageType.STOP))
        node.join()

    def send_statement_to_node(
        self,
        statement: Statement,
        node_id: int | NodeEnum | None = None,
        node: ActiveMQNode | None = None,
    ):
        assert (
            node_id is not None or node is not None
        ), "Either node_id or node must be provided"

        if node_id is not None:
            node = self.get_node_by_id(node_id)

        if node is None:
            raise ValueError(f"Node with id {node_id} not found")

        node.queue.put(
            CTRLMessage(
                type=CTRLMessageType.ADD_STATEMENT,
                payload=statement,
            )
        )


# ADAPT THIS CALLBACK TO SEND COMPOSITE EVENTS TO ACTIVE MQ
class QueryCallbackImpl:
    def receive(self, timestamp, inEvents, outEvents):
        # PrintEvent(timestamp, inEvents, outEvents)
        # Send to specific topic
        print(f"PySiddhi received {( timestamp, inEvents, outEvents)}")
        # self.activemq_node.send()


# export JAVA_HOME='/Users/robertjunghanns/Library/Java/JavaVirtualMachines/openjdk-15.0.1/Contents/Home'
# export SIDDHISDK_HOME='/Users/robertjunghanns/Library/Siddhi/siddhi-sdk-5.1.2'
class PySiddhiListener(stomp.ConnectionListener):
    # def __init__(self):
    # self.activemq_node = activemq_node
    # self.siddhiManager = SiddhiManager()

    # self.siddhiApp = (
    #     "define stream eventStream (eventType string);"
    #     + "@info(name = 'query1') from eventStream select eventType insert into outputStream;"
    # )

    # # Add queries on eventStrem for each statement of node
    # for i, statement in enumerate(activemq_node.statements):
    #     # HERE Parse statements from activemq_node to siddhi queries
    #     self.siddhiApp += ""

    # # Generate the Siddhi runtime
    # self.siddhiManager.createSiddhiAppRuntime(self.siddhiApp)

    # for i, statement in enumerate(activemq_node.statements):
    #     # Add the callback implementation to the runtime
    #     self.siddhiAppRuntime.addCallback("query"+str(i), QueryCallbackImpl(activemq_node))

    # # Retrieve the input handler to push events into Siddhi
    # self.inputHandler = self.siddhiAppRuntime.getInputHandler("eventStream")

    # # Start event processing
    # self.siddhiAppRuntime.start()

    def on_error(self, message):
        raise Exception('received an error "%s"' % message)

    def on_message(self, message):
        print(f"PYSIDDHI - Received message {message}")
        return
        # self.inputHandler.send([message])
