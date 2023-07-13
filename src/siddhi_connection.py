import os
import time

from PySiddhi.core.query.output.callback.QueryCallback import QueryCallback
from PySiddhi.core.SiddhiManager import SiddhiManager

from connection import ActiveMQNode, make_connection
from evaluation_plan import Query, StatementParser, make_safe_topic_name

ACTIVEMQ_HOST = os.environ.get("ACTIVEMQ_HOST", "localhost")
ACTIVEMQ_PORT = int(os.environ.get("ACTIVEMQ_PORT", 61613))
STATEMENTS = os.environ.get("STATEMENTS", "")
NODE_ID = os.environ.get("NODE_ID", None)
SLEEP = float(os.environ.get("SLEEP", 20))


class SiddhiQueryOutputCallbackActiveMQ(QueryCallback):
    # Capture outputstream of Siddhi query and forward to ActiveMQ
    def __init__(self, *args, activeMQNode: ActiveMQNode, **kwargs):
        super().__init__(*args, **kwargs)
        self.activeMQNode = activeMQNode

    def receive(self, timestamp, inEvents, outEvents):
        for event in inEvents:
            event_data = event.getData()

            if not event_data:
                print(f"Received empty event from Siddhi query {event}")
                continue

            # event_data is the same as the topic in our case
            event_topic = event_data[0]
            event_topic_mq = make_safe_topic_name(event_topic)
            print(
                f"SiddhiQueryOutputCallbackActiveMQ - sending message {event_topic} back to ActiveMQ (translated to '/topic/{event_topic_mq}')"
            )

            self.activeMQNode.send(event_topic, topic=f"/topic/{event_topic_mq}")


class SiddhiActiveMQNode(ActiveMQNode):
    def __init__(
        self,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.activemq_connection = None  # init siddhi first
        self.siddhi_runtime = None
        self.siddhi_manager = SiddhiManager()

    def start(self):
        self.bootstrap_siddhi()
        self.activemq_connection = make_connection(listener=self)
        self.subscribe_to_topics()

    def stop(self):
        self.unsubscribe_from_topics()
        self.disconnect()
        self.siddhi_manager.shutdown()

    def bootstrap_siddhi(
        self,
        input_stream="cseEventStream",
        output_stream="outputStream",
        attribute="symbol",
    ):
        input_stream_def = f"define stream {input_stream} ({attribute} string); "
        queries_def = " ".join(
            [
                statement.query.to_siddhi_query(
                    input_stream=input_stream,
                    output_stream=output_stream,
                    attribute=attribute,
                )
                for statement in self.statements
            ]
        )

        app_string = f"{input_stream_def} {queries_def}"
        # print(f"Initializing Siddhi runtime with app string: {app_string}")

        self.siddhi_runtime = self.siddhi_manager.createSiddhiAppRuntime(app_string)

        # forward each event from the output stream to the activeMQ message queue
        for topic in [statement.query.topic for statement in self.statements]:
            self.siddhi_runtime.addCallback(
                topic, SiddhiQueryOutputCallbackActiveMQ(activeMQNode=self)
            )

        self.siddhi_runtime.start()

    def on_message(self, message):
        print(f"SiddhiActiveMQNode - Received message {message.body}")
        message_topic = message.body

        if self.siddhi_runtime:
            # siddhi seems to be able to handle the special characters in the topic name
            self.siddhi_runtime.getInputHandler("cseEventStream").send([message_topic])
        else:
            print("Siddhi runtime not initialized, skipping message forwarding...")

    def on_error(self, error):
        print(f"SiddhiActiveMQNode - Received error {error}")


if __name__ == "__main__":
    statements = [
        StatementParser(statement).parse() for statement in STATEMENTS.split("|")
    ]

    print("Waiting for ActiveMQ to start")
    time.sleep(SLEEP)

    siddhi_activeMQ_node = SiddhiActiveMQNode(
        id_=NODE_ID,
        statements=statements,
    )

    siddhi_activeMQ_node.start()
    time.sleep(3600)
