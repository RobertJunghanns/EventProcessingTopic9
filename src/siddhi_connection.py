import os
import time

from PySiddhi.core.query.output.callback.QueryCallback import QueryCallback
from PySiddhi.core.SiddhiManager import SiddhiManager

from connection import ActiveMQNode, make_connection
from evaluation_plan import StatementParser, Query

ACTIVEMQ_HOST = os.environ.get("ACTIVEMQ_HOST", "localhost")
ACTIVEMQ_PORT = os.environ.get("ACTIVEMQ_PORT", 61613)
STATEMENTS = os.environ.get("STATEMENTS", "")
NODE_ID = os.environ.get("NODE_ID", None)


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

            print(
                f"SiddhiQueryOutputCallbackActiveMQ - sending message {event_data[0]} back to ActiveMQ"
            )
            # event_data == topic, for now we only send empty messages
            self.activeMQNode.send(event_data[0], topic=f"/topic/{event_data[0]}")


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
        #print(f"Initializing Siddhi runtime with app string: {app_string}")
        

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
        print('-----------------------------------------')
        print(message_topic)
        print('-----------------------------------------')
        
        if self.siddhi_runtime:
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
    time.sleep(20)

    siddhi_activeMQ_node = SiddhiActiveMQNode(
        id_=NODE_ID,
        statements=statements,
    )

    siddhi_activeMQ_node.start()
    time.sleep(3600)
