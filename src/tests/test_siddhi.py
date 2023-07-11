from time import sleep

from PySiddhi.core.query.output.callback.QueryCallback import QueryCallback
from PySiddhi.core.SiddhiManager import SiddhiManager
from PySiddhi.core.util.EventPrinter import PrintEvent
from PySiddhi.DataTypes.LongType import LongType

from evaluation_plan import StatementParser
from siddhi_connection import SiddhiActiveMQNode


# Add listener to capture output events
class QueryCallbackImpl(QueryCallback):
    def receive(self, timestamp, inEvents, outEvents):
        PrintEvent(timestamp, inEvents, outEvents)


def test_siddhi():
    print("test_siddhi Manager")
    siddhiManager = SiddhiManager()

    # Siddhi Query to filter events with volume less than 150 as output
    siddhiApp = (
        "define stream cseEventStream (symbol string, price float, volume long); "
        "@info(name = 'query1') from cseEventStream[volume < 150] select symbol,price insert into outputStream;"
    )

    # Generate runtime
    siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp)

    # Add callback implementation to capture output events
    siddhiAppRuntime.addCallback("query1", QueryCallbackImpl())

    # Retrieving input handler to push events into Siddhi
    inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream")

    # Starting event processing
    siddhiAppRuntime.start()

    # Sending events to Siddhi
    inputHandler.send(["IBM", 700.0, LongType(100)])
    inputHandler.send(["WSO2", 60.5, LongType(200)])
    inputHandler.send(["GOOG", 50, LongType(30)])
    inputHandler.send(["IBM", 76.6, LongType(400)])
    inputHandler.send(["WSO2", 45.6, LongType(50)])

    # Wait for response
    sleep(1)

    siddhiManager.shutdown()


def test_node_siddhi():
    """
    Run this test with 'pytest -vv -s' to see the output from the process
    """

    statement = "SELECT AND(E, SEQ(C, J, A)) FROM AND(E, SEQ(J, A)), C ON {5}"
    parser = StatementParser(statement=statement)
    statement = parser.parse()

    node_5 = SiddhiActiveMQNode(
        id_=99,
        statements=[statement],
    )

    node_5.start()
