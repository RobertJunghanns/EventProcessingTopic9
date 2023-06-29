import stomp

from connection import ActiveMQNode, LogListener
from evaluation_plan import StatementParser

if __name__ == "__main__":
    """
    Distributed Evaluation Plan

    1. SELECT SEQ(A, F, C) FROM A, F, C ON {0}
    2. SELECT SEQ(J, A) FROM J, A ON {4}
    3. SELECT AND(C, E, D, F) FROM C, E, D, F ON {2, 4}
    4. SELECT AND(C, E, B, D, F) FROM B, AND(C, E, D, F) ON {0, 1, 2, 3, 4, 5}
    5. SELECT AND(E, SEQ(J, A)) FROM E, SEQ(J, A) ON {9}
    6. SELECT AND(E, SEQ(C, J, A)) FROM AND(E, SEQ(J, A)), C ON {5, 9}

    hostPort = '61613' # Robert: 61613

    activeMqNodes = []

    # Create Nodes for first Statement
    parser = QueryParser('SELECT AND(E, SEQ(C, J, A)) FROM AND(E, SEQ(J, A)), C ON {5, 9}');
    parsedDict = parser.parse()

    for nodeId in parsedDict['nodes']:
        activeMqNode = activeMqNode(hostPort, nodeId, parsedDict['query'], parsedDict['inputs'])
        activeMqNodes.append(activeMqNode)

    activeMqNodes[0].send('TEST')
    """

    # TODO Make this work with Ubuntu in Docker!
    #
    # @Robert: THIS WORKS FOR ME AFTER ADJUSTING THE
    # cd /usr/local/Cellar/activemq/<version>/libexec/conf
    # WITH
    # <transportConnector name="stomp" uri="stomp://localhost:61613"/>
    #
    # @Max worked for me out of the box

    def make_connection(listener=LogListener()):
        hosts = [("localhost", 61613)]
        conn = stomp.Connection(host_and_ports=hosts)
        conn.set_listener("", listener)
        conn.connect("admin", "admin", wait=True)
        return conn

    # Register a subscriber with ActiveMQ. This tells ActiveMQ to send
    # all messages received on the topic 'topic-1' to this listener
    # conn.subscribe(destination="/topic/topic-1", id="test", ack="auto")
    #
    # Act as a message publisher and send a message the queue queue-1
    # conn.disconnect()

    statement = "SELECT AND(E, SEQ(C, J, A)) FROM AND(E, SEQ(J, A)), C ON {5, 9}"

    parser = StatementParser(statement=statement)
    statement = parser.parse()

    amq_nodes = []

    for node in statement.nodes:
        amq_node = ActiveMQNode(
            connection=make_connection(),
            id_=node.value,
            query_topic=statement.query.topic,
            input_topics=statement.inputs_topics,
        )
        amq_node.send(f"TEST from {amq_node.id}")
