from evaluationPlan import QueryParser
from connection import activeMqNode

import stomp

class Listener(stomp.ConnectionListener):
    def on_error(self, headers, message):
        print('received an error "%s"' % message)
    def on_message(self, headers, message):
        print('received a message "%s"' % message)

if __name__ == "__main__":
    """
    Distributed Evaluation Plan
    1. SELECT SEQ(A, F, C) FROM A, F, C ON {0}
    2. SELECT SEQ(J, A) FROM J, A ON {4}
    3. SELECT AND(C, E, D, F) FROM C, E, D, F ON {2, 4}
    4. SELECT AND(C, E, B, D, F) FROM B, AND(C, E, D, F) ON {0, 1, 2, 3, 4, 5}
    5. SELECT AND(E, SEQ(J, A)) FROM E, SEQ(J, A) ON {9}
    6. SELECT AND(E, SEQ(C, J, A)) FROM AND(E, SEQ(J, A)), C ON {5, 9}
    
    hostPort = '8161'

    activeMqNodes = []

    # Create Nodes for first Statement
    parser = QueryParser('SELECT AND(E, SEQ(C, J, A)) FROM AND(E, SEQ(J, A)), C ON {5, 9}');
    parsedDict = parser.parse()
    
    for nodeId in parsedDict['nodes']:
        activeMqNode = activeMqNode(hostPort, nodeId, parsedDict['query'], parsedDict['inputs']) 
        activeMqNodes.append(activeMqNode)

    activeMqNodes[0].send('TEST')
    """ 
    parser = QueryParser('SELECT AND(E, SEQ(C, J, A)) FROM AND(E, SEQ(J, A)), C ON {5, 9}');
    print(parser.parse())

    # THIS FOR SOME REASON THROWS A stomp.exception.ConnectFailedException
    hosts = [('localhost', 61616)] 
    conn = stomp.Connection(host_and_ports=hosts)
    conn.set_listener('', Listener())
    conn.connect('admin', 'admin', wait=True)
    # Register a subscriber with ActiveMQ. This tells ActiveMQ to send
    # all messages received on the topic 'topic-1' to this listener
    conn.subscribe(destination='/topic/topic-1', ack='auto') 
    # Act as a message publisher and send a message the queue queue-1
    conn.disconnect()
