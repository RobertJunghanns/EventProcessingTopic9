import time
from multiprocessing import Queue

import pytest
import stomp

from connection import (
    ActiveMQNode,
    CTRLMessage,
    CTRLMessageType,
    LogListener,
    NodeManager,
    make_connection,
)
from evaluation_plan import (
    AtomicEventType,
    NodeEnum,
    Operator,
    Query,
    Statement,
    StatementParser,
)


@pytest.mark.parametrize(
    "text, expected",
    [
        ("AND", Operator.AND),
        ("SEQ", Operator.SEQ),
        ("AND(A, B)", Operator.AND),
        ("SEQ(A, B)", Operator.SEQ),
    ],
)
def test_operator_from_string(text, expected):
    assert Operator.from_string(text) == expected


def test_operator_from_string_raises():
    with pytest.raises(ValueError):
        Operator.from_string("OR")


@pytest.mark.parametrize(
    "text, expected",
    [
        ("A", AtomicEventType("A")),
        ("B", AtomicEventType("B")),
        ("C", AtomicEventType("C")),
        ("D", AtomicEventType("D")),
        ("E", AtomicEventType("E")),
        ("F", AtomicEventType("F")),
        ("J", AtomicEventType("J")),
    ],
)
def test_atomic_from_string(text, expected):
    assert AtomicEventType.from_string(text) == expected


def test_atomic_from_string_raises():
    with pytest.raises(ValueError):
        AtomicEventType.from_string("Z")


def test_query_from_string():
    text = "AND(A, B, C)"
    query = Query.from_string(text)

    assert query.operator == Operator.AND
    assert query.operands == [
        AtomicEventType("A"),
        AtomicEventType("B"),
        AtomicEventType("C"),
    ]


def test_nested_query_from_string():
    text = "SEQ(A, AND(B, C))"
    query = Query.from_string(text)

    assert query.operator == Operator.SEQ
    assert query.operands == [
        AtomicEventType("A"),
        Query(Operator.AND, [AtomicEventType("B"), AtomicEventType("C")]),
    ]


def test_nested_query_from_string_two():
    text = "SEQ(A, AND(B, C), D, AND(E, F, SEQ(A, B, C)), J)"
    query = Query.from_string(text)

    assert query.operator == Operator.SEQ
    assert query.operands == [
        AtomicEventType("A"),
        Query(Operator.AND, [AtomicEventType("B"), AtomicEventType("C")]),
        AtomicEventType("D"),
        Query(
            Operator.AND,
            [
                AtomicEventType("E"),
                AtomicEventType("F"),
                Query(
                    Operator.SEQ,
                    [AtomicEventType("A"), AtomicEventType("B"), AtomicEventType("C")],
                ),
            ],
        ),
        AtomicEventType("J"),
    ]


def test_statement_parser():
    statement = "SELECT SEQ(J, A) FROM J, A ON {4}"
    parsed = StatementParser(statement).parse()

    assert parsed == Statement(
        nodes=[NodeEnum.FOUR],
        query=Query(
            operator=Operator.SEQ, operands=[AtomicEventType.J, AtomicEventType.A]
        ),
        inputs=[AtomicEventType.J, AtomicEventType.A],
    )


def test_statement_parser_two():
    statement = "SELECT AND(E, SEQ(J, A)) FROM E, SEQ(J, A) ON {5, 9}"
    parsed = StatementParser(statement).parse()

    assert parsed == Statement(
        nodes=[NodeEnum.FIVE, NodeEnum.NINE],
        query=Query(
            operator=Operator.AND,
            operands=[
                AtomicEventType.E,
                Query(
                    operator=Operator.SEQ,
                    operands=[AtomicEventType.J, AtomicEventType.A],
                ),
            ],
        ),
        inputs=[
            AtomicEventType.E,
            Query(
                operator=Operator.SEQ, operands=[AtomicEventType.J, AtomicEventType.A]
            ),
        ],
    )


def test_connection():
    # smoke test
    conn = stomp.Connection(host_and_ports=[("localhost", 61613)])
    conn.set_listener("", LogListener())

    conn.connect("admin", "admin", wait=True)
    conn.disconnect()


def test_activemq_with_statement(capsys):
    statement = "SELECT AND(E, SEQ(C, J, A)) FROM AND(E, SEQ(J, A)), C ON {5, 9}"
    parser = StatementParser(statement=statement)
    statement = parser.parse()

    topic = f"/topic/{statement.query.topic}"  # currently this is a hash

    # Set up subscription to query topic
    conn = make_connection()
    conn.subscribe(
        destination=topic,
        id=f"test-sub-{statement.query.topic}",
        ack="auto",
    )

    for node in statement.nodes:
        queue = Queue()
        node = ActiveMQNode(id_=node.value, queue=queue, statements=[statement])

        # Do not start Node as Process, just check if sending a message works
        node.activemq_connection = make_connection()
        node.send(f"Hello from Node {node.id}", topic=topic)

    time.sleep(0.05)
    captured = capsys.readouterr().out
    assert "Hello from Node 5" in captured
    assert "Hello from Node 9" in captured


def test_nodes_as_processes():
    """
    Run this test with 'pytest -vv -s' to see the output from the process
    """
    statement = "SELECT AND(E, SEQ(C, J, A)) FROM AND(E, SEQ(J, A)), C ON {5}"
    parser = StatementParser(statement=statement)
    statement = parser.parse()

    queue = Queue()
    node_5 = ActiveMQNode(id_=5, queue=queue, statements=[statement])

    # Start node as Process
    node_5.start()

    # From now on, we can only communicate with the node process via the queue
    node_5.queue.put(CTRLMessage(CTRLMessageType.STOP))

    # Wait for node process to finish
    node_5.join()


def test_nodemanager():
    """
    Run this test with 'pytest -vv -s' to see the output from the processes
    """
    manager = NodeManager()

    # we can address nodes either by their integer id or by the NodeEnum class
    manager.start_node(NodeEnum.ONE)
    manager.start_node(2)

    statement_one = Statement(
        [NodeEnum.ONE],
        Query(Operator.AND, [AtomicEventType.A, AtomicEventType.B]),
        inputs=[AtomicEventType.A, AtomicEventType.B],
    )

    statement_two = Statement(
        [NodeEnum.TWO],
        Query(Operator.AND, [AtomicEventType.C, AtomicEventType.D]),
        inputs=[AtomicEventType.C, AtomicEventType.D],
    )

    # we can address nodes either by their integer id or by the NodeEnum class
    manager.send_statement_to_node(node_id=1, statement=statement_one)
    manager.send_statement_to_node(node_id=NodeEnum.TWO, statement=statement_two)

    manager.stop_node(1)
    manager.stop_node(2)
