import pytest
import stomp
import time

from connection import ActiveMQNode, ExceptionListener, LogListener, make_connection
from evaluation_plan import (
    AtomicEventType,
    Node,
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
        nodes=[Node.FOUR],
        query=Query(
            operator=Operator.SEQ, operands=[AtomicEventType.J, AtomicEventType.A]
        ),
        inputs=[AtomicEventType.J, AtomicEventType.A],
    )


def test_statement_parser_two():
    statement = "SELECT AND(E, SEQ(J, A)) FROM E, SEQ(J, A) ON {5, 9}"
    parsed = StatementParser(statement).parse()

    assert parsed == Statement(
        nodes=[Node.FIVE, Node.NINE],
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
    """
    This test doesn't seem to work yet, but it's a start.
    I monitored the ActiveMQ broker on the console and I can see the messages
    being sent on the topic, but the ExceptionListener is not being called on
    the subscriber side. I'm not sure why yet...
    """

    conn = stomp.Connection(host_and_ports=[("localhost", 61613)])
    conn.set_listener("", LogListener())
    conn.connect("admin", "admin", wait=True)

    # Set up subscription to query topic
    conn.subscribe(
        destination="/topic/ba1c8dd36be209285e64c7bb1e41d817",  # hash of the query.topic
        id="ba1c8dd36be209285e64c7bb1e41d817_5",
        ack="auto",
    )

    statement = "SELECT AND(E, SEQ(C, J, A)) FROM AND(E, SEQ(J, A)), C ON {5, 9}"

    parser = StatementParser(statement=statement)
    statement = parser.parse()

    for node in statement.nodes:
        node_conn = make_connection()
        amq_node = ActiveMQNode(
            connection=node_conn,
            id_=node.value,
            query_topic=statement.query.topic,
            input_topics=statement.inputs_topics,
        )
        amq_node.send(f"TEST from {amq_node.id}")

        time.sleep(0.1)
        captured = capsys.readouterr()
        assert f"TEST from {amq_node.id}" in captured.out
