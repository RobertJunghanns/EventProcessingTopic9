import pytest

from evaluation_plan import AtomicEventType, Operator, Query, StatementParser


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

    assert parsed == {
        "nodes": [4],
        "query": Query(
            operator=Operator.SEQ, operands=[AtomicEventType.J, AtomicEventType.A]
        ),
        "inputs": [AtomicEventType.J, AtomicEventType.A],
    }


def test_statement_parser_two():
    statement = "SELECT AND(E, SEQ(J, A)) FROM E, SEQ(J, A) ON {5, 9}"
    parsed = StatementParser(statement).parse()

    assert parsed == {
        "nodes": [5, 9],
        "query": Query(
            operator=Operator.AND,
            operands=[
                AtomicEventType.E,
                Query(
                    operator=Operator.SEQ,
                    operands=[AtomicEventType.J, AtomicEventType.A],
                ),
            ],
        ),
        "inputs": [
            AtomicEventType.E,
            Query(
                operator=Operator.SEQ, operands=[AtomicEventType.J, AtomicEventType.A]
            ),
        ],
    }
