from collections import defaultdict

from evaluation_plan import (
    AtomicEventType,
    Operator,
    Query,
    Statement,
    StatementParser,
)

STATEMENTS = [
    "SELECT SEQ(A, F, C) FROM A, F, C ON {0}",
    "SELECT SEQ(J, A) FROM J, A ON {4}",
    "SELECT AND(C, E, D, F) FROM C, E, D, F ON {2, 4}",
    "SELECT AND(C, E, B, D, F) FROM B, AND(C, E, D, F) ON {0, 1, 2, 3, 4, 5}",
    "SELECT AND(E, SEQ(J, A)) FROM E, SEQ(J, A) ON {9}",
    "SELECT AND(E, SEQ(C, J, A)) FROM AND(E, SEQ(J, A)), C ON {5, 9}",
]

OPTIMIZED_STATEMENTS = [
    "SELECT SEQ(A, F, C) FROM A, F, C ON {0}",
    "SELECT SEQ(J, A) FROM J, A ON {4}",
    "SELECT AND(C, E, D, F) FROM C, E, D, F ON {2, 4}",
    "SELECT AND(B, AND(C, E, D, F)) FROM B, AND(C, E, D, F) ON {0, 1, 2, 3, 4, 5}",  # oprands in query change
    "SELECT AND(E, SEQ(J, A)) FROM E, SEQ(J, A) ON {9}",
    "SELECT AND(E, SEQ(C, J, A)) FROM E, SEQ(C, J, A) ON {5, 9}",
    "SELECT SEQ(C, J, A) FROM C, J, A ON {10}",  # Helper Node
]


def dep_optimizer(statements):
    # first parse all statements to get an overview
    parsed_statements = []
    statements_by_node = defaultdict(list)
    query_to_node_map = defaultdict(list)

    for statement in STATEMENTS:
        parsed_statement = StatementParser(statement).parse()
        parsed_statements.append(parsed_statement)

        for node in parsed_statement.nodes:
            statements_by_node[node.value].append(statement)
            query_to_node_map[str(parsed_statement.query)].append(node.value)

    optimized = []

    for statement in parsed_statements:
        query = statement.query

        if query.operator == Operator.AND:
            # See if we can make a smart substitution
            # .e.g replace AND(C, E, B, D, F) with AND(B, AND(C, E, D, F))

            for input_ in statement.inputs:
                if (
                    isinstance(input_, Query)
                    and str(input_) not in str(query)
                    and all([op in query.operands for op in input_.operands])
                ):
                    # input_ is not part of the query, but all its operands are
                    optimized_query = Query(
                        operator=query.operator,
                        operands=[
                            *(op for op in query.operands if op not in input_.operands),
                            input_,
                        ],
                    )
                    statement.query = optimized_query

        for operand in query.operands:
            if isinstance(operand, Query) and operand not in statement.inputs:
                # check another node already produces this event
                # and append event to inputs if so
                if str(operand) in query_to_node_map:
                    query.inputs.append(operand)
                    continue
                else:
                    # create helper node
                    helper_node_id = max(statements_by_node.keys()) + 1
                    helper_query = operand
                    helper_inputs = operand.operands
                    optimized.append(
                        Statement(
                            nodes=[helper_node_id],
                            query=helper_query,
                            inputs=helper_inputs,
                        )
                    )

        optimized.append(statement)

    return optimized


def test_operator_equality():
    query_a = Query(Operator.AND, [AtomicEventType("A"), AtomicEventType("B")])
    query_b = Query(Operator.AND, [AtomicEventType("B"), AtomicEventType("A")])

    assert query_a == query_a
    assert query_a == query_b
    assert query_b == query_a


def test_operator_inequality():
    query_a = Query(Operator.AND, [AtomicEventType("A"), AtomicEventType("B")])
    query_b = Query(Operator.AND, [AtomicEventType("A"), AtomicEventType("C")])

    assert query_a != query_b


def test_dep_optimizer():
    optimized = dep_optimizer(STATEMENTS) or []

    assert sorted([op.to_string() for op in optimized]) == sorted(OPTIMIZED_STATEMENTS)
