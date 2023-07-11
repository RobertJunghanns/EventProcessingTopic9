import hashlib
import re
from enum import Enum
from typing import Any, List, Union


class ValueEnum(Enum):
    @classmethod
    def values(cls) -> "list[str]":
        return [enum.value for enum in cls]

    @classmethod
    def from_string(cls, text: str) -> "Any":
        for enum in cls:
            if text.startswith(enum.value):
                return enum
        raise ValueError(f"Unknown AtomicEventType: {text}")


class AtomicEventType(ValueEnum):
    A = "A"
    B = "B"
    C = "C"
    D = "D"
    E = "E"
    F = "F"
    J = "J"

    @property
    def topic(self):
        return self.value

    def to_siddhi_query(
        self,
        input_stream="cseEventStream",
        output_stream="outputStream",
        attribute="symbol",
    ):
        return (
            f"@info(name = '{self.topic}') "
            f"from {input_stream}[{attribute} == '{self.value}']  "
            f"select {attribute} "
            f"insert into {output_stream}; "
        )

    def __str__(self) -> str:
        return self.value


class Operator(ValueEnum):
    AND = "AND"
    SEQ = "SEQ"


class NodeEnum(Enum):
    ZERO = 0
    ONE = 1
    TWO = 2
    THREE = 3
    FOUR = 4
    FIVE = 5
    SIX = 6
    SEVEN = 7
    EIGHT = 8
    NINE = 9


class Statement:
    def __init__(
        self,
        nodes: "List[NodeEnum]",
        query: "Query",
        inputs: "List[Union[AtomicEventType,Query]]",
    ) -> None:
        self.nodes = nodes
        self.query = query
        self.inputs = inputs

    @property
    def input_topics(self) -> List[str]:
        return sorted([input_.topic for input_ in self.inputs])

    def __eq__(self, other) -> bool:
        if not isinstance(other, Statement):
            return False

        return (
            self.query.topic == other.query.topic
            and self.input_topics == other.input_topics
        )

    def __repr__(self) -> str:
        return (
            f"Statement(nodes={self.nodes}, query={self.query}, inputs={self.inputs})"
        )


class Query:
    def __init__(
        self, operator: Operator, operands: "List[Union[AtomicEventType, Query]]"
    ) -> None:
        self.operator = operator
        self.operands = operands

    @property
    def topic(self) -> str:
        return str(self)

    @property
    def hash_topic(self) -> str:
        hash_md5 = hashlib.md5(str(self).encode("UTF-8"))
        return hash_md5.hexdigest().strip()

    def to_siddhi_query(
        self,
        input_stream="cseEventStream",
        output_stream="outputStream",
        attribute="symbol",
    ):
        # TODO: Implement proper Siddhi query generation for AND and SEQ here
        return (
            f"@info(name = '{self.topic}') "
            f"from {input_stream}[{attribute} == '{self.topic}']  "
            f"select {attribute} "
            f"insert into {output_stream}; "
        )

    @classmethod
    def from_string(cls, text: str) -> "Query":
        """
        Create a Query from a string representation.
        query = Operator(AtomicEventType, (Query|AtomicEventType)+)


        text: "AND(A, B, C)"
        returns: Query(
                    operator=Operator.AND,
                    operands=[AtomicEventType.A, AtomicEventType.B, AtomicEventType.C]
                )

        text: "SEQ(A, AND(B, C))"
        returns: Query(
                    operator=Operator.SEQ,
                    operands=[
                        AtomicEventType.A,
                        Query(Operator.AND, [AtomicEventType.B, AtomicEventType.C])
                    ]
                )
        """

        # A query always starts with an operator
        # and the first operand is always an AtomicEventType

        match = Query.match_operator(text)

        if not match:
            raise ValueError(
                f"Invalid Query: '{text}'. Must start with a valid operator"
            )

        operator = Operator.from_string(match.group(1))
        operands = Query.parse_operands(match.group(2))

        return cls(operator, operands)

    @staticmethod
    def match_operator(text):
        valid_operators = "|".join([operator.value for operator in Operator])
        return re.match(rf"({valid_operators})\((.*)\)", text)

    @staticmethod
    def parse_operands(operands: str) -> "List[Union[AtomicEventType, Query]]":
        return [
            Query.parse_operand(operand) for operand in Query.split_operands(operands)
        ]

    @staticmethod
    def split_operands(operands: str) -> "list[str]":
        """
        Split operands into a list of strings, one level deep.

        We can't simply split on "," because of nested queries.
        Instead, we parse the string character by character and
        split on "," only if we are NOT inside a nested query.

        Subqueries are identified by the number of currently open parenthesis.
        """
        STOP = "$"
        operands = f"{operands}{STOP}"

        atoms = AtomicEventType.values()
        operators = Operator.values()

        buffer = ""
        open_parenthesis = 0

        results = []

        for index, char in enumerate(operands):
            if char == STOP:
                break

            if char in (",", " ") and not buffer:
                continue

            # We encountered an AtomicEventType
            if char in atoms and operands[index + 1] in (",", "$") and not buffer:
                results.append(char)

            # We encountered a Subquery
            else:
                buffer += char

                if char == "(":
                    open_parenthesis += 1
                elif char == ")":
                    open_parenthesis -= 1

                if char == ")" and open_parenthesis == 0:
                    results.append(buffer)
                    buffer = ""

        # quick sanity check
        for item in results:
            assert any(
                [item.startswith(operator) for operator in operators]
                + [len(item) == 1 and item.startswith(atom) for atom in atoms]
            ), f"Query splitting failed, encountered an unknown item: {results}"

        return results

    @staticmethod
    def parse_operand(operand: str) -> "Union[AtomicEventType, Query]":
        try:
            # Operand is a Query
            Operator.from_string(operand)
            return Query.from_string(operand)
        except ValueError:
            # Operand is an AtomicEventType
            return AtomicEventType.from_string(operand)

    def __eq__(self, other) -> bool:
        if not isinstance(other, Query):
            return False

        return self.operator == other.operator and self.operands == other.operands

    def __repr__(self) -> str:
        return f"Query(operator={self.operator}, operands={self.operands})"

    def __str__(self) -> str:
        return f"{self.operator.value}({', '.join([str(operand) for operand in self.operands])})"


class StatementParser:
    def __init__(self, statement: str) -> None:
        self.statement = statement

    def parse(self) -> Statement:
        """
        Parse a statement from the Distributed Evaluation Plan:

        1. SELECT SEQ(A, F, C) FROM A, F, C ON {0}
        2. SELECT SEQ(J, A) FROM J, A ON {4}
        3. SELECT AND(C, E, D, F) FROM C, E, D, F ON {2, 4}
        4. SELECT AND(C, E, B, D, F) FROM B, AND(C, E, D, F) ON {0, 1, 2, 3, 4, 5}
        5. SELECT AND(E, SEQ(J, A)) FROM E, SEQ(J, A) ON {9}
        6. SELECT AND(E, SEQ(C, J, A)) FROM AND(E, SEQ(J, A)), C ON {5, 9}

        Example 1

        query: SELECT SEQ(J, A) FROM J, A ON {4}
        returns:  Statement(
                    nodes=[Node.FOUR],
                    query=Query(
                        operator=Operator.SEQ, operands=[AtomicEventType.J, AtomicEventType.A]
                    ),
                    inputs=[AtomicEventType.J, AtomicEventType.A]
                )

        Example 2
        query: SELECT AND(E, SEQ(J, A)) FROM E, SEQ(J, A) ON {9}
        returns: Statement(
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

        """
        regex = r"SELECT (.*) FROM (.*) ON {(.*)}"
        match = re.match(regex, self.statement)

        if not match:
            raise ValueError(f"Invalid statement: '{self.statement}'")

        query = Query.from_string(match.group(1))
        inputs = Query.parse_operands(match.group(2))
        nodes = [NodeEnum(int(node)) for node in match.group(3).split(",")]

        return Statement(query=query, inputs=inputs, nodes=nodes)
