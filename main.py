import re
from dataclasses import dataclass
from enum import Enum


class AtomicEventType(Enum):
    A = 1
    B = 2
    C = 3
    D = 4
    E = 5
    F = 6
    J = 7


class Operator(Enum):
    AND = 1
    SEQ = 2


class Node(Enum):
    ONE = 1
    TWO = 2
    THREE = 3
    FOUR = 4
    FIVE = 5
    SIX = 6
    SEVEN = 7
    EIGHT = 8
    NINE = 9


@dataclass
class QueryParser:
    query: str

    def parse(self):
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
        result: {
            "nodes" = [Node.FOUR]
            "query": "SEQ(AtomicEventType.J, AtomicEventType.A)", --> ADVERTISE
            "inputs": [AtomicEventType.J, AtomicEventType.A]  --> SUBSRIBE TO
        }

        Example 2
        query: SELECT AND(E, SEQ(J, A)) FROM E, SEQ(J, A) ON {9}
        result: {
            "nodes" = [Node.NINE]
            "query": "AND(AtomicEventType.E, SEQ(AtomicEventType.J, AtomicEventType.A))",
            "inputs": [AtomicEventType.E, SEQ(AtomicEventType.J, AtomicEventType.A)]
        }

        Example 3 - Different Interpretation
        query: AND(E, SEQ(C, J, A)) FROM AND(E, SEQ(J, A)), C ON {5, 9}

        Split up Subqueries on multiple nodes or execute the same query on all nodes?
        result: [{
            "nodes" = [Node.NINE]
            "query": "SEQ(C, J, A))",
            "inputs": [AtomicEventType.C, AtomicEventType.J, AtomicEventType.A]
        },
        {
            "nodes" = [Node.FIVE]
            "query": "AND(AtomicEventType.E, SEQ(C,J,A))",
            "inputs": [AtomicEventType.J, AtomicEventType.A]
        }]
        """
        self.check_query()

        # Extract nodes
        pattern = r"{(.*?)}"
        match = re.search(pattern, self.query)
        if match:
            nodes = match.group(1).split(",")
            nodes = [int(id.strip()) for id in nodes]
        else:
            raise Exception("Nodes not found")

        # Extract query component
        pattern = r"SELECT\s+(.*?)\s+FROM"
        match = re.search(pattern, self.query)
        if match:
            query = match.group(1).strip()
        else:
            raise Exception("Query not found")
        
        # Extract inputs (subscription event types)
        pattern = r"FROM\s+(.*?)\s+ON"

        match = re.search(pattern, self.query)
        if match:
            substring = match.group(1)
            inputs = self.split_ignore_parentheses(substring, ",") #split on every comma that is not enclosed in brakets
            inputs = [elem.strip() for elem in inputs] #strip emty characters from results
        else:
            raise Exception("Inputs not found")
        
        result = {
            "nodes": nodes,
            "query": query,
            "inputs": inputs
        }
        return result

    def check_query(self):
        if self.query == "":
            raise Exception("Query is empty")
        
    @staticmethod
    def split_ignore_parentheses(string, delimiter):
        """
        Split a String on every delimiter that is not enclosed in brakets
        """
        result = []
        stack = []
        current = ""
        for char in string:
            if char == delimiter and not stack:
                result.append(current.strip())
                current = ""
            else:
                current += char
                if char == "(":
                    stack.append("(")
                elif char == ")":
                    if stack:
                        stack.pop()
        result.append(current.strip())
        return result        
            



if __name__ == "__main__":
    parser = QueryParser('SELECT AND(E, SEQ(C, J, A)) FROM AND(E, SEQ(J, A)), C ON {5, 9}');
    print(parser.parse());

