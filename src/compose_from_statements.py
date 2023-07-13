from collections import defaultdict

from evaluation_plan import StatementParser

NODE_BASE = """
  node_{node_id}:
    build:
      context: .
      dockerfile: ./Dockerfile
    environment:
      - ACTIVEMQ_HOST=activemq
      - ACTIVEMQ_PORT=61613
      - STATEMENTS={statements}
      - NODE_ID={node_id}
      - SLEEP=100
    depends_on:
      activemq:
        condition: service_healthy
    command: ["python3", "siddhi_connection.py"]
"""

ACTIVE_MQ_BASE = """
  activemq:
    image: islandora/activemq:main
    container_name: activemq
    environment:
      - ACTIVEMQ_NAME=activemq
      - ACTIVEMQ_USER=admin
      - ACTIVEMQ_PASSWORD=admin
    ports:
      - 8161:8161
    healthcheck:
      test: ["CMD", "sleep", "1"]
      interval: 5s
      retries: 0
"""

ATOMIC_PRODUCER = """
  eventproducer:
    build:
      context: .
      dockerfile: ./Dockerfile
    environment:
      - ACTIVEMQ_HOST=activemq
      - ACTIVEMQ_PORT=61613
    depends_on:
      activemq:
        condition: service_healthy
    command: ["python3", "atomicEventProducer.py"]
"""


STATEMENTS = [
    "SELECT SEQ(A, F, C) FROM A, F, C ON {0}",
    "SELECT SEQ(J, A) FROM J, A ON {4}",
    "SELECT AND(C, E, D, F) FROM C, E, D, F ON {2, 4}",
    "SELECT AND(C, E, B, D, F) FROM B, AND(C, E, D, F) ON {0, 1, 2, 3, 4, 5}",
    "SELECT AND(E, SEQ(J, A)) FROM E, SEQ(J, A) ON {9}",
    "SELECT AND(E, SEQ(C, J, A)) FROM AND(E, SEQ(J, A)), C ON {5, 9}",
]

if __name__ == "__main__":
    statements_by_node = defaultdict(list)

    for statement in STATEMENTS:
        parsed_statement = StatementParser(statement).parse()
        for node in parsed_statement.nodes:
            statements_by_node[node.value].append(statement)

    with open("docker-compose.statements.yaml", "w") as f:
        f.write('version: "3.8"\n')
        f.write("services: \n")

        for node, statement_strings in statements_by_node.items():
            f.write(
                NODE_BASE.format(node_id=node, statements="|".join(statement_strings))
            )
            f.write("\n")

        f.write(ACTIVE_MQ_BASE)
        f.write("\n")
        f.write(ATOMIC_PRODUCER)
