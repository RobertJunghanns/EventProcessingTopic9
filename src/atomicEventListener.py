import time
from connection import ActiveMQNode
from evaluation_plan import Statement, AtomicEventType

import os

SLEEP = float(os.environ.get("SLEEP", 20))


if __name__ == "__main__":
    atomic_events = [
        AtomicEventType.A,
        AtomicEventType.B,
        AtomicEventType.C,
        AtomicEventType.D,
        AtomicEventType.E,
        AtomicEventType.F,
        AtomicEventType.J,
    ]

    statement = Statement(query=None, inputs=atomic_events, nodes=None)
    print("Waiting for ActiveMQ to start")
    time.sleep(SLEEP)
    ActiveMQNode(id_=0, statements=[statement]).start()  # handles subscription

    time.sleep(3600)
