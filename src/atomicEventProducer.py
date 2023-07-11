import csv
import time

from connection import ActiveMQNode


class AtomicEventProducer(ActiveMQNode):
    fileNameAEvents = "data/atomicEvents_A_timestamps.csv"
    fileNameBEvents = "data/atomicEvents_B_timestamps.csv"
    fileNameCEvents = "data/atomicEvents_C_timestamps.csv"
    fileNameDEvents = "data/atomicEvents_D_timestamps.csv"
    fileNameEEvents = "data/atomicEvents_E_timestamps.csv"
    fileNameFEvents = "data/atomicEvents_F_timestamps.csv"
    fileNameJEvents = "data/atomicEvents_J_timestamps.csv"

    def __init__(self, *args, eventIntervalsFileName, eventTimestamps=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.eventIntervalsFileName = eventIntervalsFileName

        if eventTimestamps is not None:
            self.eventTimestamps = eventTimestamps
        else:
            with open(self.eventIntervalsFileName) as f:
                reader = csv.reader(f, delimiter=",")
                self.eventTimestamps = list(reader)

    def pushEvents(self):
        previousEvent = ["0", ""]
        timeDifferenceToCurrentEvent = 0
        for currentEvent in self.eventTimestamps:
            timeDifferenceToCurrentEvent = int(currentEvent[0]) - int(previousEvent[0])
            time.sleep(timeDifferenceToCurrentEvent / 1000)
            print(f"AtomicEventProducer sending event:  {currentEvent[1]}")
            self.send(currentEvent[1], topic=f"/topic/{currentEvent[1]}")
            previousEvent = currentEvent


def register_and_start_atomic_event_producers():
    atomic_event_producer = AtomicEventProducer(
        id_=0,
        eventIntervalsFileName="data/combinedEventTimestamps.csv",
    )
    print("nodes created. Start pushing atomic events now")
    atomic_event_producer.pushEvents()


if __name__ == "__main__":
    time.sleep(10)  # wait for ActiveMQ to start
    register_and_start_atomic_event_producers()
