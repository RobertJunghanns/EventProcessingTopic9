import csv
import time

from connection import ActiveMQNode


class AtomicEventProducer(ActiveMQNode):
    fileNameAEvents = "atomicEvents_A_timestamps.csv"
    fileNameBEvents = "atomicEvents_B_timestamps.csv"
    fileNameCEvents = "atomicEvents_C_timestamps.csv"
    fileNameDEvents = "atomicEvents_D_timestamps.csv"
    fileNameEEvents = "atomicEvents_E_timestamps.csv"
    fileNameFEvents = "atomicEvents_F_timestamps.csv"
    fileNameJEvents = "atomicEvents_J_timestamps.csv"

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
            self.send(currentEvent[1], topic=f"/topic/{currentEvent[1]}")
            previousEvent = currentEvent


def register_and_start_atomic_event_producers():
    atomic_event_producer = AtomicEventProducer(
        id_=0,
        eventIntervalsFileName="combinedEventTimestamps.csv",
    )
    print("nodes created. Start pushing atomic events now")
    atomic_event_producer.pushEvents()
