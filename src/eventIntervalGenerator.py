"""
This class generates files which describe in which intervals events should be pushed to the queue
"""
import csv
import random
from pathlib import Path

from atomicEventProducer import AtomicEventProducer

file_root = Path(__file__).parent


class EventIntervaldGenerator:
    def __init__(self) -> None:
        pass

    def generate_random_event_intervals(
        self, totalTimeInMs, minTimeStepInMs, maxTimeStepInMs, filename
    ):
        print("generating intervals for file: " + filename)

        eventIntervals = []
        timeEventsAreCalculatedFor = 0
        while timeEventsAreCalculatedFor < totalTimeInMs:
            nextTimeInterval = random.randint(minTimeStepInMs, maxTimeStepInMs)
            eventIntervals.append(timeEventsAreCalculatedFor + nextTimeInterval)
            timeEventsAreCalculatedFor += nextTimeInterval

        self.writeSingleEventIntervalsToFile(eventIntervals, filename)
        return eventIntervals

    def generate_fixed_event_intervals(self, totalTimeInMs, interval, filename):
        print("generating intervals for file: " + filename)

        eventIntervals = []
        timeEventsAreCalculatedFor = 0
        while timeEventsAreCalculatedFor < totalTimeInMs:
            eventIntervals.append(timeEventsAreCalculatedFor + interval)
            timeEventsAreCalculatedFor += interval

        self.writeSingleEventIntervalsToFile(eventIntervals, filename)
        return eventIntervals

    def writeSingleEventIntervalsToFile(self, eventIntervals, filename):
        with open(filename, "w") as f:
            writer = csv.writer(f)
            writer.writerow(eventIntervals)

    def writeCombinedEventIntervalsToFile(self, combinedEventTimestamps, filename):
        with open(
            filename, "w", newline=""
        ) as f:  # newline-argument needed for windows machines
            writer = csv.writer(f)
            writer.writerows(combinedEventTimestamps)


if __name__ == "__main__":
    # Generate atomic events
    totalTimeInMs = (
        600000  # set this variable to decide how long events should be generated for
    )
    eventIntervalGenerator = EventIntervaldGenerator()
    a_event_intervals = eventIntervalGenerator.generate_random_event_intervals(
        totalTimeInMs, 500, 30000, AtomicEventProducer.fileNameAEvents
    )
    b_event_intervals = eventIntervalGenerator.generate_random_event_intervals(
        totalTimeInMs, 1000, 30000, AtomicEventProducer.fileNameBEvents
    )
    c_event_intervals = eventIntervalGenerator.generate_fixed_event_intervals(
        totalTimeInMs, 30000, AtomicEventProducer.fileNameCEvents
    )
    d_event_intervals = eventIntervalGenerator.generate_random_event_intervals(
        totalTimeInMs, 30000, 600000, AtomicEventProducer.fileNameDEvents
    )
    e_event_intervals = eventIntervalGenerator.generate_random_event_intervals(
        totalTimeInMs, 500, 30000, AtomicEventProducer.fileNameEEvents
    )
    f_event_intervals = eventIntervalGenerator.generate_fixed_event_intervals(
        totalTimeInMs, 5000, AtomicEventProducer.fileNameFEvents
    )
    j_event_intervals = eventIntervalGenerator.generate_random_event_intervals(
        totalTimeInMs, 500, 30000, AtomicEventProducer.fileNameJEvents
    )

    ms_iterator = 0
    combinedEventsArray = []
    while ms_iterator < totalTimeInMs:
        if ms_iterator % 100000 == 0:
            print("{}ms of {}ms merged".format(ms_iterator, totalTimeInMs))
        if a_event_intervals.__contains__(ms_iterator):
            eventTimeStamp = [ms_iterator, "A"]
            combinedEventsArray.append(eventTimeStamp)
        if b_event_intervals.__contains__(ms_iterator):
            eventTimeStamp = [ms_iterator, "B"]
            combinedEventsArray.append(eventTimeStamp)
        if c_event_intervals.__contains__(ms_iterator):
            eventTimeStamp = [ms_iterator, "C"]
            combinedEventsArray.append(eventTimeStamp)
        if d_event_intervals.__contains__(ms_iterator):
            eventTimeStamp = [ms_iterator, "D"]
            combinedEventsArray.append(eventTimeStamp)
        if e_event_intervals.__contains__(ms_iterator):
            eventTimeStamp = [ms_iterator, "E"]
            combinedEventsArray.append(eventTimeStamp)
        if f_event_intervals.__contains__(ms_iterator):
            eventTimeStamp = [ms_iterator, "F"]
            combinedEventsArray.append(eventTimeStamp)
        if j_event_intervals.__contains__(ms_iterator):
            eventTimeStamp = [ms_iterator, "J"]
            combinedEventsArray.append(eventTimeStamp)

        ms_iterator += 1
    eventIntervalGenerator.writeCombinedEventIntervalsToFile(
        combinedEventsArray, str(file_root / "data/combinedEventTimestamps.csv")
    )
