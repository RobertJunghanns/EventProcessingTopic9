"""
This class generates files which describe in which intervals events should be pushed to the queue
"""
import csv
import random
from atomicEventProducer import AtomicEventProducer

class EventIntervaldGenerator:
    def __init__(self) -> None:
        pass

    def generate_random_event_intervals(self, totalTimeInMs, minTimeStepInMs, maxTimeStepInMs, filename):
        print('generating intervals for file: ' + filename)

        eventIntervals = []
        timeEventsAreCalculatedFor = 0
        while timeEventsAreCalculatedFor < totalTimeInMs:
            nextTimeInterval = random.randint(minTimeStepInMs, maxTimeStepInMs)
            eventIntervals.append(nextTimeInterval)
            timeEventsAreCalculatedFor += nextTimeInterval
        
        self.write_to_file(eventIntervals, filename)

    def generate_fixed_event_intervals(self, totalTimeInMs, interval, filename):
        print('generating intervals for file: ' + filename)

        eventIntervals = []
        timeEventsAreCalculatedFor = 0
        while timeEventsAreCalculatedFor < totalTimeInMs:
            eventIntervals.append(interval)
            timeEventsAreCalculatedFor += interval
        
        self.write_to_file(eventIntervals, filename)


    def write_to_file(self, eventIntervals, filename):
        with open(filename, 'w') as f:
            writer = csv.writer(f)
            writer.writerow(eventIntervals)



if __name__ == "__main__":
    #Generate atomic events
    eventIntervalGenerator = EventIntervaldGenerator()
    eventIntervalGenerator.generate_random_event_intervals(600000, 500, 30000, AtomicEventProducer.fileNameAEvents)
    eventIntervalGenerator.generate_random_event_intervals(600000, 1000, 30000, AtomicEventProducer.fileNameBEvents)
    eventIntervalGenerator.generate_fixed_event_intervals(600000, 30000, AtomicEventProducer.fileNameCEvents)
    eventIntervalGenerator.generate_random_event_intervals(600000, 30000, 600000, AtomicEventProducer.fileNameDEvents)
    eventIntervalGenerator.generate_random_event_intervals(600000, 500, 30000, AtomicEventProducer.fileNameEEvents)
    eventIntervalGenerator.generate_fixed_event_intervals(600000, 1000, AtomicEventProducer.fileNameFEvents)
    eventIntervalGenerator.generate_random_event_intervals(600000, 500, 30000, AtomicEventProducer.fileNameJEvents)