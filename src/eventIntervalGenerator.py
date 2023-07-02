"""
This class generates files which describe in which intervals events should be pushed to the queue
"""
import csv
import random

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
        
        with open(filename, 'w') as f:
            writer = csv.writer(f)
            writer.writerow(eventIntervals)


