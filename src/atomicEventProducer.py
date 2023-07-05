import csv
import threading
import time

from connection import ActiveMQNode, make_connection
from evaluation_plan import AtomicEventType

class AtomicEventProducer(ActiveMQNode):
  fileNameAEvents = 'atomicEvents_A_timestamps.csv'
  fileNameBEvents = 'atomicEvents_B_timestamps.csv'
  fileNameCEvents = 'atomicEvents_C_timestamps.csv'
  fileNameDEvents = 'atomicEvents_D_timestamps.csv'
  fileNameEEvents = 'atomicEvents_E_timestamps.csv'
  fileNameFEvents = 'atomicEvents_F_timestamps.csv'
  fileNameJEvents = 'atomicEvents_J_timestamps.csv'

  def __init__(self, eventIntervalsFileName, connection, id):
    super().__init__(connection, id, None, ['A', 'B', 'C', 'D', 'E', 'F', 'J'])
    with open(eventIntervalsFileName) as f:
        reader = csv.reader(f, delimiter=",")
        self.eventTimestamps = list(reader)

  def pushEvents(self):
    previousEvent = ["0", ""]
    timeDifferenceToCurrentEvent = 0
    for currentEvent in self.eventTimestamps:
      timeDifferenceToCurrentEvent = int(currentEvent[0]) - int(previousEvent[0])  
      time.sleep(timeDifferenceToCurrentEvent / 1000)
      super().setQueryTopic(currentEvent[1])
      super().send(currentEvent[1])
      previousEvent = currentEvent


def register_and_start_atomic_event_producers():
    connection = make_connection()
    atomic_event_producer = AtomicEventProducer('combinedEventTimestamps.csv', connection, 'atomicEventProducer')
    print('nodes created. Start pushing atomic events now')
    atomic_event_producer.pushEvents()