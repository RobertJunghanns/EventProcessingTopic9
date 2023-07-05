import csv
import threading
import time

from connection import ActiveMQNode, make_connection
from evaluation_plan import AtomicEventType

class AtomicEventProducer(ActiveMQNode):
  fileNameAEvents = 'atomicEvents_A_intervals.csv'
  fileNameBEvents = 'atomicEvents_B_intervals.csv'
  fileNameCEvents = 'atomicEvents_C_intervals.csv'
  fileNameDEvents = 'atomicEvents_D_intervals.csv'
  fileNameEEvents = 'atomicEvents_E_intervals.csv'
  fileNameFEvents = 'atomicEvents_F_intervals.csv'
  fileNameJEvents = 'atomicEvents_J_intervals.csv'

  def __init__(self, eventIntervalsFileName, connection, id):
    super().__init__(connection, id, None, ['A', 'B', 'C', 'D', 'E', 'F', 'J'])
    with open(eventIntervalsFileName) as f:
        reader = csv.reader(f, delimiter=",")
        self.eventTimestamps = list(reader)

  def pushEvents(self):
    print(self.eventTimestamps)
    eventsPushedCounter = 0
    currentTime = 0
    while eventsPushedCounter < len(self.eventTimestamps):
      print('next event is')
      #time.sleep(self.eventTimestamps[eventsPushedCounter + 1][0] - currentTime)
      print('now pushing ' + self.eventTimestamps[eventsPushedCounter + 1][1])


def register_and_start_atomic_event_producers():
    connection = make_connection()
    atomic_event_producer = AtomicEventProducer('combinedEventTimestamps.csv', connection, 'atomicEventProducer')
    print('nodes created. Start pushing atomic events now')
    atomic_event_producer.pushEvents()