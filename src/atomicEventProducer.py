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

  def __init__(self, atomicEventType, eventIntervalsFileName, connection, id):
    super().__init__(connection, id, None, atomicEventType.value)
    self.atomicEventType = atomicEventType
    with open(eventIntervalsFileName) as f:
        reader = csv.reader(f, delimiter=",")
        self.eventIntervals = list(reader)[0]

  def pushEvents(self):
    for interval in self.eventIntervals:
      print('now sending, then waiting for {}s'.format(int(interval)/1000))
      super().send("A")
      time.sleep(int(interval)/1000)

def register_and_start_atomic_event_producers():
    connection = make_connection()

    print('creating atomic eventProducers...')
    a_event_producer = AtomicEventProducer(AtomicEventType.A, AtomicEventProducer.fileNameAEvents, connection, 'a_node')
    b_event_producer = AtomicEventProducer(AtomicEventType.B, AtomicEventProducer.fileNameBEvents, connection, 'b_node')
    c_event_producer = AtomicEventProducer(AtomicEventType.C, AtomicEventProducer.fileNameCEvents, connection, 'c_node')
    d_event_producer = AtomicEventProducer(AtomicEventType.D, AtomicEventProducer.fileNameDEvents, connection, 'd_node')
    e_event_producer = AtomicEventProducer(AtomicEventType.E, AtomicEventProducer.fileNameEEvents, connection, 'e_node')
    f_event_producer = AtomicEventProducer(AtomicEventType.F, AtomicEventProducer.fileNameFEvents, connection, 'f_node')
    j_event_producer = AtomicEventProducer(AtomicEventType.J, AtomicEventProducer.fileNameJEvents, connection, 'j_node')

    print('nodes created. Start pushing atomic events now')
    a_event_producer.pushEvents()