import csv
from connection import ActiveMQNode
from evaluation_plan import AtomicEventType

class AtomicEventProducer(ActiveMQNode):
  fileNameAEvents = 'atomicEvents_A_intervals.csv'
  fileNameBEvents = 'atomicEvents_B_intervals.csv'
  fileNameCEvents = 'atomicEvents_C_intervals.csv'
  fileNameDEvents = 'atomicEvents_D_intervals.csv'
  fileNameEEvents = 'atomicEvents_E_intervals.csv'
  fileNameFEvents = 'atomicEvents_F_intervals.csv'
  fileNameJEvents = 'atomicEvents_J_intervals.csv'

  def __init__(self, atomicEventType, eventIntervalsFileName):
    self.atomicEventType = atomicEventType
    with open(eventIntervalsFileName) as f:
        reader = csv.reader(f)
        self.eventIntervals = list(reader)