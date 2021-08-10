
# from sortedcontainers import SortedList, SortedSet, SortedDict
# from collections import OrderedDict
# import pandas as pd
from enum import Enum
from enum import IntEnum
from src.SimulatorV10 import simulatorV10


class Side(IntEnum):
  BUY  = ord('B')
  SELL = ord('S')


if __name__ == "__main__":
  sim = simulatorV10()
  print(Side.BUY.value, Side.SELL.value)
  print(Side.BUY == 66)
  print(Side(66))