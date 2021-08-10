import sys, os.path
src_dir = (os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))
sys.path.append(src_dir)

from DetailedLob import DetailedLob
from collections import OrderedDict

class LobBs:
  def __init__(self, mbp_id=0):  # part, callback
    self.details = DetailedLob()
    self.mbo_buy = {}  # Bid side of lob
    self.mbo_sell = {}  # Ask side of lob
    self.mbp_id = mbp_id  # orderbook id of lob
    self.order_to_time_stamp = {}
    self.mboState = None
    self.orders = {'B': OrderedDict(),
             'S': OrderedDict()}
    # self.part = part
    # self.callback = callback
    # self.order_resp = OrderResponse()

  def process_relevant_messages(self, itchMsg):
    _type = itchMsg.MessageType

    # if _type == 'O':
    #   self.mboState = itchMsg.StateName.strip()
    #   print(self.mboState)
    if _type in 'ADE':
      if _type == 'D':
        # print('type D')
        self.details.deletion(itchMsg)
      elif _type == 'A':
        self.details.insert_add(itchMsg)
      elif _type == 'E':
        # print('type E')
        self.details.execution(itchMsg)
      # self.details.construct_lob(50)
