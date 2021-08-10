from sortedcontainers import SortedList
from collections import OrderedDict
from operator import neg
from enum import IntEnum
# import pandas as pd
from Side import BUY, SELL ,Side

# class Side(IntEnum):
#   BUY  = ord('B')
#   SELL = ord('S')


class DetailedLob:
  # Aliasing

  def __init__(self):
    self.mbo = {None: {BUY: {}, SELL: {}}}  # MBO - most detailed
    self.id_to_price   : dict[int,int] = {None: {}}
    self.sorted_prices : dict[int,dict[Side, SortedList]] \
        = {None: {BUY: SortedList(key=neg), SELL: SortedList()}}
    self.mbp : dict[int,dict[str, dict]] = {None: {BUY: {}, SELL: {}}}  # MBP

  def insert_add(self, itch_message):
    side = Side(ord(itch_message.Side))
    price = itch_message.Price
    order_id = itch_message.OrderID
    quantity = itch_message.Quantity
    rank = itch_message.Rank
    symbol_id = itch_message.mbp_id
    if symbol_id not in self.mbp.keys():
      self.mbo.update({symbol_id: {BUY: {}, SELL: {}}})  # MBO - most detailed
      self.id_to_price.update({symbol_id: {}})
      self.sorted_prices.update({symbol_id: {BUY: SortedList(key=neg), SELL: SortedList()}})
      self.mbp.update({symbol_id: {BUY: {}, SELL: {}}})  # MBP
    else:
      pass
    if price in self.mbp[symbol_id][side].keys():
      self.mbo[symbol_id][side][price].update({order_id: [quantity, rank]})
      self.mbp[symbol_id][side][price] += quantity
    else:
      self.sorted_prices[symbol_id][side].add(price)
      self.mbo[symbol_id][side].update({price: OrderedDict({order_id: [quantity, rank]})})
      self.construct_lob(symbol_id, 5000)
      # self.mbp[side].update({price:quantity})
    self.id_to_price[symbol_id].update({(order_id, side): price})

    # else:
    #   self.mbo.update({symbol_id:{side:{price:{order_id: [quantity,rank]}}}})
    #   self.sorted_prices.update({symbol_id:{BUY: SortedList(key=neg), SELL: SortedList()}})
    #   self.sorted_prices[symbol_id][side].add(price)
    #   self.id_to_price.update({symbol_id:{(order_id, side): price}})
    #   self.construct_lob(symbol_id, 5000)

  def get_order(self, itch_message):
    side = Side(ord(itch_message.Side))
    order_id = itch_message.OrderID
    symbol_id = itch_message.mbp_id
    price = self.id_to_price[symbol_id][(order_id, side)]
    return side, price, order_id, symbol_id

  def execution(self, itch_message):
    # side, price, order_id = self.get_order(itch_message)
    side = Side(ord(itch_message.Side))
    symbol_id = itch_message.mbp_id
    order_id = itch_message.OrderID
    price = self.id_to_price[symbol_id][(order_id, side)]

    self.mbo[symbol_id][side][price][order_id][0] -= itch_message.Quantity  # ExecutedQuantity
    self.mbp[symbol_id][side][price] -= itch_message.Quantity  # ExecutedQuantity
    if self.mbo[symbol_id][side][price][order_id][0] == 0:
      del self.mbo[symbol_id][side][price][order_id]
      if len(self.mbo[symbol_id][side][price]) == 0:
        del self.mbo[symbol_id][side][price]
        del self.mbp[symbol_id][side][price]
        self.sorted_prices[symbol_id][side].remove(price)
    return price

  def deletion(self, itch_message):
    side, price, order_id, symbol_id = self.get_order(itch_message)
    quantity = self.mbo[symbol_id][side][price][order_id][0]

    del self.mbo[symbol_id][side][price][order_id]
    self.mbp[symbol_id][side][price] -= quantity  # DeletedQuantity
    if len(self.mbo[symbol_id][side][price]) == 0:
      del self.mbo[symbol_id][side][price]
      del self.mbp[symbol_id][side][price]
      self.sorted_prices[symbol_id][side].remove(price)
    return quantity, price

  def construct_lob(self, symbol, n) -> int:
    self.mbp[symbol] = {BUY: {}, SELL: {}}

    for key, value in self.mbp[symbol].items():
      i = 0
      k = 0
      prices = self.sorted_prices[symbol][key]
      while i < n:
        try:
          price_key = prices[k]
        except IndexError:
          break
        try:
          quantity = sum([x[0] for _, x in self.mbo[symbol][key][price_key].items()])
          self.mbp[symbol][key].update({price_key: quantity})
          i += 1
        except KeyError:
          pass
        finally:
          k += 1
    return self.mbp[symbol]
