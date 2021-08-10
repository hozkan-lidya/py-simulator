import sys, os.path
src_dir = (os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))
sys.path.append(src_dir)

import aux as aux
from Side import Side, BUY, SELL

import pandas as pd
import numpy as np

algono =1
### Simulator ###
class simulatorV10:
  def __init__(self):

    self.orderresponse = pd.DataFrame(columns=['orderresponse_eventtime', 'orderresponse_algono', 'orderresponse_symbol', 'orderresponse_id',
             'orderresponse_side', 'orderresponse_price', 'orderresponse_size', 'ordersresponse_type',
             'ordersresponse_messagetypetype'])

    self.orders = pd.DataFrame(columns=['orders_eventtime', 'orders_algono', 'orders_symbol', 'orders_side', 'orders_price', 'orders_size',
             'orders_type', 'orders_message', 'orders_sendintorderid', 'orders_intorderid', 'executed'])

    self.executions = pd.DataFrame(columns=['execution_eventtime', 'execution_algono', 'execution_symbol', 'execution_price', 'execution_size','execution_side','execution_orderid'])

    self.mm = pd.DataFrame(columns=['mm_eventtime', 'mm_algono', 'mm_symbol', 'mm_side', 'mm_price', 'mm_size', 'mm_type',
           'mm_message', 'mm_orderid', 'all_priceahead', 'limit_orderahead',
           'limit_sizeahead', 'queue'])

    self.positions = pd.DataFrame(columns=['positions_algono', 'num_positions'])
    self.orderid = 1
    self.risk_check = {'algono':0, 'intorderid':0, 'result':1 }

  ### Simulator Main Functions ###
  def create_queue(self, book, symbol, side, price):
    order_side = Side(ord(side))
    if order_side == Side.SELL:
      price_levels = len([i for i in list(book.details.mbp[symbol][order_side].keys()) if i < price])
    else:
      price_levels = len([i for i in list(book.details.mbp[symbol][order_side].keys()) if i > price])
    # ids = [i for pri in price_levels for i in book.details.mbo[symbol][order_side][pri].keys()]
    # quantities = [i[0] for pri in price_levels for i in book.details.mbo[symbol][order_side][pri].values()]
    try:
      ids = list(book.details.mbo[symbol][order_side][price].keys())
    except:
      ids = []
    try:
      quantities = [i[0] for i in book.details.mbo[symbol][order_side][price].values()]
    except:
      quantities = []
    queue = pd.DataFrame(zip(ids, quantities), columns=['id', 'quantity'])
    return queue, price_levels

  def send_order(self, row, algono, sendsymbol, sendside, sendprice, sendsize, sendordertype, sendordermessage, *args):
    '''
    sendmultiplier  --> symbol multiplier
     sendprice     --> order price
     sendsize      --> order size
     sendside      --> c_bid or c_ask
     sendordertype   --> c_gtc, c_gtd, c_fok or c_fak
     sendordermessage  --> c_cancel, c_new or c_replace
     sendintorderid  --> 0 for c_new, send an internalorderid number for c_replace or c_cancel
     executed       --> yes, no, partial
     '''

    self.risk_check_mod(algono)
    if self.risk_check['algono'] == algono and self.risk_check['result'] != 1:
      print('Order can not PASS RISK CHECK')
    else:
      if sendordermessage == 'new':
        if len(args) > 0:
          print('Simulator ERROR!: User can not assign an OrderID to a New order')
        else:
          intorderid = self.risk_check['intorderid']
          sendintorderid = 0
          self.orderid += 1

      elif sendordermessage == 'replace':
        if len(args) != 1:
          print('Simulator ERROR!: User should put an OrderID for a Replace order')
        else:
          intorderid = self.risk_check['intorderid']
          sendintorderid = args[0]
          self.orderid += 1

      elif sendordermessage == 'cancel':
        if len(args) != 1:
          print('Simulator ERROR!: User should put an OrderID for a Cancel order')
        else:
          intorderid = args[0]
          sendintorderid = args[0]

      else:
        print('Simulator ERROR!: MessageType is not acceptable')

      self.orders = self.orders.sort_index().reset_index(drop=True)
      self.orders.loc[len(self.orders)] = [row.epoch_nano,algono, sendsymbol,sendside,sendprice,sendsize,
                         sendordertype,sendordermessage,sendintorderid,intorderid,'no']

      # return orders

  def risk_check_mod(self, algono):
    '''
    RISKRESPONSE (1 to 3) --> (c_risk_algono, c_risk_intorderid, c_risk_response)
    c_risk_algono     --> algono
    c_risk_intorderid   --> internal order id
    c_risk_result    --> c_accepted (1), c_reject_sample_reason (2,3,4...)
    '''
    # ~~ DO RISK CHECKS HERE ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #   check if ORDERS() array parameters are entered correctly
    #   check universal risks common for all algos
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    risk_result = 1
    self.risk_check['algono'] = algono
    self.risk_check['result'] = risk_result
    self.risk_check['intorderid'] = self.orderid

  def order_handler(self, book, row_iter):
    '''
    ORDERS() array holds both IOC or Limit orders temporarily
    IOC orders  --> 1 - executes fully
              2 - executes partially
              3 - rejects the order
    Limit orders  --> 1 - executes fully if order price is crossing the curent MBP book prices AND order size is less than MBP size
              2 - executes partially if order price is crossing the curent MBP book prices AND order size is bigger than the MBP size AND remaining size goes into the MM() array as a limit order
              3 - order goes into the MM() directly as it is not crossing any MBP prices
    '''
    self.orderresponse = self.orderresponse.iloc[0:0]
    data_elapsedtime = 165e3
    count_response = 1
    this_epoch_nano = row_iter.epoch_nano
    next_epoch_nano = row_iter.next_epoch_nano
    # self.orders = self.orders[self.orders['orders_message'] != 'accepted'].reset_index(drop = True)
    if len(self.orders[self.orders['orders_message'] != 'accepted']) > 0:
      for order_iter in self.orders[self.orders['orders_message'] != 'accepted'].itertuples(index=True):
        if next_epoch_nano >= order_iter.orders_eventtime + data_elapsedtime:
          # if len(orderresponse) > 0 :
          #   orderresponse = orderresponse[orderresponse['orderresponse_id'] != order_iter.orders_sendintorderid]  # drop the order in the loop
          # side = (BUY if order_iter.orders_side == 'B' else SELL)
          tob_bid_price, tob_ask_price = aux.take_bid_ask_price(book, order_iter.orders_symbol)
          crossed_side = (BUY if order_iter.orders_side == 'S' else SELL)
          order_size = order_iter.orders_size
          execution = 'not completed'
          price_level_counter = 0

          if order_iter.orders_type == 'fak':
            ##### CROSS THE BID-ASK SPREAD #####

            if ((order_iter.orders_side == 'B') and (order_iter.orders_price >= tob_ask_price)) or (
                (order_iter.orders_side == 'S') and (order_iter.orders_price <= tob_bid_price)):
              if order_iter.orders_side == 'B':
                price_levels = [i for i in list(book.details.mbp[order_iter.orders_symbol][SELL].keys()) if
                        i <= order_iter.orders_price]
                # booksize = sum([i[0] for pri in price_levels for i in book.details.mbo[order_iter.orders_symbol][SELL][pri].values()])
                # booksize = sum([i[0] for pri in price_levels for i in book.details.mbo[order_iter.orders_symbol][SELL][pri].values()])

              elif order_iter.orders_side == 'S':
                price_levels = [i for i in list(book.details.mbp[order_iter.orders_symbol][BUY].keys()) if
                        i >= order_iter.orders_price]
                # booksize = sum([i[0] for pri in price_levels for i in book.details.mbo[order_iter.orders_symbol][BUY][pri].values()])

              while (execution == 'not completed') and (price_level_counter < len(price_levels)):
                level_price = list(book.details.mbp[order_iter.orders_symbol][crossed_side].keys())[
                  price_level_counter]
                level_size = book.details.mbp[order_iter.orders_symbol][crossed_side][level_price]
                if level_size >= order_size:
                  executed_size = order_size
                  execution = 'completed'
                  self.order_response_full_execution(this_epoch_nano,algono,order_iter.orders_symbol,order_iter.orders_intorderid,
                                                         order_iter.orders_side,
                                                         level_price,
                                                         executed_size,
                                                         order_iter.orders_type)

                elif level_size < order_size:
                  executed_size = level_size
                  order_size -= level_size
                  price_level_counter += 1
                  self.order_response_partial_execution(this_epoch_nano,
                    algono,
                    order_iter.orders_symbol,
                    order_iter.orders_intorderid,
                    order_iter.orders_side,
                    level_price,
                    executed_size,
                    order_iter.orders_type)
            else:
              self.order_response_rejection(this_epoch_nano, algono,order_iter.orders_symbol,
                                       order_iter.orders_intorderid,
                                       order_iter.orders_side,
                                       order_iter.orders_price,
                                       order_iter.orders_size,
                                       order_iter.orders_type)

          elif order_iter.orders_type == 'fok':
            level_size = list(book.details.mbp[order_iter.orders_symbol][crossed_side].values())[0]
            level_price = list(book.details.mbp[order_iter.orders_symbol][crossed_side].keys())[0]

            if order_size <= level_size:
              executed_size = order_size
              self.order_response_full_execution(this_epoch_nano,algono,order_iter.orders_symbol,
                                                     order_iter.orders_intorderid,
                                                     order_iter.orders_side,
                                                     level_price,
                                                     executed_size,
                                                     order_iter.orders_type)
            else:
              self.order_response_rejection(this_epoch_nano, algono,order_iter.orders_symbol,
                                       order_iter.orders_intorderid,
                                       order_iter.orders_side,
                                       order_iter.orders_price,
                                       order_iter.orders_size,
                                       order_iter.orders_type)

          elif order_iter.orders_type in ['gtd', 'gtc']:
            if order_iter.orders_message in ['cancel', 'replace']:
              # If original order is in the orders array
              if order_iter.orders_sendintorderid in self.mm.mm_orderid.values:
                if self.orders[self.orders['orders_intorderid'] == order_iter.orders_sendintorderid].orders_message.values[0] == 'accepted':
                  old_price = self.mm[self.mm['mm_orderid'] == order_iter.orders_sendintorderid].mm_price.values[0]
                  self.order_limit(book, order_iter)

                  ##### Delete Original Replace/Original Cancel/ and if Cancel Delete Current Cancel Order #####
                  self.order_response_cancellation(this_epoch_nano, algono, order_iter.orders_symbol,
                                   order_iter.orders_sendintorderid,
                                   order_iter.orders_side,
                                   old_price,
                                   order_iter.orders_size,
                                   order_iter.orders_type)

                  if (order_iter.orders_message == 'replace'):
                    self.orders.at[self.orders[self.orders['orders_intorderid'] == order_iter.orders_intorderid].index, 'orders_message'] = 'accepted'  # Changing the message of both orders (prev and replace)
                    self.order_response_accept(this_epoch_nano, algono, order_iter.orders_symbol,
                                   order_iter.orders_intorderid,
                                   order_iter.orders_side,
                                   order_iter.orders_price,
                                   order_iter.orders_size,
                                   order_iter.orders_type)
              # If original order is NOT in the orders array
              else:
                # print(' Original Cancel or Replace order is not in the MM Array')
                self.order_response_rejection(this_epoch_nano, algono, order_iter.orders_symbol,
                                order_iter.orders_sendintorderid,
                                order_iter.orders_side,
                                order_iter.orders_price,
                                order_iter.orders_size,
                                order_iter.orders_type)

            elif (order_iter.orders_message != 'cancel') and \
                (((order_iter.orders_side == 'B') and (order_iter.orders_price >= tob_ask_price)) or \
                 ((order_iter.orders_side == 'S') and (order_iter.orders_price <= tob_bid_price))):
              level_price = list(book.details.mbp[order_iter.orders_symbol][crossed_side].keys())[0]
              level_size = list(book.details.mbp[order_iter.orders_symbol][crossed_side].values())[0]
              if order_size <= level_size:  # Full Execution
                executed_size = order_size
                self.order_response_full_execution(this_epoch_nano,algono,order_iter.orders_symbol,
                                                       order_iter.orders_intorderid,
                                                       order_iter.orders_side,
                                                       level_price,
                                                       executed_size,
                                                       order_iter.orders_type)

              else:  # Partial Execution
                executed_size = level_size
                self.order_response_partial_execution(this_epoch_nano,algono,order_iter.orders_symbol,order_iter.orders_intorderid,
                                                        order_iter.orders_side,
                                                        level_price,
                                                        executed_size,
                                                        order_iter.orders_type)

            elif order_iter.orders_message == 'new':
              self.order_limit(book, order_iter)
              self.order_response_accept(this_epoch_nano, algono, order_iter.orders_symbol,
                order_iter.orders_intorderid,order_iter.orders_side,order_iter.orders_price,
                order_iter.orders_size,order_iter.orders_type)

              self.orders.at[self.orders[self.orders['orders_intorderid'] == order_iter.orders_intorderid].index, 'orders_message'] = 'accepted'


          try:
            self.orders.at[self.orders[self.orders['orders_intorderid'] == order_iter.orders_intorderid].index.values[0], 'orders_message'] = 'accepted'
          except:
            pass

  def order_limit(self, book, order_info):
    '''
    limit_price --> limit order price
    limit_size    --> limit order size
    limit_message    --> limit order message (c_new=2 , c_replace=3)
    limit_multiplier  --> limit order symbol multiplier
    '''
    algono = order_info.orders_algono
    symbol = order_info.orders_symbol
    side = order_info.orders_side
    price = order_info.orders_price
    size = order_info.orders_size
    eventtime = order_info.orders_eventtime
    id = order_info.orders_intorderid
    message = order_info.orders_message
    tob_bid_price, tob_ask_price = aux.take_bid_ask_price(book, symbol)
    book_side = (BUY if side == 'B' else SELL)
    prev_order_id = -99

    if message in ['cancel', 'replace']:
      prev_order_id = order_info.orders_sendintorderid
      prev_message = self.mm[(self.mm['mm_orderid'] == prev_order_id)]
    if (prev_order_id in self.mm.mm_orderid.values) or (message == 'new'):
      if (message == 'new') or ((message == 'replace') and ((price != prev_message.mm_price.values[0]) or (
          (price == prev_message.mm_price.values[0]) & (
          size >= prev_message.mm_size.values[0])))):  ##### New Order OR ORDER SIZE INCREASED OR PRICE CHANGED #####

        if ((side == 'B') and (price > tob_bid_price)) or ((side == 'S') and (price < tob_ask_price)):
          # all_orderahead, all_sizeahead, limit_sizeahead, limit_orderahead, all_priceahead = 0, 0, 0, 0, 0
          limit_sizeahead, limit_orderahead, all_priceahead = 0, 0, 0

          queue = pd.DataFrame(columns=['id', 'quantity'])
        else:
          if price in list(book.details.mbp[symbol][book_side].keys()):
            limit_orderahead = len([i for i in book.details.mbo[symbol][book_side][price].keys()])
            limit_sizeahead = book.details.mbp[symbol][book_side][price]
          else:
            limit_sizeahead, limit_orderahead = 0, 0

          queue, all_priceahead = self.create_queue(book, symbol, side, price)
          # all_orderahead, all_sizeahead = len(queue), sum(queue.quantity)
          # all_priceahead = len([i for i in book.details.mbo[symbol][book_side][price].keys()])

        if message == 'replace':  # delete previous order row
          self.mm = self.mm[self.mm['mm_orderid'] != prev_order_id]
          # self.mm = self.mm.drop(prev_message.index.values[0], axis='index')
        # if len(self.mm) == 0:
        #   new_index = 0
        # elif len(self.mm) > 0:
        #   new_index = self.mm.index.max() + 1
        self.mm = self.mm.sort_index().reset_index(drop=True)
        # self.mm.at[len(self.mm)] = [eventtime, algono, symbol, side, price, size, order_info.orders_type, message, id,
        #           all_orderahead, all_sizeahead, limit_orderahead, limit_sizeahead, queue]

        self.mm.at[len(self.mm)] = np.array([eventtime, algono, symbol, side, price, size, order_info.orders_type, message, id,
                  all_priceahead, limit_orderahead, limit_sizeahead, queue], dtype=object)

      elif (message == 'replace') and (price == prev_message.mm_price.values[0]) and (
          size < prev_message.mm_size.values[0]):  ##### NO PRICE CHANGE AND ORDER SIZE REDUCED #####
        self.mm.at[prev_message.index.values[0], 'mm_size'] = size

      elif message == 'cancel':  # delete previous order row
        # self.mm = self.mm.drop(prev_message.index.values[0], axis='index')
        self.mm = self.mm[self.mm['mm_orderid'] != prev_order_id]
        self.mm = self.mm.sort_index().reset_index(drop=True)

    else:
      print('!!! Order is not in MM Dataframe !!!')
    # return mm

  def que_priority(self, book, row):  # wait_df, orderahead,my_order_price):
    # for algono in list_algono:  # Activate if more than one algo in simulator
    #   row_execution_quantity = row.Quantity
    execution_allowed = True
    row_side = (SELL if row.Side == 'S' else BUY)
    if row.MessageType == 'A': row_price = row.Price
    elif row.MessageType in ['D', 'E']: row_price = book.details.id_to_price[row.mbp_id][row.OrderID, row_side]

    if ((row.Side == 'S') and (row_price <= self.mm[self.mm['mm_side'] == 'S'].mm_price.max())) or \
      ((row.Side == 'B') and (row_price >= self.mm[self.mm['mm_side'] == 'B'].mm_price.min())) :

      for mm_iter in self.mm[(self.mm['mm_symbol'] == row.mbp_id) & (self.mm['mm_side'] == row.Side)].itertuples(index=True):
        mm_iter_index = self.mm[self.mm['mm_orderid'] == mm_iter.mm_orderid].index.values[0]
        mm_order_side = (SELL if mm_iter.mm_side == 'S' else BUY)
        mm_iter_queue = mm_iter.queue
        tob_bid_price, tob_ask_price = aux.take_bid_ask_price(book, mm_iter.mm_symbol)

        # Execution Control
        if (row.MessageType == 'E') & (execution_allowed is True) & (row.Side == mm_iter.mm_side):
          if (mm_iter.all_priceahead <= 0) and ((mm_iter.limit_orderahead <= 0) or
              ((mm_iter.limit_orderahead == 1) & (((mm_iter.mm_side == 'B') and (mm_iter.mm_price > tob_bid_price)) or (
                  (mm_iter.mm_side == 'S') and (mm_iter.mm_price < tob_ask_price))))):
            if row.Quantity >= mm_iter.mm_size:  # Full Execution of MM Order
              self.mm = self.mm[self.mm['mm_orderid'] != mm_iter.mm_orderid]
              self.mm = self.mm.sort_index().reset_index(drop=True)
              self.order_response_full_execution(row.epoch_nano, mm_iter.mm_algono,mm_iter.mm_symbol, mm_iter.mm_orderid,
                                                     mm_iter.mm_side,
                                                     mm_iter.mm_price,
                                                     mm_iter.mm_size,
                                                     mm_iter.mm_type)
            elif row.Quantity < mm_iter.mm_size:  # Partial Execution of MM Order
              self.mm.at[self.mm[self.mm['mm_orderid'] == mm_iter.mm_orderid].index.values[0], 'mm_size'] = mm_iter.mm_size - row.Quantity
              execution_size = row.Quantity
              self.order_response_partial_execution(row.epoch_nano, mm_iter.mm_algono,mm_iter.mm_symbol,mm_iter.mm_orderid,
                                                      mm_iter.mm_side,
                                                      mm_iter.mm_price,
                                                      execution_size,
                                                      mm_iter.mm_type)
            execution_allowed = False
            continue

          # Que Update
        # if ((row.MessageType in ['D', 'E']) & (row.OrderID in mm_iter_queue.id.values)) or ((row.MessageType == 'A') & (row.Price == mm_iter.mm_price)):
        if ((row.MessageType in ['D', 'E']) and (row_price == mm_iter.mm_price)) or \
            ((row.MessageType == 'A') and (((mm_iter.mm_side == 'S') and (row_price <= mm_iter.mm_price)) or ((mm_iter.mm_side == 'B') and (row_price >= mm_iter.mm_price)))):
          # if (row.MessageType in ['D', 'E']) & (row.OrderID in mm_iter_queue.id.values):  # ONEMLI burda price levels karsilastirmasina gerek yok & (book.details.id_to_price[row.mbp_id][(row.OrderID, row_side)] in price_levels)
          if row.MessageType == 'D':
            mm_iter_queue = mm_iter_queue[mm_iter_queue['id'] != row.OrderID]
            mm_iter_queue = mm_iter_queue.reset_index(drop=True)

          elif (row.MessageType == 'E'):# & (len(mm_iter_queue) > 0 & (row.OrderID == mm_iter_queue.id.values[0])):
            if len(mm_iter_queue[mm_iter_queue['id'] == row.OrderID]) > 0:
              if row.Quantity < mm_iter_queue[mm_iter_queue['id'] == row.OrderID].quantity.values[0]:  # Partial Execution of  one of the orders in front
                mm_iter_queue.at[(mm_iter_queue[mm_iter_queue['id'] == row.OrderID].index.values[0]), 'quantity'] -= row.Quantity

              elif row.Quantity == mm_iter_queue[mm_iter_queue['id'] == row.OrderID].quantity.values[0]:  # Full Execution of one of the orders in front
                mm_iter_queue = mm_iter_queue[mm_iter_queue['id'] != row.OrderID]
                mm_iter_queue = mm_iter_queue.reset_index(drop=True)

              else: print('Quantities does not match !!', row)

          elif (row.MessageType == 'A') and (row_price == mm_iter.mm_price):
            if mm_iter.mm_side == 'S':
              price_levels = [i for i in list(book.details.mbp[mm_iter.mm_symbol][mm_order_side].keys()) if i < mm_iter.mm_price]
            elif mm_iter.mm_side == 'B':
              price_levels = [i for i in list(book.details.mbp[mm_iter.mm_symbol][mm_order_side].keys()) if i > mm_iter.mm_price]
            order_rank = len([i for pri in price_levels for i in book.details.mbo[mm_iter.mm_symbol][mm_order_side][pri].keys()]) + len(mm_iter_queue)
            if row.Rank <= order_rank:
              mm_iter_queue.loc[row.Rank - 1.5] = row.OrderID, row.Quantity
              mm_iter_queue = mm_iter_queue.sort_index().reset_index(drop=True)

          self.mm.at[mm_iter_index, 'limit_orderahead'] = len(mm_iter_queue)
          self.mm.at[mm_iter_index, 'limit_sizeahead'] = sum(mm_iter_queue.quantity)
          self.mm.at[mm_iter_index, 'queue'] = mm_iter_queue
          if mm_iter.mm_side == 'S':
            price_levels = len([i for i in list(book.details.mbp[mm_iter.mm_symbol][mm_order_side].keys()) if
                      i <= mm_iter.mm_price])
          elif mm_iter.mm_side == 'B':
            price_levels = len([i for i in list(book.details.mbp[mm_iter.mm_symbol][mm_order_side].keys()) if
                      i >= mm_iter.mm_price])
          self.mm.at[mm_iter_index, 'all_priceahead'] = price_levels - 1

        # Market Crosses Our Order
      for mm_iter in self.mm[(self.mm['mm_symbol'] == row.mbp_id) & (self.mm['mm_side'] != row.Side)].itertuples(index=True):
        tob_bid_price, tob_ask_price = aux.take_bid_ask_price(book, mm_iter.mm_symbol)
        if (row.MessageType == 'A') & \
            (((mm_iter.mm_side == 'S') and (mm_iter.mm_price <= tob_bid_price)) or
             ((mm_iter.mm_side == 'B') and (mm_iter.mm_price >= tob_ask_price))):
          self.mm = self.mm[self.mm['mm_orderid'] != mm_iter.mm_orderid]
          self.mm = self.mm.sort_index().reset_index(drop=True)
          self.order_response_full_execution(row.epoch_nano, mm_iter.mm_algono, mm_iter.mm_symbol,mm_iter.mm_orderid,
                                                 mm_iter.mm_side,
                                                 mm_iter.mm_price,
                                                 mm_iter.mm_size,
                                                 mm_iter.mm_type)

  ### Simulator Auxiliary Functions ###
  def fill_in_orderresponse_array(self, eventtime, algono, symbol, id, side, price, size, responsetype, messagetype):
    '''
    responsetype : ' c_executed = 0, c_accepted = 1 , c_canceled = 2, c_rejected = 3
    c_orders_type : ' c_gtc = 1, c_gtd = 2, c_fak = 3 or c_fok = 4
    '''

    # self.orderresponse.loc[0 if pd.isnull(self.orderresponse.index.max()) else self.orderresponse.index.max() + 1] = [eventtime, algono,
    #                                                  symbol, id, side,
    #                                                  price, size,
    #                                                  responsetype,
    #                                                  messagetype]
    self.orderresponse.loc[len(self.orderresponse)] = [eventtime,algono,
                                                     symbol, id, side,
                                                     price, size,
                                                     responsetype,
                                                     messagetype]
    # return orderresponse

  def fill_in_executions_array(self, eventtime, algono, symbol, price, executed_size, side,id):
    # in_position = True
    # if execution_quantity >= size:
    #   in_position = False
    #   executed_size = size
    #   size = 0
    # else:
    #   size -= execution_quantity
    #   executed_size = execution_quantity
    # self.executions.loc[0 if pd.isnull(self.executions.index.max()) else self.executions.index.max() + 1] = [eventtime, algono, symbol, id,
    #                                               side, price, executed_size]
    self.executions.loc[len(self.executions)] = [
      eventtime, algono, symbol,price, executed_size,side, id]

    # return executions  # , in_position, size

  def fill_in_positions_array(self, algono, side, position_change):
    position_sign = (1 if side == 'B' else -1)
    if len(self.positions) == 0:
      self.positions.at[0] = algono, (position_change) * (position_sign)
    else:
      self.positions.at[(self.positions[self.positions['positions_algono'] == algono].index.values[0]), 'num_positions'] += (
                                                             position_change) * (
                                                             position_sign)

    # return positions

  def order_response_full_execution(self, eventtime, algono, symbol, id, side, price, size, ordertype):
    self.fill_in_orderresponse_array(eventtime, algono, symbol, id, side, price, size, 'executed',ordertype)
    self.fill_in_executions_array(eventtime, algono, symbol,price, size,side, id)
    self.fill_in_positions_array(algono, side, size)
    self.orders.at[self.orders[self.orders['orders_intorderid'] == id].index.values[0], 'executed'] = 'yes'

    # return orderresponse, executions, positions, orders

  def order_response_partial_execution(self, eventtime, algono, symbol, id, side, price, size, ordertype):  # This size is execution size, not order size
    self.fill_in_orderresponse_array(eventtime, algono, symbol, id, side, price, size, 'executed',ordertype)
    self.fill_in_executions_array(eventtime, algono, symbol,price, size,side, id)
    self.fill_in_positions_array(algono, side, size)
    self.orders.at[self.orders[self.orders['orders_intorderid'] == id].index.values[0], 'executed'] = 'partial'

    # return orderresponse, executions, positions, orders

  def order_response_rejection(self, eventtime, algono, symbol, id, side, price, size, ordertype):
    self.fill_in_orderresponse_array(eventtime, algono, symbol, id, side, price, size, 'rejected', ordertype)
    # orders.at[orders[orders['orders_sendintorderid'] == id].index.values[0], 'orders_size'] = 0
    self.orders = self.orders[self.orders['orders_intorderid'] != id]

    # return orderresponse, orders

  def order_response_cancellation(self, eventtime, algono, symbol, id, side, price, size, ordertype):
    self.fill_in_orderresponse_array(eventtime, algono, symbol, id, side, price, size, 'cancel',ordertype)
    # orders.at[orders[orders['orders_sendintorderid'] == id].index.values[0], 'orders_size'] = 0
    self.orders = self.orders[self.orders['orders_intorderid'] != id]
    self.orders.sort_index().reset_index(drop=True)
    # return orderresponse, orders

  def order_response_accept(self, eventtime, algono, symbol, id, side, price, size, ordertype):
    self.fill_in_orderresponse_array(eventtime, algono, symbol, id, side, price, size, 'accepted',
                          ordertype)

    # return orderresponse
