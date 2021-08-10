import pandas as pd
pd.options.mode.chained_assignment = None
import numpy as np
import os
from operator import neg
from datetime import datetime
import pytz
# import time
# import math
from collections import OrderedDict
from sortedcontainers import SortedList
# from joblib import Parallel, delayed
# from itertools import combinations
pd.set_option('display.width', 320)
pd.set_option('display.max_columns', 10)

class DetailedLob:
    def __init__(self, n):
        self.mbo = {None: {'bid': {}, 'ask': {}}}  # MBO - most detailed
        self.id_to_price = {None: {}}
        self.sorted_prices = {None: {'bid': SortedList(key=neg), 'ask': SortedList()}}
        self.mbp = {None: {'bid': {}, 'ask': {}}}  # MBP

    def insert_add(self, itch_message):
        side = ('ask' if itch_message.Side == 'S' else 'bid')
        price = itch_message.Price
        order_id = itch_message.OrderID
        quantity = itch_message.Quantity
        rank = itch_message.Rank
        symbol_id = itch_message.mbp_id
        if symbol_id not in self.mbp.keys():
            self.mbo.update({symbol_id: {'bid': {}, 'ask': {}}})  # MBO - most detailed
            self.id_to_price.update({symbol_id: {}})
            self.sorted_prices.update({symbol_id: {'bid': SortedList(key=neg), 'ask': SortedList()}})
            self.mbp.update({symbol_id: {'bid': {}, 'ask': {}}})  # MBP
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
        #     self.mbo.update({symbol_id:{side:{price:{order_id: [quantity,rank]}}}})
        #     self.sorted_prices.update({symbol_id:{'bid': SortedList(key=neg), 'ask': SortedList()}})
        #     self.sorted_prices[symbol_id][side].add(price)
        #     self.id_to_price.update({symbol_id:{(order_id, side): price}})
        #     self.construct_lob(symbol_id, 5000)

    def get_order(self, itch_message):
        side = ('ask' if itch_message.Side == 'S' else 'bid')
        order_id = itch_message.OrderID
        symbol_id = itch_message.mbp_id
        price = self.id_to_price[symbol_id][(order_id, side)]
        return side, price, order_id, symbol_id

    def execution(self, itch_message):
        # side, price, order_id = self.get_order(itch_message)
        side = ('ask' if itch_message.Side == 'S' else 'bid')
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

    def construct_lob(self, symbol, n):
        self.mbp[symbol] = {'bid': {}, 'ask': {}}

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
class LobBs:
    def __init__(self, mbp_id=0):  # part, callback
        self.details = DetailedLob(5000)
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

    def process_relevant_messages(self, raw_itch_message):
        itchMsg = raw_itch_message
        _type = itchMsg.MessageType

        # if _type == 'O':
        #     self.mboState = itchMsg.StateName.strip()
        #     print(self.mboState)
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
        order_side = ('ask' if side == 'S' else 'bid')
        if side == 'S':
            price_levels = len([i for i in list(book.details.mbp[symbol][order_side].keys()) if i < price])
        elif side == 'B':
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
        sendmultiplier    --> symbol multiplier
         sendprice         --> order price
         sendsize          --> order size
         sendside          --> c_bid or c_ask
         sendordertype     --> c_gtc, c_gtd, c_fok or c_fak
         sendordermessage  --> c_cancel, c_new or c_replace
         sendintorderid    --> 0 for c_new, send an internalorderid number for c_replace or c_cancel
         executed           --> yes, no, partial
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
        c_risk_algono         --> algono
        c_risk_intorderid     --> internal order id
        c_risk_result      --> c_accepted (1), c_reject_sample_reason (2,3,4...)
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
        IOC orders    --> 1 - executes fully
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
                    #     orderresponse = orderresponse[orderresponse['orderresponse_id'] != order_iter.orders_sendintorderid]  # drop the order in the loop
                    # side = ('bid' if order_iter.orders_side == 'B' else 'ask')
                    tob_bid_price, tob_ask_price = aux.take_bid_ask_price(book, order_iter.orders_symbol)
                    crossed_side = ('bid' if order_iter.orders_side == 'S' else 'ask')
                    order_size = order_iter.orders_size
                    execution = 'not completed'
                    price_level_counter = 0

                    if order_iter.orders_type == 'fak':
                        ##### CROSS THE BID-ASK SPREAD #####

                        if ((order_iter.orders_side == 'B') and (order_iter.orders_price >= tob_ask_price)) or (
                                (order_iter.orders_side == 'S') and (order_iter.orders_price <= tob_bid_price)):
                            if order_iter.orders_side == 'B':
                                price_levels = [i for i in list(book.details.mbp[order_iter.orders_symbol]['ask'].keys()) if
                                                i <= order_iter.orders_price]
                                # booksize = sum([i[0] for pri in price_levels for i in book.details.mbo[order_iter.orders_symbol]['ask'][pri].values()])
                                # booksize = sum([i[0] for pri in price_levels for i in book.details.mbo[order_iter.orders_symbol]['ask'][pri].values()])

                            elif order_iter.orders_side == 'S':
                                price_levels = [i for i in list(book.details.mbp[order_iter.orders_symbol]['bid'].keys()) if
                                                i >= order_iter.orders_price]
                                # booksize = sum([i[0] for pri in price_levels for i in book.details.mbo[order_iter.orders_symbol]['bid'][pri].values()])

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
        limit_size        --> limit order size
        limit_message        --> limit order message (c_new=2 , c_replace=3)
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
        book_side = ('bid' if side == 'B' else 'ask')
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
                #     new_index = 0
                # elif len(self.mm) > 0:
                #     new_index = self.mm.index.max() + 1
                self.mm = self.mm.sort_index().reset_index(drop=True)
                # self.mm.at[len(self.mm)] = [eventtime, algono, symbol, side, price, size, order_info.orders_type, message, id,
                #                     all_orderahead, all_sizeahead, limit_orderahead, limit_sizeahead, queue]

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
        #     row_execution_quantity = row.Quantity
        execution_allowed = True
        row_side = ('ask' if row.Side == 'S' else 'bid')
        if row.MessageType == 'A': row_price = row.Price
        elif row.MessageType in ['D', 'E']: row_price = book.details.id_to_price[row.mbp_id][row.OrderID, row_side]

        if ((row.Side == 'S') and (row_price <= self.mm[self.mm['mm_side'] == 'S'].mm_price.max())) or \
            ((row.Side == 'B') and (row_price >= self.mm[self.mm['mm_side'] == 'B'].mm_price.min())) :

            for mm_iter in self.mm[(self.mm['mm_symbol'] == row.mbp_id) & (self.mm['mm_side'] == row.Side)].itertuples(index=True):
                mm_iter_index = self.mm[self.mm['mm_orderid'] == mm_iter.mm_orderid].index.values[0]
                mm_order_side = ('ask' if mm_iter.mm_side == 'S' else 'bid')
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
        #                                                                                                    symbol, id, side,
        #                                                                                                    price, size,
        #                                                                                                    responsetype,
        #                                                                                                    messagetype]
        self.orderresponse.loc[len(self.orderresponse)] = [eventtime,algono,
                                                                                                       symbol, id, side,
                                                                                                       price, size,
                                                                                                       responsetype,
                                                                                                       messagetype]
        # return orderresponse

    def fill_in_executions_array(self, eventtime, algono, symbol, price, executed_size, side,id):
        # in_position = True
        # if execution_quantity >= size:
        #     in_position = False
        #     executed_size = size
        #     size = 0
        # else:
        #     size -= execution_quantity
        #     executed_size = execution_quantity
        # self.executions.loc[0 if pd.isnull(self.executions.index.max()) else self.executions.index.max() + 1] = [eventtime, algono, symbol, id,
        #                                                                                           side, price, executed_size]
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

class aux:
    def create_order(epoch_nano, MessageType, mbp_id, Side, Price, what, Quantity, OrderID):
        data = [[0, epoch_nano, MessageType, mbp_id, Side, Price, what, Quantity, OrderID]]
        new_order = pd.DataFrame(data, columns=['_', 'epoch_nano', 'MessageType', 'mbp_id', 'Side', 'Price', 'what', 'Quantity',
                                                'OrderID'])
        return new_order
    def create_books(df, epoch_to_stop):
        i = 0
        a = LobBs()
        for row in df.itertuples(index=False):
            if row.epoch_nano <= epoch_to_stop:
                a.process_relevant_messages(row)
            else:
                break
        return a
    def continue_books(book_to_cont, data_eq_or_fut, epoch_to_start, epoch_to_stop):
        data = data_eq_or_fut[data_eq_or_fut['epoch_nano'] > epoch_to_start]
        for row in data.itertuples(index=False):
            if row.epoch_nano <= epoch_to_stop:
                book_to_cont.process_relevant_messages(row)
            else:
                break
        return book_to_cont
    def append_books(book, order):
        for row in order.itertuples(index=False):
            book.process_relevant_messages(row)
        # return book
    def get_equity_data(path, day, symbol):
        path_eq = path + '/' + day + '/eq/' + day + '_' + symbol + '.csv'
        data_eq = pd.read_csv(path_eq, header=None)
        column_names = ['_', 'epoch_nano', 'MessageType', 'mbp_id', 'Side', 'Price', 'Rank', 'Quantity', 'OrderID']
        try:
            data_eq.columns = column_names
        except:
            print(symbol, day, 'ERROR: Column mismatch')
        return data_eq
    def get_equity_data_new(path, day, symbol):
        path_eq = path + '/' + day + '/eq/' + day + '_EQU_' + symbol + '.csv'
        data_eq = pd.read_csv(path_eq, header=None)
        column_names = ['_', 'epoch_nano', 'MessageType', 'mbp_id', 'Side', 'Price', 'Rank', 'Quantity', 'OrderID']
        try:
            data_eq.columns = column_names
        except:
            print(symbol, day, 'ERROR: Column mismatch')
        return data_eq

    def get_future_data(path, day, symbol):
        path_fut = path + '/' + day + '/fut/' + day + '_' + symbol + '.csv'
        data_fut = pd.read_csv(path_fut, header=None)
        column_names = ['_', 'epoch_nano', 'MessageType', 'mbp_id', 'Side', 'Price', 'Rank', 'Quantity', 'OrderID']
        try:
            data_fut.columns = column_names
        except:
            print(symbol, day, 'ERROR: Column mismatch')
        return data_fut
    def get_eq_equilibrium_price(symbol, month, date):
        trade_path = '/home/umut/cefis2/lidya_opening/data_' + month + '/trade/' + symbol + '_trade.csv'
        dat_trade = pd.read_csv(trade_path)
        dat_trade['date'] = dat_trade['date_time'].str.split('T').str[0]
        equity_equilibrium_price = dat_trade[dat_trade['date'] == date].price.values[0]
        return equity_equilibrium_price
    def create_first_orders_of_eq(book, no_of_levels, theo_price_diff_bid, theo_price_diff_ask):  # No equity info before 9.55
        bid_prices_list_f = list(book.details.mbp['bid'].keys())[:no_of_levels]
        ask_prices_list_f = list(book.details.mbp['ask'].keys())[:no_of_levels]
        bid_quantities_list = list(book.details.mbp['bid'].values())[:no_of_levels]
        ask_quantities_list = list(book.details.mbp['ask'].values())[:no_of_levels]

        bid_prices_list_eq_order = [round((bid_price_f - theo_price_diff_bid), 2) for bid_price_f in bid_prices_list_f]
        ask_prices_list_eq_order = [round((ask_price_f - theo_price_diff_ask), 2) for ask_price_f in ask_prices_list_f]

        bid_order_dict = dict(zip(bid_prices_list_eq_order, bid_quantities_list))
        ask_order_dict = dict(zip(ask_prices_list_eq_order, ask_quantities_list))

        return bid_order_dict, ask_order_dict
    def detect_changes_in_books(book_to_cont, data_eq_or_fut, epoch_to_start, epoch_to_stop, side):
        data = data_eq_or_fut[
            (data_eq_or_fut['epoch_nano'] > epoch_to_start) & (data_eq_or_fut['epoch_nano'] <= epoch_to_stop)]
        data_collect = []
        prev_price = 0
        i = 0
        for row in data.itertuples(index=False):
            # if row.epoch_nano <= epoch_to_stop:
            book_to_cont.process_relevant_messages(row)
            first_price = list(book_to_cont.details.mbp[side].keys())[0]
            if first_price != prev_price:
                price_to_collect = first_price
                quantity_to_collect = list(book_to_cont.details.mbp[side].values())[0]
                time_to_collect = row.epoch_nano
                date_to_collect = datetime.fromtimestamp(row.epoch_nano / 1e9, tz=pytz.timezone('Europe/Istanbul'))
                data_collect.append({'symbol': symbol, 'price': price_to_collect,
                                     'quantity': quantity_to_collect, 'time': time_to_collect, 'date': date_to_collect})
            prev_price = first_price
        # else:
        #    break
        return book_to_cont, data_collect
    def collect_all_epoch_nano_fut_and_eq(data_eq, data_f):
        d1 = data_eq.epoch_nano
        d2 = data_f.epoch_nano
        d_all = set(d1.append(d2))
        return d_all
    def find_order_id_to_follow(order, book, order_id):
        side = order.Side.values[0]
        price = order.Price.values[0]
        side_to_follow = 'bid' if side == 'B' else 'ask' if side == 'S' else 'no_act'
        index_to_use = list(book.details.mbo[side_to_follow][price]).index(order_id) - 1
        id_to_follow = list(book.details.mbo[side_to_follow][price])[index_to_use]
        return side, price, id_to_follow
    def detect_tob_changes(data, symbol, day):
        data_collect = []
        prev_bid_price = 0
        prev_ask_price = 0
        epoch_start = data.epoch_nano.values[0]
        i = 0
        book = LobBs()
        for row_iter in data.itertuples(index=False):
            # if row.epoch_nano <= epoch_to_stop:
            book.process_relevant_messages(row_iter)
            if row_iter.epoch_nano > epoch_start:
                if len(list(book.details.mbp['bid'].keys())) > 0:
                    bid_price = list(book.details.mbp['bid'].keys())[0]
                else:
                    break
                if len(list(book.details.mbp['ask'].keys())) > 0:
                    ask_price = list(book.details.mbp['ask'].keys())[0]
                else:
                    break
                # if ('bid_price' in locals()) and ('ask_price' in locals()):
                if (prev_bid_price != bid_price) or (prev_ask_price != ask_price):
                    bid_quantity = list(book.details.mbp['bid'].values())[0]
                    ask_quantity = list(book.details.mbp['ask'].values())[0]
                    date_to_collect = datetime.fromtimestamp(row_iter.epoch_nano / 1e9, tz=pytz.timezone('Europe/Istanbul'))
                    data_collect.append({'symbol': symbol, 'day': day, 'bid_quantity': bid_quantity, 'bid_price': bid_price,
                                         'ask_price': ask_price, 'ask_quantity': ask_quantity,
                                         'time': row_iter.epoch_nano, 'date': date_to_collect})
                # print(row_iter)
                prev_bid_price = bid_price
                prev_ask_price = ask_price

        return data_collect
    def take_bid_ask_price(book, symbol):
        bid_price = list(book.details.mbp[symbol]['bid'].keys())[0]
        ask_price = list(book.details.mbp[symbol]['ask'].keys())[0]
        return bid_price, ask_price
    def take_bid_ask_size(book,symbol):
        bid_size = list(book.details.mbp[symbol]['bid'].values())[0]
        ask_size = list(book.details.mbp[symbol]['ask'].values())[0]
        return bid_size, ask_size
    def calculate_bid_ask_ratio(book):
        bid_s = list(book.details.mbp['bid'].values())[0]
        ask_s = list(book.details.mbp['ask'].values())[0]
        bid_ask_r = bid_s / ask_s
        return bid_ask_r
    def get_day_list(path):
        eq_list = [root for root, f, d in os.walk(path) if root.endswith('eq')]
        day_list = [p.split('/')[-2] for p in eq_list]
        return sorted(day_list)
    def get_underlying_symbol_list(day_iter, day_list, index_symbol, month_iter):
        if day_list[0] == day_iter:
            day_to_read = day_iter
        else:
            day_to_read = day_list[day_list.index(day_iter) - 1]
        # day_to_read = day_iter

        path_to_weigths = '/home/umut/index_strategy/bist_index_weights/' + str(
            month_iter) + '/Endeks Ağırlık Düzeltme Sonrası/endeks_agirlik_ds_genel_' + day_to_read + '.csv'

        # path_to_weigths = '/home/umut/index_strategy/bist_index_weights/Nov19/Endeks Ağırlık Düzeltme Öncesi/endeks_agirlik_do_genel_' + day_to_read + '.csv'
        index_weigths = pd.read_csv(path_to_weigths, sep=';', skiprows=1)
        index_weigths = index_weigths[index_weigths['INDEX CODE'] == index_symbol]
        index_weigths['symbol'] = index_weigths['EQUITY CODE'].str.split('.').str[0]
        return index_weigths.symbol.values
    def get_tick_size(price):
        if price < 20:
            tick_size = 1
        elif price < 50:
            tick_size = 2
        elif price < 100:
            tick_size = 5
        else:
            tick_size = 10
        return tick_size
    def data_merger(path, day, symbol_list):
        merged = pd.DataFrame()
        for symbol_iter in symbol_list:
            df = aux.get_equity_data(path, day, symbol_iter)
            df['index'] = df.index
            merged = merged.append(df)
            merged.sort_values(['epoch_nano', 'index'], inplace=True)
            merged = merged.reset_index(drop=True)
        return merged

    def prepare_symbol_data(path, day, symbol):
        data = aux.get_equity_data_new(path, day, symbol)
        data['date'] = [datetime.fromtimestamp(i / 1e9, tz=pytz.timezone('Europe/Istanbul')) for i in data.epoch_nano]
        data['day'] = day
        data['hour'] = data['date'].dt.hour
        data['minute'] = data['date'].dt.minute
        data['second'] = data['date'].dt.second
        data['microsecond'] = data['date'].dt.microsecond
        # data['prev_second'] = data.second.shift(1)
        data['next_epoch_nano'] = data.epoch_nano.shift(-1)
        data = data[data['hour'] < 18]
        return data

day_iter = '20210730'
month_iter = str(day_iter)[:6]
year = str(month_iter)[:4]
path = '/root/shared/TEST_raw_data/TEST_raw_csv_bist/' + year + '/' + str(month_iter) + '/'
symbol_iter = 'GARAN'
data = aux.prepare_symbol_data(path,day_iter,symbol_iter)

algono = 1
in_position = False
algo_open = False
circuit_breaker = False
order_id_entry, order_id_exit = -1, -1

book = LobBs()
sim = simulatorV10()

for row_iter in data.iloc[:, :].itertuples(index=True):
    book.process_relevant_messages(row_iter)

    if algo_open is True:
        try:
            bid_price, ask_price = aux.take_bid_ask_price(book, symbol_iter)
            bid_size, ask_size = aux.take_bid_ask_size(book, symbol_iter)
            tick_size = aux.get_tick_size(ask_price)
        except:
            continue
        # Que Priority
        if len(sim.mm) > 0:
            sim.que_priority(book, row_iter)

        # Listening Orderresponse
        if len(sim.orderresponse) > 0:
            if len(sim.orderresponse[sim.orderresponse['orderresponse_id'] == order_id_entry]) > 0:
                if sim.orderresponse[sim.orderresponse['orderresponse_id'] == order_id_entry].ordersresponse_type.values[0] == 'executed': # Which means entry order is executed
                    sim.send_order(row_iter, algono, symbol_iter, 'S', ask_price, 1, 'gtd', 'new')
                    order_id_exit = sim.risk_check['intorderid']

            if len(sim.orderresponse[sim.orderresponse['orderresponse_id'] == order_id_exit]) > 0:
                if sim.orderresponse[sim.orderresponse['orderresponse_id'] == order_id_exit].ordersresponse_type.values[0] == 'executed': # Which means exit order is executed
                    print('Success')
                    in_position = False

        # Trigger Condition
        if in_position == False and (bid_size/ask_size < 0.50):
            sim.send_order(row_iter, algono, symbol_iter, 'B', bid_price, 1, 'gtd','new')
            order_id_entry = sim.risk_check['intorderid']
            in_position = True


    # ############## Algo Supervisor ##################
    # BEGINNING OF DAY
    if (algo_open is False) and (circuit_breaker is False) and row_iter.Rank == -14 and row_iter.hour == 10:
        algo_open = True

    # CIRCUIT BREAKER - SESSION MESSAGES
    if row_iter.MessageType == 'O':
        if circuit_breaker is False and row_iter.Rank > -14 and row_iter.Rank < -1:
            # print(count,'CIRCUIT BREAKER STARTED', count, symbol_iter, day_iter)
            circuit_breaker = True
            algo_open = False

        if circuit_breaker is True and row_iter.Rank == -14:
            # print(count,'CIRCUIT BREAKER FINISHED', count, symbol_iter, day_iter)
            circuit_breaker = False
            algo_open = True

    # END OF DAY
    if (algo_open is True) and (row_iter.hour == 17 and row_iter.minute >= 55):
        # print(count,'End of Day')
        algo_open = False

        # END OF DAY CANCELING OUTSTANDING MM ORDERS
        for mm_row in sim.mm.itertuples(index=True):
            if (mm_row.mm_size > 0):
                sim.send_order(row_iter, algono, symbol_iter, mm_row.mm_side, 0, 0, 'gtd', 'cancel',
                               mm_row.mm_orderid)
        # print(count,'Canceling Positions', sim.positions)
        if ((len(sim.positions) > 0) and (sim.positions[sim.positions['positions_algono'] == algono].num_positions.values[0] < 0)):
            sim.send_order(row_iter, algono, symbol_iter, 'B', ask_price + 5 * tick_size / 100,
                           -sim.positions[sim.positions['positions_algono'] == algono].num_positions.values[0],
                           'fak', 'new')
        elif ((len(sim.positions) > 0) and (sim.positions[sim.positions['positions_algono'] == algono].num_positions.values[0] > 0)):
            sim.send_order(row_iter, algono, symbol_iter, 'S', bid_price - 5 * tick_size / 100,
                           sim.positions[sim.positions['positions_algono'] == algono].num_positions.values[0],
                           'fak', 'new')

    if (len(sim.orders)) > 0:
        sim.order_handler(book, row_iter)

print(sim.orders)
print(sim.positions)
print(sim.executions)
