import sys, os.path
src_dir = (os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))
sys.path.append(src_dir)
from Side import Side, BUY, SELL
from LobBs import LobBs
import aux as aux

import os
import pytz
import pandas as pd
pd.set_option('display.width', 320)
pd.set_option('display.max_columns', 10)
from datetime import datetime



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
def get_equity_data_new(path, day, symbol) -> pd.DataFrame:
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
  bid_prices_list_f = list(book.details.mbp[BUY].keys())[:no_of_levels]
  ask_prices_list_f = list(book.details.mbp[ SELL].keys())[:no_of_levels]
  bid_quantities_list = list(book.details.mbp[BUY].values())[:no_of_levels]
  ask_quantities_list = list(book.details.mbp[ SELL].values())[:no_of_levels]

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
  #  break
  return book_to_cont, data_collect
def collect_all_epoch_nano_fut_and_eq(data_eq, data_f):
  d1 = data_eq.epoch_nano
  d2 = data_f.epoch_nano
  d_all = set(d1.append(d2))
  return d_all
def find_order_id_to_follow(order, book, order_id):
  side = order.Side.values[0]
  price = order.Price.values[0]
  side_to_follow = BUY if side == 'B' else  SELL if side == 'S' else 'no_act'
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
      if len(list(book.details.mbp[BUY].keys())) > 0:
        bid_price = list(book.details.mbp[BUY].keys())[0]
      else:
        break
      if len(list(book.details.mbp[ SELL].keys())) > 0:
        ask_price = list(book.details.mbp[ SELL].keys())[0]
      else:
        break
      # if ('bid_price' in locals()) and ('ask_price' in locals()):
      if (prev_bid_price != bid_price) or (prev_ask_price != ask_price):
        bid_quantity = list(book.details.mbp[BUY].values())[0]
        ask_quantity = list(book.details.mbp[ SELL].values())[0]
        date_to_collect = datetime.fromtimestamp(row_iter.epoch_nano / 1e9, tz=pytz.timezone('Europe/Istanbul'))
        data_collect.append({'symbol': symbol, 'day': day, 'bid_quantity': bid_quantity, 'bid_price': bid_price,
                    'ask_price': ask_price, 'ask_quantity': ask_quantity,
                    'time': row_iter.epoch_nano, 'date': date_to_collect})
      # print(row_iter)
      prev_bid_price = bid_price
      prev_ask_price = ask_price

  return data_collect
def take_bid_ask_price(book : LobBs, symbol):
  bid_price = list(book.details.mbp[symbol][BUY].keys())[0]
  ask_price = list(book.details.mbp[symbol][ SELL].keys())[0]
  return bid_price, ask_price
def take_bid_ask_size(book,symbol):
  bid_size = list(book.details.mbp[symbol][BUY].values())[0]
  ask_size = list(book.details.mbp[symbol][ SELL].values())[0]
  return bid_size, ask_size
def calculate_bid_ask_ratio(book):
  bid_s = list(book.details.mbp[BUY].values())[0]
  ask_s = list(book.details.mbp[ SELL].values())[0]
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

def prepare_symbol_data(path, day, symbol) -> pd.DataFrame:
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
