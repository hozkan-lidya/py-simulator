
# from sortedcontainers import SortedList, SortedSet, SortedDict
# from collections import OrderedDict
# import pandas as pd
import src.aux as aux
from src.SimulatorV10 import simulatorV10
from src.LobBs import LobBs

day_iter = '20210729'
month_iter = str(day_iter)[:6]
year = str(month_iter)[:4]
path = '/root/shared/TEST_raw_data/TEST_raw_csv_bist/' + year + '/' + str(month_iter) + '/'
symbol_iter = 'GARAN'
data = aux.prepare_symbol_data(path,day_iter,symbol_iter)


# TODO: algono is a global variable used in the functions from aux.
# We should seek a way to reformulate it as a local variable
algono = 1
in_position = False
algo_open = False
circuit_breaker = False
order_id_entry, order_id_exit = -1, -1

book = LobBs()
sim = simulatorV10()

for row_iter in data.iloc[:, :].itertuples(index=True):
  book.process_relevant_messages(row_iter)

  if algo_open:
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
