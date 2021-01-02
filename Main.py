# -*- coding: utf-8 -*-
"""
Created on Wed Dec 16 16:50:42 2020

@author: Work

creating a test data handler

LAST LEFT OFF ON MOVING AVERAGES NOT PERFECTLY MATCHING EXPECTING MOVING AVERAGES
ON T3 and DF, semi fixed setting adjust to False and increasing data pts. its
accurate enough now. doesnt seem to impact signal dates anymore but diff still
exists in the 2nd MA (1st is perfect)

minor difference remaining, now noticable issue is items coming out of sequence
solved difference by removing multiprocessing for queue and using queue module instead

orders are not filling at the right time [done by adding remove order function]

miscalculating how much to purchase under 'MAX' order, resultiung in neg cash

last left off: need some way to cancel SL orders after they are no longer required
(prob best to do so after the close when signals are calculated)
"""
import pandas as pd
import yfinance as yf
from queue import Queue
import numpy as np
import math

############
# list of events

class Event:
    pass


class CloseEvent(Event):
    def __init__(self, timestamp, price):
        self.timestamp = timestamp
        self.price = price

class OrderEvent(Event):
    def __init__(self, timestamp, order_action, order_reason, order_type, quantity, price, force):
        self.timestamp = timestamp
        self.quantity = quantity
        self.order_type = order_type
        self.order_reason = order_reason #to close, to open
        self.order_action = order_action
        self.price = price
        self.force = force
 
class SignalEvent(Event):
    def __init__(self, timestamp, signal_type):
        self.timestamp = timestamp
        self.signal_type = signal_type
        
class OpenEvent(Event):
    def __init__(self, timestamp, price):
        self.timestamp = timestamp
        self.price = price

class FillEvent(Event):
    def __init__(self, timestamp, quantity, action, fill_cost, comission = 0):
        self.timestamp = timestamp
        self.quantity = quantity
        self.action = action
        self.fill_cost = fill_cost
        self.comission = comission

######################
#df for personal reference, checking work and accuracy
# twindow = 20
# df = yf.Ticker('SPY').history(period = "1y")
# date_list = df.index.tolist() #changing indexing, reformatting, getting rid of useless data
# date_list = [pd.to_datetime(i).date() for i in date_list] 
# df.index = date_list
# df.index.rename('Date', inplace = True)
# df.drop(columns =['Volume', 'Dividends', 'Stock Splits', 'High', 'Low'], inplace = True)
# df['MA1'] = df.Close.ewm(span = twindow, min_periods = twindow, adjust = False).mean()
# df['MA2'] = df.Close.ewm(span = twindow*4, min_periods = twindow*4, adjust = False).mean()
# df['Difference'] = df.MA1 - df.MA2
# df["Position"] = np.nan
# df["Position"].mask(df.Difference >= 0, "long", inplace = True)
# df["Position"].mask(df.Difference < 0, "short", inplace = True)
# df["Signal"] = df.Position == df.Position.shift()
# #df = df[df.Signal != True]

######################

class DataHandler():
    def __init__(self, event_queue, symbol):
        self.event_queue = event_queue
        self.symbol = symbol
        
        self.symbol_data = None
        self.latest_symbol_data = []
        
        self.continue_test =  True
        
        self.initialize()
        self.gen = self.generator()
        
        
    def initialize(self):
        df = yf.Ticker(self.symbol).history(period = "4mo")
        date_list = df.index.tolist() #changing indexing, reformatting, getting rid of useless data
        date_list = [pd.to_datetime(i).date() for i in date_list] 
        df.index = date_list
        df.index.rename('Date', inplace = True)
        df.drop(columns =['Volume', 'Dividends', 'Stock Splits', 'High', 'Low'], inplace = True)
        self.symbol_data = df
        #print(self.symbol_data.head())
    
    def generator(self):
        for index, value in self.symbol_data.iterrows():
            yield tuple([index, 'OPEN', value.Open])
            yield tuple([index, 'CLOSE', value.Close])
    
    def update_data(self):
        try:
            data = next(self.gen)
            #print(data)
            if data is not None and len(data) > 0:
                self.latest_symbol_data.append(data)
                if data[1] == 'OPEN':
                    self.event_queue.put(OpenEvent(data[0], data[2]))
                elif data[1] == 'CLOSE':
                    self.event_queue.put(CloseEvent(data[0], data[2]))
        except StopIteration:
            self.continue_test = False
            #print("DataHandler out of data")
    
    def get_latest_data(self, days = 128):
        return self.latest_symbol_data[-days*2: ]
            
#############################################
class Strategy():
    def __init__(self, event_queue, data):
        self.data = data
        self.event_queue = event_queue
        self.symbol = self.data.symbol
        self.bought = False
        self.prev_signal = None
        
    def calculate_signal(self, event, window): #Exp Mov Avg of window and win*4
        if isinstance(event, CloseEvent):
            
            data = self.data.get_latest_data()
            df = pd.DataFrame(data, columns = ['Timestamp', 'Event', 'Price'])
            df.index = df.Timestamp
            df = df[df.Event != 'OPEN']
            df['MA1'] = df.Price.ewm(
                span = window, min_periods = window, adjust = False).mean()
            df['MA2'] = df.Price.ewm(
                span = window*4 , min_periods = window*4, adjust = False).mean()
            difference = df.MA1.iloc[-1] - df.MA2.iloc[-1]
            latest_date = df.last_valid_index()
            
            if self.bought is False: 
                if difference > 0:
                    signal = SignalEvent(latest_date, 'LONG')
                    self.prev_signal = 'LONG'
                    self.event_queue.put(signal)
                    self.bought = True
                elif difference < 0:
                    signal = SignalEvent(latest_date, 'SHORT')
                    self.prev_signal = 'SHORT'
                    self.event_queue.put(signal)
                    self.bought = True
            
            elif self.bought is True and self.prev_signal == 'SHORT' and difference > 0:
                signal = SignalEvent(latest_date, 'LONG')
                self.prev_signal = 'LONG'
                self.event_queue.put(signal)
            
            elif self.bought is True and self.prev_signal == 'LONG' and difference < 0:
                signal = SignalEvent(latest_date, 'SHORT')
                self.prev_signal = 'SHORT'
                self.event_queue.put(signal)
            #self.test3 = df


##############################################
            
class Portfolio():
    
    def __init__(self, event_queue, data, initial_capital):
        self.event_queue = event_queue
        self.data = data
        self.intial_capital = initial_capital
        
        self.symbol = self.data.symbol
        
        self.current_pos_qty = 0
        self.current_pos_price = 0   #price per qshare #-qty for short stocks
        self.cash = initial_capital
        self.comission = 0 #
        
        self.total = self.current_pos_qty*self.current_pos_price + self.cash -self.comission
        
    
    def update_position_price(self):
        price = self.data.get_latest_data()[-1][2] #keep in mind both c and o are in this
        self.current_pos_price = price
        self.total = self.current_pos_qty*self.current_pos_price + self.cash -self.comission
        
    def create_close_pos_order(self, event): #sends order to close whatever pos is open
        if isinstance(event, SignalEvent):
            if self.current_pos_qty > 0:
                order =  OrderEvent(event.timestamp, 'SELL', 'TO CLOSE', 'MARKET',  self.current_pos_qty, None, 'EOD')
                self.event_queue.put(order)
            elif self.current_pos_qty < 0:
                order =  OrderEvent(event.timestamp, 'BUY', 'TO CLOSE', 'MARKET', -self.current_pos_qty, None, 'EOD')
                self.event_queue.put(order)
            elif self.current_pos_qty == 0:
                pass
    
    def create_open_pos_order(self, event):
        if isinstance(event, SignalEvent):
            direction = event.signal_type
            quantity = 'MAX' #largest possible position at time of fill
            if direction == 'LONG':
                order_action = 'BUY'
            elif direction == 'SHORT':
                order_action = 'SELL'
           
            order_type = 'MARKET'
            
            order = OrderEvent(event.timestamp, order_action, 'TO OPEN', order_type, quantity, None, 'EOD')
            self.event_queue.put(order)               
                
    def post_fill_update(self, event):
        if isinstance(event, FillEvent):
            if event.action == 'BUY':
                self.current_pos_qty += event.quantity
                self.cash -= event.quantity * event.fill_cost
            elif event.action == 'SELL':
                self.current_pos_qty -= event.quantity
                self.cash += event.quantity * event.fill_cost
            if self.current_pos_qty == 0:
                self.current_pos_price = 0
            elif self.current_pos_qty != 0:    
                self.current_pos_price = event.fill_cost
            
            self.comission += event.comission
            self.total = self.current_pos_qty*self.current_pos_price + self.cash -self.comission
            
            
    def create_stoploss_order(self, event, percentage):
        if isinstance(event, FillEvent):
            if self.current_pos_qty != 0:
                fill_cost = event.fill_cost
                if self.current_pos_qty >0:
                    stoploss_price = fill_cost*(1-percentage) #if long, stoploss is lower price
                elif self.current_pos_qty <0:
                    stoploss_price = fill_cost*(1+percentage) #if short, stoploss is higher price
                if event.action == 'BUY':
                    action = 'SELL'
                elif event.action == 'SELL':
                    action = 'BUY'
                order = OrderEvent(event.timestamp, action, 'TO CLOSE', 'STOP',  event.quantity, stoploss_price, 'GTC')
                self.event_queue.put(order)

##############################################
            
class OrderLog():
    def __init__(self, event_queue, data, order_log, portfolio):
        self.event_queue = event_queue
        self.data = data
        self.order_log = order_log
        self.portfolio = portfolio
            
    
    def add_to_log(self, event):
        if isinstance(event, OrderEvent):
            timestamp = event.timestamp
            quantity = event.quantity
            order_type = event.order_type
            order_action = event.order_action
            status = 'OPEN'
            price = event.price
            force = event.force
                
            order = [timestamp, order_action, order_type, quantity, price, force, status]
            self.order_log.append(order)

    def order_to_fill(self, event):
        if isinstance(event, OpenEvent):
            for order in self.order_log:
                if order[2] == 'MARKET': #fill immediately if market order
                    timestamp = self.data.get_latest_data()[-1][0]
                    action = order[1]
                    quantity = order[3]
                    price = self.data.get_latest_data()[-1][2]
                    if quantity == 'MAX':
                        self.portfolio.update_position_price()
                        quantity = math.floor(self.portfolio.total/price)
                    if event.price is None:
                        fill_cost = price
                    elif event.price is not None:
                        fill_cost = event.price
                    fill = FillEvent(timestamp, quantity, action, fill_cost)
                    order[6] = 'FILLED'
                    self.event_queue.put(fill)
                if order[2] == 'STOP':
                    trigger = False
                    price = self.data.get_latest_data()[-1][2]
                    if self.portfolio.current_pos_qty > 0 and order[4] > price: #if long and price is lower than SL
                        trigger = True
                    elif self.portfolio.current_pos_qty <0 and order[4] < price: #if short and price is higher than SL
                        trigger = True
                    
                    if trigger is True:
                        timestamp = self.data.get_latest_data()[-1][0]
                        action = order[1]
                        quantity = order[3]
                        fill_cost = order[4]
                        fill = FillEvent(timestamp, quantity, action, fill_cost)
                        order[6] = 'FILLED'
                        self.event_queue.put(fill)
                            
    
    def remove_filled_orders(self, event):
        if isinstance(event, FillEvent):
            for order in self.order_log:
                if order[6] == 'FILLED':
                    self.order_log.remove(order)
    
    def remove_eod_orders(self, event):
        if isinstance(event, CloseEvent):
            for order in self.order_log:
                if order[5] == 'EOD':
                    self.order_log.remove(order)
                    
    def remove_cancelled_orders(self,event):
        if isinstance(event, CloseEvent):
            for order in self.order_log:
                if order[6] == 'CANCELLED':
                    self.order_log.remove(order)
    
    def cancel_stop_orders(self, event):
        if isinstance(event,SignalEvent):
            for order in self.order_log:
                if order[2] == 'STOP':
                    order[6] == 'CANCELLED'
##############################################
# real loop
event_queue = Queue()
order_log = []

""" 
Put starting capital
test window is how long you want the short moving average to be, long MA is 4x longer
ticker is the ticker symbol for the stock
ignore stopless percentage for now
"""
start_capital = 10000
test_window = 4
ticker = 'GE'
stoploss_percentage = 0.05


data = DataHandler(event_queue, ticker)
strategy = Strategy(event_queue, data)
port = Portfolio(event_queue, data, start_capital)
log = OrderLog(event_queue, data, order_log, port)

while True:
    if data.continue_test is True:
        data.update_data()
    else:
        break
    
    while True:
        try:
            event = event_queue.get(block = False)
        except:
            break
        
        if event is not None:
            if isinstance(event, OpenEvent):
                log.order_to_fill(event) #check order log [NEED IF EMPTY STATEMENT]
                    #create fill event if true from port
                port.update_position_price()
                print(str(event.timestamp) + " AM " + str(round(event.price,2)) +
                      "                  EQUITIES: " + str(round(port.current_pos_qty*port.current_pos_price,2)) +
                      " CASH: " + str(round(port.cash,2)))
                
            elif isinstance(event, CloseEvent):
                strategy.calculate_signal(event, window = test_window) #creates event SIGNAL if true  
                port.update_position_price()#update portfolio
                log.remove_eod_orders(event) #cancel orders that are EOD
                print(str(event.timestamp) + " PM " + str(round(event.price,2)) + 
                      "                  EQUITIES: " + str(round(port.current_pos_qty*port.current_pos_price,2)) +
                      " CASH: " + str(round(port.cash,2)))
                
           
            elif isinstance(event, SignalEvent):
                port.create_close_pos_order(event)#needs updating below
                port.create_open_pos_order(event) #use portfolio info to calculate what the order should be
                print(str(event.timestamp) + " PM SIGNAL " + str(event.signal_type)
                      + " ***************************************")
            
            elif isinstance(event, OrderEvent):
                log.add_to_log(event)#add to order log
                print(str(event.timestamp) + " PM ORDER CREATED: " + str(event.order_type) + " " + 
                      str(event.order_action) + " " + str(event.quantity) + " @ " + 
                      str(event.price))
               
            
            elif isinstance(event, FillEvent):
                log.remove_filled_orders(event) #remove from order log 
                port.post_fill_update(event) #update portfolio
                #port.create_stoploss_order(event, stoploss_percentage) #create stoploss order ONLY IF IT WAS TO OPEN
                #not working atm
                print(str(event.timestamp) + " AM ORDER FILLED: " + str(event.action) + " " + 
                      str(event.quantity) + " @ " + str(round(event.fill_cost,2)) + " E: " 
                      + str(round(port.current_pos_qty*port.current_pos_price,2)) + " C: " +
                      str(round(port.cash,2)))


############################
#test statements

