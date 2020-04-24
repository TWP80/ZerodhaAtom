#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 22 20:43:15 2020

@author: harpal
"""

from whatappAtom import WhatsApp
from MarketSimulator import TickSimulator
from signal import signal, SIGINT
from sys import exit
import threading

import time
import json
import datetime as dt
import pandas as pd

def handler(signal_received, frame):
    # Handle any cleanup here
    tick_sim.stop()
    
signal(SIGINT, handler)
timeStamp =dt.datetime.now().replace(microsecond=0)
stocks =['SBICARD']
tick_sim = TickSimulator(time_interval = 0.1)
tick_sim.subscribe(tickers = stocks, last_n_days = 2)

'''
Sample Tick for Reference
[{'Avg. price': 527.0, 'Close': 525.9, 'High': 527.0, 'LTQ': 1, 
  'LTT': '2020-04-22 09:07:06', 'Low': 527.0, 'Lower circuit': 420.75, 
  'Open': 527.0, 'Upper circuit': 631.05, 'Volume': '5,115', 'change': '0.21 %', 
  'exchange': 'NSE', 'holdings': 39, 'ltp': 527.0, 'symbol': 'SBICARD', 
  'timestamp': '2020-04-22 09:12:52.628599', market_stop_time
  'total_bids': '1,28,987', 'total_offers': '1,28,987'}]
'''

today = dt.datetime.today()
market_start_time = dt.datetime.combine(today, dt.time(9, 15, 0))
market_stop_time = dt.datetime.combine(today, dt.time(15, 29, 59))

def on_ticks(ticks):   
    for tick in ticks:
        timestemp = dt.datetime.combine(today,tick['timestamp'].time())
        time_diff = (timestemp-market_start_time).total_seconds()       
        print('.', end = '')
        if timestemp >= market_start_time and timestemp <= market_stop_time:
            #Market Open time Only
            print('\r' + str(time_diff), end='  ')
            #tick_sim.place_order(exchange =tick['exchange'], symbol = tick['symbol'] )

tick_sim.on_ticks = on_ticks    
tick_sim.start()
tick_sim.join()

 
  
