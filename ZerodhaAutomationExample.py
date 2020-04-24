#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 12 14:15:45 2020
@author: harpal
@email : shiftbybit@gmail.com
"""
from selenium import webdriver
from ZerodhaAtom import ZC, ZerodhaConnect
from StockDataLogger import StockLogger
from whatappAtom import WhatsApp  #Send Log Massage on Whatspp
from MarketSimulator import TickSimulator
from signal import signal, SIGINT
from sys import exit
import threading

import time
import json
import datetime as dt


def handler(signal_received, frame):
    # Handle any cleanup here
#    chatter.stop()
    z.stop()
    hs_logger.stop()

signal(SIGINT, handler)


chrome_options = chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--disable-notifications")
headless =  True
if headless:
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--proxy-server='direct://'")
    chrome_options.add_argument("--proxy-bypass-list=*")
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--ignore-certificate-errors')

driver = webdriver.Chrome(chrome_options=chrome_options)

'''
Put the credential in following format in credential_file
{"usr":"AB1234", "pswd":"abcdef@1234',"pin":"123456"}
'''

#Get the crederntial from following Json File change the path if required
credential_file = '/home/harpal/Desktop/credential_file.json'
with open(credential_file, 'rb') as input:
    user_credential = json.load(input)

today = dt.datetime.today()
market_start_time = dt.datetime.combine(today, dt.time(9, 15, 0))
market_stop_time = dt.datetime.combine(today, dt.time(15, 29, 59))
z = ZerodhaConnect(driver =  driver, **user_credential)                                   
hs_logger = StockLogger(chunk_size = 60)
#chatter= WhatsApp()

#Callback method will be called at fixed interval and will give the tick data of active watchlist
def on_ticks(ticks):
    timestemp = dt.datetime.combine(today,ticks[0]['timestamp'].time())
    #Market Open
    if timestemp >= market_start_time and timestemp <= market_stop_time:
        hs_logger.log_ticks(ticks)
        print('\r' + str(timestemp), end='  ')
        
    #Market Closed
    if timestemp > market_stop_time:
        #chatter.stop()
        hs_logger.stop()
        z.stop()
    #chatter.send_massage(str(ticks))

    print(dt.datetime.now())
  
    
    '''
    #Example of placing order:-
    
    z.place_order(symbol='YESBANK',exchange='NSE',
                  product=ZC.PRODUCT_TYPE_CNC,
                  transaction_type = ZC.TRANSACTION_TYPE_BUY,
                  order_type=ZC.ORDER_TYPE_MARKET, qtn=100)
    
    #Example for getting Margin detail:
    margins = z.get_margins()
    print(margins)
    
    #Example for getting Margin detail:
    holdings = z.get_holdings()
    print(holdings)
    '''

z.on_ticks = on_ticks 

# Sucbscribe tick data from watchlist marker    
z.subscribe(wlist_index = 1,time_interval = 1,mode = ZC.MODE_DEPTH_5)


#Start Thread to collect the data form Zerodha Web Page
z.start()
hs_logger.start()
#chatter.start()

#Join Main Thread
z.join()
hs_logger.join()
#chatter.join()
