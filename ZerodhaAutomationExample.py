#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 12 14:15:45 2020
@author: harpal
@email : shiftbybit@gmail.com
"""
from selenium import webdriver
from ZerodhaAtom import ZC, ZerodhaConnect
import time
import json

chrome_options = None
headless =  False
if headless:
    chrome_options = webdriver.ChromeOptions()
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

z = ZerodhaConnect(driver =  driver, **user_credential)                                    

#Callback method will be called at fixed interval and will give the tick data of active watchlist
def on_ticks(ticks,time_stemp):
    #print('Time Stamp:',time_stemp)
    #print(ticks)
    
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
z.subscribe(wlist_index = 1,time_interval = 1,mode = ZC.MODE_LTP)


#Start Thread to collect the data form Zerodha Web Page
z.start()

#Join Main Thread
z.join()