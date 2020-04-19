#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 19 15:38:30 2020

@author: harpal
"""
import os
import time
import threading
import datetime as dt
import pandas as pd


class TickGenerator:
    def __init__(self,data_path=None, exchange = None, symbol=None, start_date= None, end_date=None):
        self.data_path = data_path
        self.exchange = exchange
        self.symbol = symbol
        self.start_date = start_date
        self.end_date = end_date
        
    def __iter__(self):
        return next(self)
    
    def __next__(self):
        for date in self.daterange():
            datafile = self.data_path +'/'+self.exchange+'/'+self.symbol+'/' + str(date) + ".csv"
            if not os.path.exists(datafile):
                continue
            for chunk in pd.read_csv(datafile, chunksize=100):
                index = 0
                while index < chunk.shape[0]:
                    tick =  chunk.iloc[index].to_dict()
                    tick['symbol'] = self.symbol
                    tick['exchange'] = self.exchange
                    print(tick)
                    index += 1
                    yield tick
        
        yield None
            
    
    def daterange(self):
        for n in range(int((self.end_date - self.start_date).days) + 1):
            yield self.start_date + dt.timedelta(n)
            
class TickSimulator(threading.Thread):
    def __init__(self,data_path='/home/harpal/Desktop/StockHistorical/data', 
                 time_interval = 0):
           threading.Thread.__init__(self)
           self.tickers =  None
           self.start_date = dt.datetime.now().date() -dt.timedelta(5)
           self.end_date = dt.datetime.now().date()
           self.data_path = data_path
           self.time_interval = time_interval
           self.tick_genrators = []
           
           # Call banck for tick collection from tick gererator
           self.on_ticks = False
           
    def run(self):
        #Get Tick Gererator for all tickers
        if self.on_ticks:
            #Create Tick generator method for all subscribed tickers
   
            for exchange in ['NSE','BSE']:
                for ticker in self.tickers:
                    if not os.path.exists(self.data_path+ '/'+exchange+'/'+ticker):
                        continue
                    tick_gen = TickGenerator(data_path=self.data_path, exchange=exchange, 
                                             symbol = ticker,start_date = self.start_date, 
                                             end_date=self.end_date)
                    self.tick_genrators.append(iter(tick_gen))
                    
            #Get Tick by tick from tick generator    
            while len(self.tick_genrators) > 0:
                start_time = time.time()
                #Get one tick for each ticker in list
                ticks = []
                for tick_gen in self.tick_genrators.copy():
                    tick = next(tick_gen)
                    if not tick:
                        self.tick_genrators.remove(tick_gen) 
                        continue
                    ticks.append(tick)
                
                
                self.on_ticks(ticks) #Callback Method for ticks
                escaped_time = time.time() -  start_time 
                if escaped_time < self.time_interval:
                    time.sleep(self.time_interval - escaped_time)
                #time.sleep(5)
            
        
    def subscribe(self, start_date = None, end_date = None, tickers:list= None ):
        self.tickers = tickers
        if start_date:
            self.start_date = start_date
        if end_date:
            self.end_date = end_date
        if not self.tickers:
            self.tickers = set()
            for exchange in ['NSE','BSE']:                           
                ticker_list = list(ticker for ticker in os.listdir(self.data_path+ '/'+exchange))
                self.tickers.update(ticker_list)
       
            #print(self.tick_genrators)
                   
