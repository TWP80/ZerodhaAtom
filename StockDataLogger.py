#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 15 23:49:28 2020

@author: harpal
"""

import os
import threading
import datetime
import queue
import time
import pandas as pd

class TickLogger():
    def __init__(self, csv_file_name, tick):
        self.csv_file_name = csv_file_name
        self.pd = pd.DataFrame()
        self.pd = self.pd.append(tick, ignore_index=True)
        if os.path.exists(csv_file_name):
            self.pd = self.pd.append(tick, ignore_index=True)
        else:
            self.pd.to_csv(self.csv_file_name, index = False) 
        self.pd = self.pd[0:0]
        
    def append(self,tick):
        self.pd = self.pd.append(tick, ignore_index=True)
        
    def save(self):
        self.pd.to_csv(self.csv_file_name, mode='a', header=False, index = False) 
        self.pd = self.pd[0:0]
        

class StockLogger(threading.Thread):
    def __init__(self, base_path='/home/harpal/Desktop/StockHistorical/data', chunk_size = 10):
        # Call the Thread class's init function
        threading.Thread.__init__(self)
        self.date = date = datetime.datetime.now().date()
        self.base_path = base_path
        self.ticks_queue = queue.Queue(500)
        if base_path:
            self.base_path = base_path
            
        self.__logger_dict = {'NSE':{},'BSE':{}}      
        self.stop_flag = False
        self.chunk_size = chunk_size
        self.count = 0
        
    
    def __log_into_panda(self,ticks):
        self.count += 1
        #print(self.count)
        for tick in ticks:
            symbol = tick['symbol']
            exchange = tick['exchange']
            if self.__logger_dict[exchange].get(symbol,None):
                self.__logger_dict[exchange][symbol].append(tick)
            else:
                log_path = self.base_path+'/'+exchange+'/'+symbol               
                if not os.path.exists(log_path):
                    os.makedirs(log_path)
                    
                log_file = log_path +'/'+str(self.date)+'.csv' 
                self.__logger_dict[exchange][symbol] = TickLogger(csv_file_name = log_file, tick=tick)
    
    def save_to_files(self):
        for exchange in self.__logger_dict:
            exchange_temp = self.__logger_dict[exchange]
            for tick_logger in exchange_temp:
                exchange_temp[tick_logger].save()
                
    def stop(self):
        self.stop_flag = True
        
    def log_ticks(self,ticks:dict):
        if not self.ticks_queue.full():
            self.ticks_queue.put(ticks)
        else:
            print('Not able to log data')
        
                
    def run(self):
        while True:
            while not self.ticks_queue.empty():
                ticks = self.ticks_queue.get()
                self.__log_into_panda(ticks)
            
            if self.count >= self.chunk_size:
                self.count = 0
                self.save_to_files()
                 
            if self.stop_flag:
                self.save_to_files()
                break
            time.sleep(10)
                
                 
        
                
            
            