#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr  2 17:41:46 2020
@author: harpal
shiftbybit@gmail.com
"""

import time
import datetime as dt
import threading
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver import ActionChains
#from selenium.common.exceptions import NoSuchElementException 
import pandas as pd
from io import StringIO 
from bs4 import BeautifulSoup

class None_Web_Eelement:
    __instance = None
    def __init__(self):
        if None_Web_Eelement.__instance != None:
            raise Exception("Sigleton Class")
        else:
            self.text = None
            None_Web_Eelement.__instance = self
    def __bool__(self):
        return False
    def getInstance():
        """ Static access method. """
        if None_Web_Eelement.__instance == None:
            None_Web_Eelement()
        return None_Web_Eelement.__instance
    def click(self):
        print('Element:Not Found')
        
    def find_element_by_xpath(self,text):
        print('Parent Element of :'+ text +' Not Found')
        return self
    
class ZC:
    MAX_NO_WATCHLIST = 5
    # Available tick subscribtion modes.
    MODE_DEPTH_20 = "depth_20"
    MODE_DEPTH_5 = "depth_5"
    MODE_LTP = "ltp"
    
    #Order Parameter
    TRANSACTION_TYPE_BUY = "Buy (B)"
    TRANSACTION_TYPE_SELL = "Sell (S)"
    PRODUCT_TYPE_CNC = "Cash and carry. Delivery based trades"
    PRODUCT_TYPE_MIS = "Intraday squareoff with extra leverage"
    ORDER_TYPE_MARKET = "Market"
    ORDER_TYPE_LIMIT = "Limit"
    
    
    #Page reference
    login_url = "https://kite.zerodha.com/"
    xpath_username = "//*[@id='container']/div/div/div/form/div[2]/input"
    xpath_pswd = "//*[@id='container']/div/div/div/form/div[3]/input"
    xpath_pin = "//*[@id='container']/div/div/div/form/div[2]/div/input"
        
    
class ZerodhaConnect(threading.Thread):
    def __init__(self,driver = None, usr=None, pswd=None, pin = None):
        # Call the Thread class's init function
        threading.Thread.__init__(self)
        #Elements References
        
        self.ticker_element = None
        #Init valriables
        self.driver =  driver
        self.driver.get(ZC.login_url)
        self.driver.maximize_window()
       
        ''' Login Procedure '''
        if usr:        
            self.driver.find_element_by_xpath(ZC.xpath_username).send_keys(usr)
            self.driver.find_element_by_xpath(ZC.xpath_pswd).send_keys(pswd+Keys.ENTER)
            time.sleep(1)
            self.driver.find_element_by_xpath(ZC.xpath_pin).send_keys(pin+Keys.ENTER)
        else:
            self.wait_for_login()
            
        ''' Left Container elements '''
        time.sleep(5)
        
        self.el_left_container = self.driver.find_element_by_class_name('container-left')
        self.el_watchlist_selector = self.el_left_container.find_element_by_class_name('marketwatch-selector')
        self.wachlists = self.el_watchlist_selector.find_elements_by_tag_name('li')
        self.mode =  ZC.MODE_LTP # default mode
        
        #self.stock_elements = {}
        # Callbacks
        self.on_ticks = None
        self.sleep_time = 1

    def wait_for_login(self):
        while True:
            time.sleep(5)
            if "dashboard" in self.driver.current_url:
                break;
        print('Login Successfully')
            
    def subscribe(self,wlist_index = 0, time_interval = 5,mode='ltp'):
        if wlist_index < 1 or wlist_index > 5:
            raise Exception('Invalid wlist_index should be in range of 1 to 5')
        self.selected_wlist = wlist_index
        self.wachlists[wlist_index-1].click()
        self.time_interval = time_interval
        open_watchlist =  self.el_left_container.find_element_by_class_name("vddl-list")
        el_all_ins = open_watchlist.find_elements_by_class_name("instrument")
        self.instruments_count = len(el_all_ins)
        self.mode = mode
        ticker_map = {'NSE':{},'BSE':{}}
        for el in el_all_ins:
            symbol =  ZerodhaConnect.find_el_by_class_name(el,'nice-name').text
            exchange =  ZerodhaConnect.find_el_by_class_name(el,'exchange').text
            if exchange is None:
                exchange = 'NSE'
            ticker_map[exchange][symbol] = el
            
        self.ticker_element = ticker_map
        if self.mode == ZC.MODE_DEPTH_5 or mode == ZC.MODE_DEPTH_20:
            for el in el_all_ins:
                time.sleep(1)
                self.driver.execute_script("arguments[0].scrollIntoView();", el)
                hover = ActionChains(self.driver).move_to_element(el)
                hover.perform()
                
                #time.sleep(0.3)
                action = ZerodhaConnect.find_el_by_class_name(el,'actions')
                action.find_element_by_xpath("//button[@data-balloon='Market Depth (D)']").click()
                #continue
                if mode == ZC.MODE_DEPTH_20:
                    time.sleep(1)
                    try:
                        dp_toggle = ZerodhaConnect.find_el_by_class_name(el,'depth-toggle')
                        self.driver.execute_script("arguments[0].scrollIntoView();", dp_toggle)
                        dp_toggle.click()
                    except Exception:
                        print('Depth 20 Error')
            
    def place_order(self,symbol = None,
                    exchange= None, 
                    product = ZC.PRODUCT_TYPE_CNC,
                    transaction_type =ZC.TRANSACTION_TYPE_BUY,
                    order_type = ZC.ORDER_TYPE_MARKET,
                    price = None, 
                    qtn = 1 ):
        try:
            el = self.ticker_element[exchange][symbol]
        except Exception:
            print('Element not found for order')
            return False
        
        try:
            self.driver.execute_script("arguments[0].scrollIntoView();", el)
            hover = ActionChains(self.driver).move_to_element(el)
            hover.perform()
            action = el.find_element_by_class_name('actions')       
            action.find_element_by_xpath("//button[@data-balloon='"+transaction_type+"']").click()
            order_window = self.driver.find_element_by_class_name("order-window")
            # Default order Type in Zerodha is Market
            #time.sleep(0.1)
            if product == ZC.PRODUCT_TYPE_MIS:
                order_window.find_element_by_xpath("//div[@data-balloon='"+product+"']").click()
            if order_type == ZC.ORDER_TYPE_LIMIT:   
                order_type = order_window.find_element_by_xpath("//div[@data-balloon='"+ order_type+"']")
                order_type.click()
                price_form = order_window.find_element_by_xpath("//input[@label='Price']") 
                price_form.clear()
                price_form.clear()
                if price is None:
                    return False
                price_form.send_keys(str(price))

            order_qtn = order_window.find_element_by_xpath("//input[@label='Qty.']")
            print(order_qtn.text)
            order_qtn.clear()
            order_qtn.send_keys(str(qtn) + Keys.ENTER)
        except Exception as e:
            print('Order Error:', e)
            return False
        
           
    def run(self):
        time.sleep(1)
        while True:
            if self.on_ticks:
                #start_time = int(round(time.time() * 1000))
                start_time = time.time()
                timeStemp =dt.datetime.now()
                
                src_code = self.el_left_container.get_attribute('innerHTML')
                soup =  BeautifulSoup(src_code,'lxml')
                
                
                instruments = soup.find_all('div', class_='instrument')
                active_list = int(self.get_soup_text(soup,'li','item selected'))
                
                if self.selected_wlist != active_list or self.instruments_count != len(instruments):
                    print('change on browser')
                    self.subscribe(wlist_index= active_list,mode=self.mode,time_interval=self.time_interval)
                    continue
                #Get Tick Data
                ticks = self.get_ticks_data(instruments)                       
                self.on_ticks(ticks, timeStemp)
                escaped_time = time.time() -  start_time
 
                if escaped_time < self.time_interval:
                    time.sleep(self.time_interval - escaped_time)
                                       
                
    def get_ticks_data(self,instruments):
        ticks = []
        for el in instruments:
            tickdata = {}
            tickdata['symbol'] = self.get_soup_text(el,'span','nice-name')
            exchange = self.get_soup_text(el,'span','exchange')
            if exchange is None:
                exchange = 'NSE'
            tickdata['exchange']=exchange
            tickdata['holdings'] =  self.get_soup_text(el,'span','holding-quantities')
            tickdata['ltp'] =  self.get_soup_text(el,'span','last-price')
            tickdata['change'] =  self.get_soup_text(el,'span','change-percent')
            tickdata['ltp'] =  self.get_soup_text(el,'span','last-price')
            
            mk_dp = el.find('div', class_='market-depth')
            if mk_dp:
                #Get OHLC Data
                ohlc_table = mk_dp.find('div', class_='ohlc').find_all('div', class_='six columns')
                for data in ohlc_table:
                    tickdata[data.label.text] = data.span.text
                    
                    #Get Bids Table
                    bids_table = el.find('table', class_='six columns buy')
                    #bids_pd = pd.DataFrame(columns = ['bid', 'orders', 'qty'])
                    #self.get_table_in_pd(bids_pd,bids_table)
                    total_bids = self.get_soup_text(bids_table.tfoot,'td','text-right')
                    #tickdata['bid_table'] = bids_pd
                    tickdata['total_bids'] = total_bids
                    
                    #Get Offers Table
                    offer_table = el.find('table', class_='six columns sell')
                    #offer_pd = pd.DataFrame(columns = ['bid', 'orders', 'qty'])
                    #self.get_table_in_pd(offer_pd,offer_table)
                    total_offers = self.get_soup_text(bids_table.tfoot,'td','text-right')
                    #tickdata['offer_table'] = offer_pd
                    tickdata['total_offers'] = total_offers 
            ticks.append(tickdata)
        return ticks
        
    def get_margins(self):
        self.driver.find_element_by_partial_link_text('Funds').click()
        time.sleep(0.1)
        container =  self.driver.find_element_by_class_name('container-right')
        html_src = container.get_attribute('innerHTML')
        soup =  BeautifulSoup(html_src,'lxml')
        rows = soup.find_all('div', class_='six columns')
        
        margins = {}
        for row in rows:
            data = margins[next(row.h3.stripped_strings)] = {}
            table = row.find('table', class_="table")
            items = table.find_all('tr')
            for item in items:
                tds = item.find_all('td')
                data[tds[0].text.strip()] =tds[1].text.strip()
        return margins
    
    def get_holdings(self):
        self.driver.find_element_by_partial_link_text('Holdings').click()
        time.sleep(0.1)
        container =  self.driver.find_element_by_class_name('container-right')
        html_src = container.get_attribute('innerHTML')
        
        keys = []
        Holdings = {}
        soup =  BeautifulSoup(html_src,'lxml')
        table = soup.find('table')        
        heads = table.thead.tr.find_all('th')
        for item in heads:
            keys.append(item.text.strip())
            
        data_items =table.tbody.find_all('tr')
            
        for tds in data_items:
            values = tds.find_all('td')
            holding = {}
            symbol = values[0].text.strip()
            for i in range(len(values)):
                holding[keys[i]] =values[i].text.strip()
                Holdings[symbol] = holding
                
        return Holdings 
            
    @staticmethod
    def get_table_in_pd(pd_table,el_table):
        rows = el_table.tbody.find_all('tr')
        for i in range(len(rows)):
            row = rows[i]
            row_data = []
            tds = row.find_all('td')
            for data in tds:
                row_data.append(data.text.strip())
            pd_table.loc[i] = row_data
        
    @staticmethod
    def get_soup_text(soup:BeautifulSoup=None, tag=None, class_name=None):
        item = soup.find(tag, class_ = class_name)
        if item:
            try:
                data = item.text.strip()
                return data
            except Exception:
                return item.text 
        return None
        
    @staticmethod
    def find_el_by_class_name(el,class_name):
        try:
            return el.find_element_by_class_name(class_name)
        except Exception:
            return None_Web_Eelement.getInstance()




