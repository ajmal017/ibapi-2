import datetime
import time
import threading

from ibapi.client import EClient
from ibapi.wrapper import EWrapper  
from ibapi.contract import Contract


import pandas as pd
import pytz


class LiveIBapi(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self) 
        self.tickers = ['WFC','VCIT','VCSH'] #,'TIP','LQD','UCO','CVX','GOLD','SLV','LAC','WFC','GOOGL', 'BYND', 'XLE',  'DIA'] #'GDX',
       
        # self.bars = list()
        # self.ticker_dict = dict()
        self.contracts = list() # list of ibapi contract objects
        # create connection to IB gateway in a seprate thread
        self.create_conection()
        # fill the self.contracts list
        self.create_contracts()
        #unique reqId inital value
        self.id = 1

        #####LIVE DATA VARS#######
        self.current_min = datetime.datetime.now(tz=pytz.utc).replace(microsecond=0,second=0)
        
        self.live_bars = dict()
        self.make_live_bars()
    
    def create_conection(self):
        self.connect('127.0.0.1', 7497, 130)
        api_thread = threading.Thread(target=self.run_loop, daemon=True)
        api_thread.start()
        

    ######### live data functions
    def tickPrice(self, reqId, tickType, price, attrib):
        #print(reqId, tickType, price, attrib)  
        save_time = self.current_min + datetime.timedelta(minutes=1)
        if datetime.datetime.now(tz=pytz.utc) >= save_time:
            if tickType == 4:
                self.set_close_date(reqId=reqId, item=price)
                print(self.live_bars)
                self.update_live_bars(self.ticker_dict[reqId])
                
        else:
            self.make_bar(reqId=reqId,tickType=tickType, item=price)

    def tickSize(self, reqId, tickType, size):
        self.make_bar(reqId=reqId, tickType=tickType, item=size)
        print(reqId, size)
 

    ################   REQUEST SELCTION ##########################
    def make_bar(self, reqId, tickType, item):
        ticker = self.ticker_dict[reqId]
        if tickType == 8: # volume
            self.live_bars[ticker]['volume'] += item
        elif tickType == 4: # price
            if self.live_bars[ticker]['Open'] == 0:
                self.live_bars[ticker]['Open'] = item
            if self.live_bars[ticker]['high'] <= item or self.live_bars[ticker]['high']==0:
                self.live_bars[ticker]['high'] = item
            if self.live_bars[ticker]['low'] == 0 or self.live_bars[ticker]['low'] >= item:
                self.live_bars[ticker]['low'] = item
         
    def set_close_date(self, reqId,  item):
        ticker = self.ticker_dict[reqId]
        self.live_bars[ticker]['close'] = item
        self.current_min = datetime.datetime.now(tz=pytz.utc).replace(microsecond=0,second=0)
        self.live_bars[ticker]['date'] = self.current_min

    def make_live_bars(self):
        for ticker in self.tickers:
            self.live_bars[ticker] = {
                'symbol':ticker,
                'Open':0,
                'close':0,
                'high':0,
                'low':0,
                'volume':0,
                'date': None
            }
        self.current_min = datetime.datetime.now(tz=pytz.utc).replace(microsecond=0,second=0)
    
    def update_live_bars(self,ticker):
        self.live_bars[ticker]['volume'] = 0
            
        self.current_min = datetime.datetime.now(tz=pytz.utc).replace(microsecond=0,second=0)

    def reqid(self):
        # makes unique ticker for reqId 
        self.id += 1
        return self.id
        

    def create_contracts(self):
        for ticker in self.tickers:
            _contract = Contract()
            _contract.symbol = ticker
            _contract.secType = 'STK'
            _contract.exchange = 'SMART'
            _contract.currency = 'USD'
            self.contracts.append(_contract)
            


    def run_loop(self):
        self.run()

    def subscribe_live(self):
        for contract in self.contracts:
            self.id = self.reqid()
            self.ticker_dict[self.id] = contract.symbol 
            time.sleep(10)
            self.reqMktData(self.id , contract,"", False, False, [])



r = LLive_IBapi()
#r.historical_data_request(ticker='BTC',month=7,year=2020,day=10)
r.subscribe_live()


















