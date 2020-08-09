import datetime
import time
import threading

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract


import pandas as pd
import pytz
import os


class HistoricalIBapi(EWrapper, EClient):
    """
    Used to send request and recive responses from interactive brokers gateway
    Contracts represent assets of various classes. Call historical_data_request 
    with list of tickers:str, month:int, year:int, day:int
    """

    def __init__(self, directory, tickers, year, month, day):
        EClient.__init__(self, self)
        # self.tickers = ['VCIT']#'VCIT','VCSH','TIP','LQD','UCO','CVX',,]#'SLV','LAC','WFC','GOOGL', 'BYND', 'XLE',  'DIA'] #'GDX',

        self.directory = directory
        self.tickers = tickers
        self.year = year
        self.month = month
        self.day = day

        self.bars = list()
        self.ticker_dict = dict()
        self.contracts = dict()  # list of ibapi contract objects
        # create connection to IB gateway in a seprate thread
        self.__create_conection()
        # fill the self.contracts list
        self.__create_contracts()

        # unique reqId inital value
        self.id = 1
        # create dates contracts where added in list
        self.contract_added_date = dict()
        self.__date_added_reference()
        time.sleep(len(tickers)*2)
        # start the loading  into the directory
        self.historical_data_request(
            month=self.month, year=self.year, day=self.day)

    def historical_data_request(self, month, year, day):
        # function the user calls with start dates
        for date_str in self.__generate_dates(month=self.month, year=self.year, day=self.day):
            date = datetime.datetime.strptime(date_str, '%Y%m%d %H:%M:%S')
            if date.weekday() < 5:  # only send request for weekdays
                for ticker in self.contracts:
                    if date > self.contract_added_date[ticker]:
                        if  os.path.exists('/data/{}/{}/{:02d}/{:02d}/df.csv'.format(ticker, date.year, date.month, date.day)) == False:
                            self.id = self.__reqid()
                            print('reqid ', self.id,' contract ticker ', ticker)
                            # add reqId:ticker to dict
                            self.ticker_dict[self.id] = ticker
                            # male historic request to connection
                            time.sleep(10)
                            print('made it ' ,date_str)
                            self.reqHistoricalData(self.id, self.contracts[ticker], date_str,
                                                   "1 D", "1 min", "Trades", 1, 1, False, [])

    ###################   RECIVER WRAPPER FUNCTIONS SECTION ######################
    ########## historical data is recived here and minipulated #########
    def historicalData(self, reqId, bar):
        ticker = self.ticker_dict[reqId]
        # format bar data and append to self.bars list
        one_bar = {
            'ticker': ticker,
            'date': bar.date,
            'Open': bar.open,
            'high': bar.high,
            'low': bar.low,
            'close': bar.close,
            'volume': bar.volume,
        }
        self.bars.append(one_bar)

    def historicalDataEnd(self, reqId, start, end):
        ticker = self.ticker_dict[reqId]
        df = pd.DataFrame(self.bars)
        self.bars = []
        # make folders to store values
        date_string = df['date'][0]
        element_list = date_string.split()
        date = element_list[0]
        year, month, day = date[0:4], date[4:6], date[-2:]
        path = self.__make_dir_string(ticker, year, month, day) + '/df.csv'
        # fill missing values before saving
        df.to_csv(path)
        del df

    def headTimestamp(self, reqId, headTimestamp):
        ticker = self.ticker_dict[reqId]
        print('{} added on {}'.format(ticker, headTimestamp))
        date = datetime.datetime.strptime(headTimestamp, '%Y%m%d %H:%M:%S')
        self.contract_added_date[ticker] = date

    ####################################################################################
    def __create_conection(self):
        self.connect('127.0.0.1', 7497, 130)
        api_thread = threading.Thread(target=self.__run_loop, daemon=True)
        api_thread.start()

    def __reqid(self):
        # makes unique ticker for reqId
        self.id += 1
        return self.id

    def __create_contracts(self):
        for ticker in self.tickers:
            _contract = Contract()
            _contract.symbol = ticker
            _contract.secType = 'STK'
            _contract.exchange = 'SMART'
            _contract.currency = 'USD'

            self.contracts[ticker] = _contract

    def __run_loop(self):
        self.run()

    def __generate_dates(self, month, year, day):
        date_list = []  # list of datetime objects for end of each day
        current_datetime = datetime.datetime.today().replace(
            hour=0, minute=0 ,second=0, microsecond=0)
        request_from_date = datetime.datetime(year, month, day)
        timedelta_request_range = current_datetime - request_from_date
        for day in range(timedelta_request_range.days):
            a_date = current_datetime - datetime.timedelta(days=day)
            yield a_date.strftime("%Y%m%d %H:%M:%S")

    def __date_added_reference(self):
        for _contract in self.contracts:
            self.id = self.__reqid()
            self.ticker_dict[self.id] = self.contracts[_contract].symbol
            time.sleep(2)
            self.reqHeadTimeStamp(
                self.id, self.contracts[_contract], "TRADES", 1, 1)

    def __make_dir_string(self, ticker, year, month, day):
        path = "/data"
        path = path + '/' + ticker
        path = path + '/' + year
        path = path + '/' + month
        path = path + '/' + day
        try:
            os.makedirs(path)
        except OSError:
            print("Creation of the directory failed")
            return path
        else:
            print("Successfully created the directory %s " % path)
            return path


r = HistoricalIBapi(directory='/data',
                    tickers=['AMZN', 'WFC'],
                    month=12,
                    year=2019,
                    day=10)
