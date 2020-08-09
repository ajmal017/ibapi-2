import os, glob
import pandas as pd


class DataHandler():
    def __init__(self):
        self.final_df = pd.DataFrame()
        self.index_removal_list = list()
        
    
    def __str__(self):
        return self.final_df

    def make_df(self, ticker, year, month, day, ):
        directory = '/data/'+ticker+'/'+year+'/'+month+'/'+day+'/df.csv'
        self.df = pd.read_csv(directory)
    
    def full_df(self, ticker=None):
        if ticker:
            years = os.listdir('/data/'+ ticker +'/')
            for year in years:
                print(year)
                months = os.listdir('/data/'+ ticker+'/'+ year +'/')
                for month in months:
                    days = os.listdir('/data/'+ ticker+'/'+ year +'/'+ month + '/')
                    for day in days:
                        df = pd.read_csv('/data/'+ ticker+'/'+ year +'/'+ month + '/'+ day+'/df.csv')
                        self.make_df(ticker=ticker , year=year, month=month, day=day)

                        if self.df.shape[0] != 390:
                            x = self.df.shape[0]
                            print("*****the data is not complete for {} {} {} {} and only has {} bars*****".format( ticker,year, month, day, x))
                            for value in range(self.final_df.index[-1], self.df.shape[0] + self.final_df.index[-1]):
                                self.index_removal_list.append(value )
                        self.final_df = pd.concat([self.final_df, self.df],ignore_index=True)
                        
                        
                        
                        # if self.df.shape[0] == 390:
                        #     self.final_df = pd.concat([self.final_df, self.df],ignore_index=True)
                        # else: 
                        #     x = self.df.shape[0]
                        #     print("*****the data is not complete for {} {} {} {} and only has {} bars*****".format( ticker,year, month, day, x))
            self.final_df.rename(columns = {'Unnamed: 0':'minute'} ,inplace = True) 
    
    def sample_df(self, ticker=None):
        if ticker:
            years = os.listdir('/data/'+ ticker +'/')
            just_one_year = 0
            for year in years:
                if just_one_year < 1:
                    just_one_year += 1
                    months = os.listdir('/data/'+ ticker+'/'+ year +'/')
                    just_one_month = 0
                    for month in months:
                        if just_one_month < 1:
                            just_one_month += 1
                            days = os.listdir('/data/'+ ticker+'/'+ year +'/'+ month + '/')
                            for day in days:
                                df = pd.read_csv('/data/'+ ticker+'/'+ year +'/'+ month + '/'+ day+'/df.csv')
                                self.make_df(ticker=ticker , year=year, month=month, day=day)
                                if self.df.shape[0] != 390:
                                    x = self.df.shape[0]
                                    print("*****the data is not complete for {} {} {} {} and only has {} bars*****".format( ticker,year, month, day, x))
                                    for value in range(self.final_df.index[-1], self.df.shape[0] + self.final_df.index[-1]):
                                        self.index_removal_list.append(value )
                                    print(self.index_removal_list)
                                self.final_df = pd.concat([self.final_df, self.df],ignore_index=True)


            self.final_df.rename(columns = {'Unnamed: 0':'minute'} ,inplace = True) 

            return self.final_df
    
    def rolling_average(self, *windows):
        for window in windows:
            self.final_df['mean'] = self.final_df[['Open','close','high','low']].mean(axis=1)
            colName = '{}rolling'.format(window)
            self.final_df[colName] = self.final_df['mean'].rolling(window= window).mean()
    
    def drop_holidays(self):
        print('including holidays {}-minutes'.format(handler.final_df.shape[0]))
        self.final_df.drop(self.index_removal_list, axis=0, inplace=True)
        print('droped {} ,minutes'.format(len(self.index_removal_list)))
        print('droping holidays holidays {}-minutes remain'.format(handler.final_df.shape[0]))
    
    def format_datetime_col(self):
        print(self.final_df['date'][0])
        self.final_df['date'] =  pd.to_datetime(self.final_df['date'], format='%Y%m%d  %H:%M:%S')
    
    def bar_feed(self, ticker=False, sample=False):
        if not ticker: # when no ticker is defined use final_df
            for index, row in self.final_df.iterrows():
                yield row
        if ticker:
            if sample:
                temp_df = pd.read_csv('/data/sample/{}/df.csv'.format(ticker))
            if not sample:
                temp_df = pd.read_csv('/data/full/{}/df.csv'.format(ticker))

            for index, row in temp_df.iterrows():
                yield row



handler = DataHandler()
# handler.full_df(ticker='INTC')


# #handler.full_df(ticker="INTC")

# handler.rolling_average(10,45, 90, 180, 360)
# handler.drop_holidays()
# handler.format_datetime_col()
for bar in handler.bar_feed('INTC'):
    print(bar['date'])
    pass


# handler.final_df.to_csv('/data/sample/INTC/df.csv')

# print(handler.final_df)

# dfs = pd.read_csv('/data/sample/INTC/df.csv', chunksize=390)
# for df in dfs:
#     print(df.__str__())

# handler.sample_df(ticker='INTC')


# handler.rolling_average(10,45, 90, 180, 360)


# print(handler.final_df)