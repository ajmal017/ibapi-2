import os, glob
import pandas as pd


class Handler():
    def __init__(self):
        self.final_df = pd.DataFrame()
    
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
                        if self.df.shape[0] == 390:
                            self.final_df = pd.concat([self.final_df, self.df],ignore_index=True)
                        else: 
                            x = self.df.shape[0]
                            print("*****the data is not complete for {} {} {} {} and only has {} bars*****".format( ticker,year, month, day, x))
            self.final_df.rename(columns = {'Unnamed: 0':'minute'} ,inplace = True) 
    
    def sample_df(self, ticker=None):
        if ticker:
            years = os.listdir('/data/'+ ticker +'/')
            just_one_year = 0
            for year in years:
                if just_one_year < 1:
                    just_one_year += 1
                    print(year)
                    months = os.listdir('/data/'+ ticker+'/'+ year +'/')
                    just_one_month = 0
                    for month in months:
                        if just_one_month < 1:
                            just_one_month +=1
                            days = os.listdir('/data/'+ ticker+'/'+ year +'/'+ month + '/')
                            for day in days:
                                df = pd.read_csv('/data/'+ ticker+'/'+ year +'/'+ month + '/'+ day+'/df.csv')
                                self.make_df(ticker=ticker , year=year, month=month, day=day)
                                if self.df.shape[0] == 390:
                                    self.final_df = pd.concat([self.final_df, self.df],ignore_index=True)
                                else: 
                                    x = self.df.shape[0]
                                    print("*****the data is not complete for {} {} {} {} and only has {} bars*****".format( ticker,year, month, day, x))
            self.final_df.rename(columns = {'Unnamed: 0':'minute'} ,inplace = True) 

            return self.final_df
    
    def rolling_average(self, *windows):
        for window in windows:
            self.final_df['mean'] = self.final_df[['Open','close','high','low']].mean(axis=1)
            colName = '{}rolling'.format(window)
            self.final_df[colName] = self.final_df['mean'].rolling(window= window).mean()
    
    

# handler = Handler()
# handler.full_df(ticker='INTC')
# handler.final_df.to_csv('/data/test.csv')

# print(handler.final_df)

dfs = pd.read_csv('/data/full/INTC/df.csv', chunksize=390)
for df in dfs:
    print(df.__str__())

# handler.sample_df(ticker='INTC')


# handler.rolling_average(10,45, 90, 180, 360)


# print(handler.final_df)