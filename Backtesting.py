import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import LoadHistoryData as getData
from BacktestEngine import BacktestEngine
from Strategy import Strategy
import warnings
warnings.filterwarnings('ignore')

#setting
assetList = ['Asset'+ str(i) for i in range(1,28)] #asset 1:27
start = datetime(1992,3,1)
end = datetime(2017,10,1)
setting = {'balance':1_000_000,'assetList':assetList,'commission':0.001,'start':start,'end':end,"advancedBar":50}
backtest = BacktestEngine(setting)

#data
#read data from csv file
data = pd.read_excel('Data.xlsx',index_col=0)
data.index = pd.to_datetime(data.index)
data = data.sort_index(ascending=True)
backtest.setStrategy(Strategy(backtest)) #should be above setDatabase
backtest.setDatabase(data)
#read data from database
# db = getData.getDataBase('AShares')
# backtest.setStrategy(Strategy(backtest)) #should be above setDatabase
# backtest.setDatabase(db,fromDB=True)

#backtest
backtest.runBacktesting()
backtest.showResult()
backtest.plot()
