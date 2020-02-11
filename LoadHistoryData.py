import pandas as pd
import numpy as np
import pymongo
from datetime import datetime, timedelta
from collections import OrderedDict
import os, sys



def getDataBase(datebase,host='localhost', port=27017):
    conn = pymongo.MongoClient(host,port)
    db = conn.get_database(datebase)
    return db

def getCollection(db,collection):
    collection = db.get_collection(collection)
    return collection

def readData(db,asset,start,end):
    assert type(start) == datetime, 'parameter start should be datetime'
    assert type(end) == datetime, 'parameter end should be datetime'    

    data = pd.DataFrame()

    #save data in dir
    if not os.path.exists('data'):
        os.mkdir('data')
    assetName = asset.split(':')[0] #dirname cannot include :

    #check local data
    if os.path.exists('data\\'+ assetName+'.csv'):
        print('Loading data locally...')
        data = pd.read_csv('data\\'+ assetName+'.csv',index_col=0)        
        data.index = pd.to_datetime(data.index) #key code: change the index into datetime type
        bar = dict(data.T) #change data into ordereddict
        dateTime = set(i.date() for i in data.index)
        delta = timedelta(days=1)
        date = start
        while date != end+delta:
            #read data by date
            if date.date() not in dateTime: #data is not in local DataBase
                bar.update(getDailyData(db,asset,date))
            print('\rLoading ...', str(getSeconds(date-start)*100//getSeconds(end-start))+'%',end='')
            #next date
            date = date+delta
            
        
        data = OrderedDict(sorted(bar.items(), key=lambda obj: obj[0]))
        
    else:
        print('Loading From Database...')
        data = getData(db,asset,start,end)
    data = pd.DataFrame(data).T #ordereddict to pd
    data.to_csv('data\\'+ assetName+'.csv')
    start = start.strftime('%Y/%m/%d')
    end = end.strftime('%Y/%m/%d')
    data = data[start:end].copy()

    print('\nData loaded successfully!')
    return data

def getData(db,asset,start,end,omit='None'):
    delta = timedelta(days=1)
    
    if omit == 'start':
        start = start + delta
    elif omit == 'end':
        end = end - delta
    elif omit == 'both':
        start = start + delta
        end = end - delta

    bar = OrderedDict()
    date = start
    while date != end+delta:
        #read data by date
        data = getDailyData(db,asset,date)
        if data: #data not empty
            bar.update(data)
                
        print('\rLoading ...', str(getSeconds(date-start)*100//getSeconds(end-start))+'%',end='')
        #next date
        date = date+delta
    
    return bar #return an orderdict

def getSeconds(td):
    days2seconds = td.days*24*60*60
    seconds = td.seconds
    return days2seconds+seconds

def getDataFromDB(db,asset,time):
    collection = db.get_collection(asset) #get collection
    ohlcv = dict()
    for i in collection.find({'datetime': time}):        
        ohlcv['open']=i['open']
        ohlcv['high']=i['high']
        ohlcv['low']=i['low']
        ohlcv['close']=i['close']
        ohlcv['volume']=i['volume']
    return ohlcv

def getDailyData(db,asset,date):
    date = date.strftime('%Y%m%d')
    collection = db.get_collection(asset) #get collection
    dailyData = OrderedDict()
    for i in collection.find({'date': date}):
        ohlcv = dict()        
        ohlcv['open']=i['open']
        ohlcv['high']=i['high']
        ohlcv['low']=i['low']
        ohlcv['close']=i['close']
        ohlcv['volume']=i['volume']
        dailyData[i['datetime']]=ohlcv
    return dailyData


def getBar(data,freq): #get frequence of bar   
    bar = dict()
    bar['open'] = data.open.resample(freq).first().dropna().values
    bar['high'] = data.high.resample(freq).max().dropna().values
    bar['low'] = data.low.resample(freq).min().dropna().values
    bar['close'] = data.close.resample(freq).last().dropna().values
    #bar['volume'] = data.volume.resample(freq).sum().dropna().values
    return bar

if __name__ == "__main__":
    db = getDataBase("VnTrader_1Min_Db")
    asset = 'IF:CTP'
    start = datetime(2013,1,4)
    end = datetime(2018,12,31)
    data = readData(db,asset,start,end)
    print(data)

