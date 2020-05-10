import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import LoadHistoryData as getData
from collections import OrderedDict

class BacktestEngine():
    #initiating
    database = None
    strategy = None
    start = datetime(1,1,1)
    end = datetime(1,1,1)
    advancedBar = 0 #using unit bar
    time = datetime(1,1,1)
    timeList = []
    unitBar = timedelta(minutes=1)
    #account
    value = 0
    balance = 0
    initial_cap = 0
    disposable_balance = 0
    position = dict()
    hValue = 0 #highest value
    drawdown = 0
    MDD_ratio = 0
    #trade
    orderList = dict()
    orderNum = 1
    tradeList = dict()
    tradeNum = 1
    transaction = dict()
    currentPrice = dict()
    commission = 0
    #other
    assetList = []
    result = dict()
    data = dict()
    currentData = dict()
    basicBar = dict()
    aggBar = dict()
    log = dict()
    log['balance']=dict()
    log['drawdown']=dict()
    
    def __init__(self,setting):
        self.balance = setting['balance']
        self.initial_cap = setting['balance']
        self.disposable_balance = self.balance
        self.setAsset(setting['assetList'])
        self.commission = setting['commission']
        self.start = setting['start']
        self.end = setting['end']
        self.advancedBar = setting['advancedBar']
    
    def setDatabase(self,database,fromDB=False):
        self.database = database
        freq = self.strategy.freq
        for asset in self.assetList:
            data = pd.DataFrame()
            if fromDB:
                data = getData.readData(self.database,asset,self.start,self.end)
                data = data.resample(freq).agg({'open':'first','high':'max','low':'min','close':'last','volume':'sum'}).dropna() #resample function generates nan in non-trading time
            else:               
                data = database[asset].resample(freq).ohlc().dropna() #resample function generates nan in non-trading time
            self.timeList = data.index #set timelist
            bar = dict()
            bar['open']=data.open.values
            bar['high']=data.high.values
            bar['low']=data.low.values
            bar['close']=data.close.values
            #bar['volume']=data.volume.values
            self.data[asset] = bar #dict
            self.currentData[asset] = dict() #dict
        print('Backtest Engine initialized...')
    
    def setStrategy(self,strategy):
        self.strategy = strategy

    def setAsset(self,assetList):
        self.assetList = assetList
        for asset in assetList:
            self.position[asset]=0

    def setCommission(self,commission):
        self.commission = commission     
           
    def runBacktesting(self):
        #value initializing
        self.value = self.balance
        #run backtest follow according to the backtest time
        for i in range(self.advancedBar,len(self.timeList)):
            self.time = self.timeList[i] #update time
            for asset in self.assetList:
                bar = dict()
                for key in self.data[asset].keys():
                    bar[key]=self.data[asset][key][i-self.advancedBar:i+1]
                self.currentData[asset] = bar #dict
            self.refreshPrice() #refresh prices
            self.trade() #trade according to previous orders and new prices
            self.cal_value() #calculate value after trading           
            self.strategy.onBar() #produce orders         
            self.log['balance'][self.time]=self.value
            self.log['drawdown'][self.time]=self.drawdown
        
        #result information
        self.log['balance'] = OrderedDict(sorted(self.log['balance'].items(), key=lambda obj: obj[0]))
        self.result['balance'] = pd.DataFrame.from_dict(self.log['balance'], orient='index')
        self.log['drawdown'] = OrderedDict(sorted(self.log['drawdown'].items(), key=lambda obj: obj[0]))
        self.result['drawdown'] = pd.DataFrame.from_dict(self.log['drawdown'], orient='index')

    def refreshPrice(self):
        for asset in self.assetList:
            o = self.currentData[asset]['open'][-1]
            h = self.currentData[asset]['high'][-1]
            l = self.currentData[asset]['low'][-1]
            c = self.currentData[asset]['close'][-1]
            #v = self.currentData[asset]['volume'][-1]
            self.currentPrice[asset]=dict({'open':o,'high':h,'low':l,'close':c})

    def trade(self):
        if len(self.orderList)!=0:
            for num in list(self.orderList.keys()).copy():
                asset = self.orderList[num]['asset']
                position = self.orderList[num]['position']
                price = self.orderList[num]['price']
                volume = self.orderList[num]['volume']
                if position=='buy':
                    if price>self.currentPrice[asset]['open']:
                        if self.disposable_balance > self.currentPrice[asset]['open']*(1+self.commission)*volume:
                            self.buyTrade(asset,self.currentPrice[asset]['open'],volume)
                            del self.orderList[num]
                        else:
                            print(self.time)
                            print('Warning! The account does not have enough balance to buy!')

                elif position=='sell':
                    if self.position[asset]>=volume:
                        self.sellTrade(asset,self.currentPrice[asset]['open'],volume)
                        del self.orderList[num]
                        if self.position[asset]==0:
                            del self.transaction[asset]
                    else:
                        print(self.time)
                        print('Warning! The account does not have enough asset to sell!')

                elif position=='short':
                    if price<self.currentPrice[asset]['open']:
                        if self.disposable_balance > self.currentPrice[asset]['open']*(1+self.commission)*volume:
                            self.sellTrade(asset,self.currentPrice[asset]['open'],volume)
                            del self.orderList[num]
                        else:
                            print(self.time)
                            print('Warning! The account does not have enough money to short!')
                
                elif position=='cover':
                    if -self.position[asset]>=volume:
                        self.buyTrade(asset,self.currentPrice[asset]['open'],volume)
                        del self.orderList[num]
                        if self.position[asset]==0:
                            del self.transaction[asset]
                    else:
                        print(self.time)
                        print('Warning! The account does not have enough asset to cover!')
    
    def order(self,asset,price,volume,position):
        assert volume>0, 'volume should be greater than 0!'
        assert position in ['buy','sell','short','cover'],'parameter position should be buy, sell, short or cover!'
        
        num = self.orderNum
        self.orderList[num]=dict({'asset':asset,'price':price,'volume':volume,'position':position})
        self.orderNum+=1
    
    def buy(self,asset,price,volume):
        self.order(asset,price,volume,'buy')
    
    def sell(self,asset,volume):
        self.order(asset,None,volume,'sell')
              
    def short(self,asset,price,volume):
        self.order(asset,price,volume,'short')

    def cover(self,asset,volume):
        self.order(asset,None,volume,'cover')

    def getOrder(self):
        return self.orderList
    
    def cancellOrder(self,num):
        del self.orderList[num]
    
    def cancellAllOrder(self):
        self.orderList=dict()
                
    def buyTrade(self,asset,price,volume):
        num = self.tradeNum
        self.position[asset]+=volume
        self.balance-=price*(1+self.commission)*volume
        self.cal_disposable_value()
        self.tradeList[num]=dict({'asset':asset,'price':price,'volume':volume,'position':'buy'})
        self.tradeNum+=1
        self.transaction[asset]=price
            
    def sellTrade(self,asset,price,volume):
        num = self.tradeNum
        self.position[asset]-=volume
        self.balance+=price*(1-self.commission)*volume
        self.cal_disposable_value()
        self.tradeList[num]=dict({'asset':asset,'price':price,'volume':volume,'position':'sell'})
        self.tradeNum+=1
        self.transaction[asset]=price

    def cal_value(self):
        self.value = self.balance   
        for asset in self.position.keys():
            self.value += self.position[asset]*self.currentPrice[asset]['close']
        #MDD
        self.hValue = max(self.value,self.hValue)
        self.drawdown = self.value-self.hValue
        self.MDD_ratio = min((self.drawdown/self.hValue),self.MDD_ratio)
    
    def cal_disposable_value(self):
        self.disposable_balance = self.balance
        for asset in self.position.keys():
            if self.position[asset]<0:
                self.disposable_balance += self.position[asset]*self.currentPrice[asset]['close']

    def plot(self):
        _, (ax1,ax2) = plt.subplots(2,1,figsize=(12,6))
        balance = self.result['balance']
        drawdown = self.result['drawdown']
        time = balance.index.astype(str)
        ax1.plot(time,balance)
        ax1.set_title('Balance')
        ax1.grid(axis='y')
        ax2.bar(time,drawdown.loc[:,0].values,width=0.5)
        ax2.set_title('Drawdown')
        ax2.grid(axis='y')
        ticks = [time[0+(len(time)-1)//10*i] for i in range(11)]
        ax1.set_xticks(ticks)
        ax1.set_xticklabels([i[2:10] for i in ticks])
        ax2.set_xticks(ticks)
        ax2.set_xticklabels([i[2:10] for i in ticks])
        plt.savefig('Backtest Result.png')
        plt.show()       
    
    def showResult(self):
        drawdown = self.result['drawdown']
        MDD = min(drawdown.values)[0]
        balance = self.result['balance']
        final_cap = balance.values[-1][0]
        total_ret = final_cap/self.initial_cap - 1

        if self.strategy.freq == 'M': #time unit: M
            monthlyRet = ((balance-balance.shift(1))/balance.shift(1)).dropna().values
            ret = (total_ret + 1)**(1/len(monthlyRet))-1
            std = monthlyRet.std() #standard deviation
            negative_std = monthlyRet[monthlyRet<0].std() #negative standard deviation
        else:
            dailyBalance = self.result['balance'].resample('D').last()
            dailyRet = ((dailyBalance-dailyBalance.shift(1))/dailyBalance.shift(1)).dropna().values
            ret = (total_ret + 1)**(1/len(dailyRet))-1
            std = dailyRet.std() #standard deviation
            negative_std = dailyRet[dailyRet<0].std() #negative standard deviation

        annual_return = 0
        annual_std = 0
        annual_negative_std = 0

        if self.strategy.freq == 'M': #time unit: M
            annual_return = (ret+1)**12-1
            annual_std = std * np.sqrt(12)
            annual_negative_std = negative_std * np.sqrt(12)
        else:
            annual_return = (ret+1)**252-1
            annual_std = std * np.sqrt(252)
            annual_negative_std = negative_std * np.sqrt(252)
        
        sharpe = annual_return/annual_std
        sortino = annual_return/annual_negative_std
        calmar = annual_return/abs(self.MDD_ratio)

        print('Start Date: ' + str(self.timeList[self.advancedBar])[:10])
        print('Final Date: ' + str(self.timeList[-1])[:10])
        print('Start Balance: $ '+ str(int(self.initial_cap)))
        print('Final Balance: $ '+ str(int(final_cap)))
        print('Maximum Drawdown: $ ' + str(int(MDD)))
        print('Maximum Drawdown Ratio: ' + str(round(self.MDD_ratio*100,2))+ '%')
        print('Total Return: ' + str(round(total_ret*100,2))+ '%')
        print('Annual Return: ' + str(round(annual_return*100,2))+ '%')
        print('Sharpe Ratio: ' + str(round(sharpe,2)))
        print('Sortino Ratio: ' + str(round(sortino,2)))
        print('Calmar Ratio: ' + str(round(calmar,2)))