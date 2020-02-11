import pandas as pd
import numpy as np
import StrategySignal as signal
import LoadHistoryData as getData


class Strategy:
    account = None
    database = None
    volume = 10
    stopPoint = 0
    initial_stopPoint = 0.02
    reset = False
    profitPoint = 4
    assetList = ['Asset'+ str(i) for i in range(1,28)]
    freq = 'M' #frequency
    gain = 0
    maxGain = 0

    def __init__(self,account):
        self.account = account
        self.database = account.database
    
    def strategy(self):    
    ################### order ###############################
        #filter the assets whose prices does not change
        self.account.cancellAllOrder()
        assetTradeList = []
        for asset in self.assetList:
            if sum(self.account.currentPrice[asset].values()) != 4:
                assetTradeList.append(asset)        
        
        entryList = []
        exitList = []
        holdList = []
        stop_profit_List = []
        
        for asset in assetTradeList:
            position = self.account.position[asset]
            self.cal_gain(asset) #calculate gain
            self.setStopPoint(asset) #reset stop point
            #signal
            entrySig = self.entrySignal(asset)
            exitSig = self.exitSignal(asset)

            if entrySig==exitSig:
                exitSig = 0 #in case of redundant exit

            if entrySig != 0:
                #opposition position signal
                if not ((entrySig == 1 and self.account.position[asset]>0) \
                    and (entrySig == -1 and self.account.position[asset]<0)):
                    entryList.append(asset) #in asset
            if exitSig != 0:
                exitList.append(asset) # out asset
            if position != 0:
                holdList.append(asset)
            
            #stop loss & take profit   
            if entrySig==0 and exitSig==0: #exit
                stop = self.stopLoss(asset)
                profit = self.takeProfit(asset)
                if stop or profit:
                    stop_profit_List.append(asset)
            
            #exit
            self.exit(asset,exitSig,abs(position)) #should use abs in case of short position
        
        #after exit
        now_holdList = list(set(holdList)-set(exitList)-set(stop_profit_List))

        #at this time should hold
        finalList = list((set(now_holdList)|set(entryList)))
        new_holdList = list(set(entryList)-set(now_holdList))
        unchangeList = list(set(now_holdList)-set(entryList))

        #1/n should hold this value for each in asset
        seperated_value = 0.9 * self.account.value/len(finalList)

        if entryList: #new entry
            disp_bal = self.account.disposable_balance
            if disp_bal < seperated_value*len(new_holdList):
                self.rebalance(unchangeList,seperated_value)
            
            for asset in entryList:
                close = self.account.currentPrice[asset]['close']
                #opposite order does not change other assets
                if asset in now_holdList:
                    self.volume = int(abs(self.account.position[asset])/1.1)
                else:
                    self.volume = seperated_value//close #should hold this volume
                if self.volume == 0:
                    continue
                #entry
                entrySig = self.entrySignal(asset) #signal
                self.entry(asset,entrySig)
                
    #########################################################

    def onBar(self):
        self.strategy()

    def entrySignal(self,asset):
        bar = self.account.currentData[asset]
        maSignal, _ = signal.maBreak(bar,15)
        atrStatus, _, _ = signal.atrStatus(bar,10,50)
        entrySignal = 0
        if maSignal == 1 and atrStatus == 1:
            entrySignal = 1
        elif maSignal == -1 and atrStatus == 1:
            entrySignal = -1
        else:
            entrySignal = 0
        return entrySignal
    
    def exitSignal(self,asset):
        bar = self.account.currentData[asset]
        MACD, _, _ = signal.MACD(bar,20,50)
        maStatus, _ = signal.maStauts(bar,20)
        exitSignal = 0
        if MACD == 1 and maStatus==1:
            exitSignal = 1
        elif MACD == -1 and maStatus==-1:
            exitSignal = -1
        else:
            exitSignal = 0
        return exitSignal

    def entry(self,asset,entrySig):
        position = self.account.position[asset]
        close = self.account.currentPrice[asset]['close']
        if entrySig == 1:
            if position<0:
                self.account.cover(asset,abs(position)) #positon < 0
                self.account.buy(asset,close*1.01,self.volume)
            elif position==0:
                self.account.buy(asset,close*1.01,self.volume)
        elif entrySig == -1:
            if position>0:
                self.account.sell(asset,position)
                self.account.short(asset,close*0.99,self.volume)
            elif position==0:
                self.account.short(asset,close*0.99,self.volume)
    
    def exit(self,asset,exitSig,volume):
        position = self.account.position[asset]
        if exitSig == 1:
            if position<0:
                self.account.cover(asset,volume)
        elif exitSig == -1:
            if position>0:
                self.account.sell(asset,volume)
    
    def stopLoss(self,asset):
        position = self.account.position[asset]
        #trailing stoploss
        if position>0:
            transaction = self.account.transaction[asset]
            if (self.maxGain-self.gain)/transaction > self.stopPoint:
                self.account.sell(asset,position)
                return True
        elif position<0:
            transaction = self.account.transaction[asset]
            if (self.maxGain-self.gain)/transaction > self.stopPoint:
                self.account.cover(asset,-position)
                return True
        return False
    
    def takeProfit(self,asset):
        close = self.account.currentPrice[asset]['close']
        position = self.account.position[asset]
        if position>0:
            transaction = self.account.transaction[asset]
            if (close - transaction)/transaction > self.profitPoint:
                self.account.sell(asset,position)
                return True
        elif position<0:
            transaction = self.account.transaction[asset]
            if (close - transaction)/transaction < -self.profitPoint:
                self.account.cover(asset,-position)
                return True
        return False       
    
    def setStopPoint(self,asset):
        bar = self.account.currentData[asset]
        position = self.account.position[asset]
        rsiStatus, _ = signal.rsiStatus(bar, 10)
       
        if position != 0:
            transaction = self.account.transaction[asset]
            if not self.reset:
                if (position>0 and rsiStatus == 1) or (position<0 and rsiStatus == -1)\
                or ((self.gain/transaction)>0.2): #narrow stop point
                    self.stopPoint = round(self.stopPoint*0.5,3)
                    self.reset = True        
        else: #reset
            self.stopPoint = self.initial_stopPoint
            self.reset = False
    
    def rebalance(self,hold,seperated_value):
        for asset in hold:
            close = self.account.currentPrice[asset]['close']
            position = self.account.position[asset]
            #reset volume
            self.volume = seperated_value//close #should hold this volume           
            #excess volume
            exitVolume = 0
            if position!=0:
                if self.volume<abs(position):
                    exitVolume = abs(position)-self.volume         
                    #exit
                    if position > 0:
                        self.exit(asset,-1,exitVolume)
                    else:
                        self.exit(asset,1,exitVolume)

    def cal_gain(self,asset):
        high = self.account.currentPrice[asset]['high']
        low = self.account.currentPrice[asset]['low']
        position = self.account.position[asset]

        if position==0:
            self.maxGain = 0
            self.gain = 0
        elif position>0:
            transaction = self.account.transaction[asset]
            self.gain = high-transaction
            self.maxGain = max(self.maxGain,self.gain)
        elif position<0:
            transaction = self.account.transaction[asset]
            self.gain = transaction-low
            self.maxGain = max(self.maxGain,self.gain)