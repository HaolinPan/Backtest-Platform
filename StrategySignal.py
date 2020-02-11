import pandas as pd
import numpy as np
import talib as ta

def MACD(bar,fastPeriod,slowPeriod):
    close = bar['close']
    fma = ta.EMA(close,fastPeriod)
    sma = ta.EMA(close,slowPeriod)

    direction = 0
    if fma[-1]>sma[-1] and fma[-2]<sma[-2]:
        direction = 1

    elif fma[-1]<sma[-1] and fma[-2]>sma[-2]:
        direction = -1

    return direction, fma, sma

def maBreak(bar,period):   
    close = bar['close']
    ma = ta.MA(close,period)

    direction = 0
    if close[-1]>ma[-1] and close[-2]<ma[-2]:
        direction = 1
    elif close[-1]<ma[-1] and close[-2]>ma[-2]:
        direction = -1
    
    return direction, ma

def maStauts(bar,period):
    close = bar['close']
    ma = ta.MA(close,period)

    status = 0
    if ma[-1]>ma[-3]:
        status = 1
    else:
        status = -1
    
    return status, ma

def atrStatus(bar,fastPeriod,slowPeriod):
    high = bar['high']
    low = bar['low']
    close = bar['close']
    fatr = ta.ATR(high,low,close,fastPeriod)
    satr = ta.ATR(high,low,close,slowPeriod)

    status = 0
    if fatr[-1] > satr[-1]*0.8:
        status = 1
    
    return status, fatr, satr

def rsiStatus(bar,period):
    close = bar['close']
    rsi = ta.RSI(close,period)

    status = 0
    if rsi[-1] < 30:
        status = 1
    elif rsi[-1] > 70:
        status = -1
    
    return status, rsi

def bollStatus(bar,period):
    close = bar['close']
    up, _, low = ta.BBANDS(close,period)

    status = 0
    if close[-1] < low[-1]:
        status = 1
    elif close[-1] > up[-1]:
        status = -1

    return status, up, low

def bollBack(bar,period):
    close = bar['close']
    up, _, low = ta.BBANDS(close,period)

    status = 0
    if close[-1] > low[-1] and close[-2] < low[-2]:
        status = 1
    elif close[-1] < up[-1] and close[-2] > up[-2]:
        status = -1

    return status, up, low