# -*- coding: utf-8 -*-
"""
Created on Thu Nov 26 23:57:15 2020

@author: yy14313
"""

# -*- coding: utf-8 -*-
"""
Created on Mon May 18 22:37:18 2020

@author: yy14313
"""

# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 12:15:47 2019

@author: yy14313
"""


import pandas as pd
import numpy as np
import datetime 
import sys
import os
import math
import itertools
import time
from multiprocessing import Pool 

tt=time.time()
os.chdir( "/mnt/storage/scratch/yy14313/Slope/2parfor" )

months=['01','02','03','04','05']
#months=['11','12']

days=range(1,32)
def iniVar(var,v,side):
    var[side+'Price'+str(v)]=[]
    var[side+'Size'+str(v)]=[]
    var[side+'ID'+str(v)]=[]
def AssignNaN(variable,v,side):
    variable[side+'Price'+str(v)]=[float('NaN')]
    variable[side+'Size'+str(v)]=[float('NaN')]
    variable[side+'ID'+str(v)]=[""]       
def Appendpre(variable,v,side,i):
    variable[side+'Price'+str(v)].append(variable[side+'Price'+str(v)][i-1])
    variable[side+'Size'+str(v)].append(variable[side+'Size'+str(v)][i-1])
    variable[side+'ID'+str(v)].append(variable[side+'ID'+str(v)][i-1])
def AppendNew(variable,v,side,tmp,pLevel,i):
    if len(tmp)!=0 and len(pLevel)==0:  ###Append new quote
        variable[side+'Price'+str(v)].append(tmp.Price)
        variable[side+'Size'+str(v)].append(tmp.Size_left)
        variable[side+'ID'+str(v)].append(tmp.OrdID_left)
    else:### Append the previous/next price level!!! Here specify tmp=[]!!!
        if pLevel=='Previous':
            variable[side+'Price'+str(v)].append(variable[side+'Price'+str(v-1)][i-1])
            variable[side+'Size'+str(v)].append(variable[side+'Size'+str(v-1)][i-1])
            variable[side+'ID'+str(v)].append(variable[side+'ID'+str(v-1)][i-1])
        elif pLevel=='Next':
            variable[side+'Price'+str(v)].append(variable[side+'Price'+str(v+1)][i-1])
            variable[side+'Size'+str(v)].append(variable[side+'Size'+str(v+1)][i-1])
            variable[side+'ID'+str(v)].append(variable[side+'ID'+str(v+1)][i-1])
 
def AppendSame(variable,v,side,tmp,i):
    if tmp.Type_left=='MO':
        variable[side+'Price'+str(v)].append(variable[side+'Price'+str(v)][i-1])
    else:
        variable[side+'Price'+str(v)].append(tmp.Price)
    variable[side+'Size'+str(v)].append(np.nansum([variable[side+'Size'+str(v)][i-1],tmp.Size_left]))
    variable[side+'ID'+str(v)].append(';'.join([variable[side+'ID'+str(v)][i-1], tmp.OrdID_left]))
def GoToBin(binVar,sideVar,v,binSide,side,tmp,i):
    if len(tmp) ==0:   ### Info at the 5th Price level goes to bin
        binVar[binSide+'Price0'].append(sideVar[side+'Price'+str(v)][i-1])
        binVar[binSide+'Size0'].append(sideVar[side+'Size'+str(v)][i-1])
        binVar[binSide+'ID0'].append(sideVar[side+'ID'+str(v)][i-1])
    else:   ### New quote goes to bin!!
        binVar[binSide+'Price0'].append(tmp.Price)
        binVar[binSide+'Size0'].append(tmp.Size_left)
        binVar[binSide+'ID0'].append(tmp.OrdID_left)
def combine5Levels(m):
    for h in days:
        h=str("{0:0=2d}".format(h))
        if os.path.isfile('OD/OD_' + str(r.Var1) + '_2016' + m  + h + '.csv') and os.path.exists('OD/OD_' + str(r.Var1) + '_2016' + m  + h + '.csv'):
            ds1=pd.read_csv('OD/OD_' + str(r.Var1) +'_2016' + m  + h + '.csv')
            #Wrong time calculation, changing time
            time_S=[]
            for line in ds1.Time:
                line = datetime.datetime.strptime(line, '%H:%M:%S.%f')    
                hr=line.hour
                mins=line.minute
                sec=line.second                                                                                                                                                                           
                milli=line.microsecond
                Times=hr*60*60 + mins*60 + sec + 0.000001*milli
                time_S.append(Times)
            ds1['time_S']=time_S
            del line,hr,mins,sec,milli,Times
        else:
            ds1=[]
        if os.path.isfile('OH/OH_' + str(r.Var1) + '_2016' + m  +h + '.csv') and os.path.exists('OH/OH_' + str(r.Var1) + '_2016' + m  +h + '.csv'):
            ds2=pd.read_csv('OH/OH_' + str(r.Var1) + '_2016' + m  + h + '.csv') 
            time_S=[]
            for line in ds2.Time:
                line = datetime.datetime.strptime(line, '%H:%M:%S.%f')    
                hr=line.hour
                mins=line.minute
                sec=line.second                                                                                                                                                                           
                milli=line.microsecond
                Times=hr*60*60 + mins*60 + sec + 0.000001*milli
                time_S.append(Times)
            ds2['time_S']=time_S
            del line,hr,mins,sec,milli,Times
        else:
            ds2=[]
        if os.path.isfile('Trades/Trades_' + str(r.Var1) + '_2016' + m  + h + '.csv') and os.path.exists('Trades/Trades_' + str(r.Var1) + '_2016' + m  + h + '.csv'):
            ds3=pd.read_csv('Trades/Trades_' + str(r.Var1) + '_2016' + m  + h + '.csv')  
        else:
            ds3=[]
        if len(ds1)!=0 and len(ds2)!=0 and len(ds3)!=0:
            dates=list(ds2['Date'].unique())
            for d in dates:
                dtmp1=ds1[ds1['Date'] == d]
                dtmp2=ds2[ds2['Date'] == d]   
                dtmp3=ds3[ds3['Date'] == d]        
                if len(dtmp1) != 0 and len(dtmp2) != 0 and len(dtmp3) != 0 and not os.path.exists('/newhome/yy14313/Slope/3LOB/data/LOB_' + str(r.Var1) + '_' + m + '_' + h + '.csv'):
                    try:
                        dtmpp= pd.merge(dtmp2, dtmp3, on='TradeID', how='left',suffixes=('_left','_right'))
                        del dtmp2,dtmp3
                        dtmpp.rename(columns={"Seq_left": "Seq"}, inplace=True)
                        dtmp= pd.merge(dtmp1, dtmpp, left_on='Seq',right_on='Seq',how='outer',suffixes=('_left','_right'))
                        del dtmp1,dtmpp
                        dtmp=dtmp.drop("Instrument_left", axis=1)   #drop column  axis=1
                        dtmp=dtmp.drop('Instrument_right', axis=1)
                       # dtmp=dtmp[dtmp.Comments !='";F'] #drop lines    axis=0
                        dtmp.sort_values(by=['Seq'], inplace=True) #sort by sequence
                        dtmp.index = range(len(dtmp.index)) #reindex by the new position
                        if len(dtmp) !=0:
                            emptyOHid=dtmp.index[(dtmp.Type_left.notnull()) & (dtmp.Type_right.isnull())].tolist()
                            emptyOH=dtmp.iloc[emptyOHid[0],11:]  #from the column 12
                            del emptyOHid
                            emptyODid=dtmp.index[(dtmp.Type_left.isnull()) & (dtmp.Type_right.notnull())].tolist()
                            emptyOD=dtmp.iloc[emptyODid[0],0:11] #column 1-11
                            del emptyODid
                            Sim_seq=dtmp.index[(dtmp.Type_left.notnull()) & (dtmp.Type_right.notnull())].tolist()
                            if len(Sim_seq) != 0:
                                i=1
                                for lineSim in Sim_seq:
                                    OD=dtmp.iloc[lineSim+i-1,0:11]
                                    OH_T=dtmp.iloc[lineSim+i-1,11:]
                                    emptyOH.Seq=OD.Seq
                                    emptyOD.Seq=OD.Seq
                                    tmp1=OD.append(emptyOH) 
                                    tmp1 = pd.DataFrame(tmp1).transpose()
                                    tmp2=emptyOD.append(OH_T)
                                    tmp2 = pd.DataFrame(tmp2).transpose()
                                    tmp3=dtmp.iloc[0:lineSim+i-1,:]
                                    tmp4=dtmp.iloc[lineSim+1+i-1:,:]
                                    tmp=tmp3.append(tmp1)
                                    tmp=tmp.append(tmp2)
                                    tmp=tmp.append(tmp4)
                                    del tmp1,tmp2,tmp3,tmp4,dtmp
                                    dtmp=tmp
                                    del tmp
                                    i=i+1
                            dtmp.index = range(len(dtmp.index)) #reindex by the new position
                            dtmp.OrdID_left=dtmp.OrdID_left.replace(np.nan,'')
                            dtmp.OrdID_right=dtmp.OrdID_right.replace(np.nan,'')
                           
                            #emsR=emsR.tolist()
                            #emsR=emsR[0]
                            ### Initiating variables ###
                            Bid={}
                            Ask={}
                            binB={}
                            binA={}
                            for v in range(1,6):
                                iniVar(Bid,v,'Bid')
                                iniVar(Ask,v,'Ask')
                            iniVar(binB,0,'binB')
                            iniVar(binA,0,'binA')
                        
                            ### Assign values
                       
                            if dtmp.Type_left.notnull()[0] & ((dtmp.Type_left[0]=='LO') | (dtmp.Type_left[0]=='CP')) & (dtmp.Direction_left[0]=='B'):
                                for v in range(1,6):
                                    AssignNaN(Ask,v,'Ask')
                                    if v==1:
                                        Bid['BidPrice'+str(v)]=[dtmp.Price[0]]
                                        Bid['BidSize'+str(v)]=[dtmp.Size_left[0]]
                                        Bid['BidID'+str(v)]=[dtmp.OrdID_left[0]]
                    
                                    else:
                                        AssignNaN(Bid,v,'Bid')                    
                            elif dtmp.Type_left.notnull()[0] & ((dtmp.Type_left[0]=='LO') | (dtmp.Type_left[0]=='CP')) & (dtmp.Direction_left[0]=='S'):
                                for v in range(1,6):
                                    AssignNaN(Bid,v,'Bid')
                                    if v==1:
                                        Ask['AskPrice'+str(v)]=[dtmp.Price[0]]
                                        Ask['AskSize'+str(v)]=[dtmp.Size_left[0]]
                                        Ask['AskID'+str(v)]=[dtmp.OrdID_left[0]]
                             
                                    else:
                                        AssignNaN(Ask,v,'Ask')                    
                            else:
                               
                                for v in range(1,6):
                                    AssignNaN(Bid,v,'Bid')
                                    AssignNaN(Ask,v,'Ask')
                            AssignNaN(binB,0,'binB')
                            AssignNaN(binA,0,'binA')
                            hidden=0
                            if dtmp.Time.notnull().iloc[0]:
                                Time=[dtmp.Time.iloc[0]]
                                time_S=[dtmp.time_S_left.iloc[0]]
                            else:
                                Time=[dtmp.Time_left.iloc[0]]
                                time_S=[dtmp.time_S_right.iloc[0]]                      
                            if dtmp.Direction_left.notnull().iloc[0]:
                                direction=[dtmp.Direction_left.iloc[0]]
                            else:
                                direction=[dtmp.Direction_right.iloc[0]]
                            
                            changeBid=[]
                            changeAsk=[]
                            for i in range(1,len(dtmp.index)):
                                tmpp=dtmp.iloc[i] 
                                if ((tmpp.Type_left=='LO') | (tmpp.Type_left=='CP')) & (tmpp.Direction_left=='B'):
                                    for v in range(1,6):
                                        Appendpre(Ask,v,'Ask',i) 
                                        if tmpp.Price==Bid['BidPrice'+str(v)][i-1]:
                                            AppendSame(Bid,v,'Bid',tmpp,i)                                                                               
                                        elif tmpp.Price<Bid['BidPrice'+str(v)][i-1]:
                                            Appendpre(Bid,v,'Bid',i)
                                      
                                            if v==5:
                                                GoToBin(binB,Bid,v,'binB','Bid',tmpp,i)
                                        else:
                                            ### Initialising the first price levels ###
                                            if v==1:    ###Not super accurate, ignore MO here!!!
                                                if math.isnan(Bid['BidPrice'+str(v)][i-1]):
                                                    AppendNew(Bid,v,'Bid',tmpp,[],i)
                                       
                                                elif tmpp.Price>Bid['BidPrice'+str(v)][i-1]:
                                                    AppendNew(Bid,v,'Bid',tmpp,[],i) 
                                                                                                        
                                             
                                                    changeBid=[1]
                                            else: 
                                                if math.isnan(Bid['BidPrice'+str(v)][i-1]): 
                                                    if (not math.isnan(Bid['BidPrice'+str(v-1)][i])) and tmpp.Price<Bid['BidPrice'+str(v-1)][i]:
                                                        AppendNew(Bid,v,'Bid',tmpp,[],i)
                                                    elif (not math.isnan(Bid['BidPrice'+str(v-1)][i])) and tmpp.Price>=Bid['BidPrice'+str(v-1)][i] and len(changeBid)!=0:
                                                        AppendNew(Bid,v,'Bid',[],'Previous',i)   #Price at previous levels changed
                                                    elif (not math.isnan(Bid['BidPrice'+str(v-1)][i])) and tmpp.Price>=Bid['BidPrice'+str(v-1)][i] and len(changeBid)==0:
                                                        Appendpre(Bid,v,'Bid',i)  #Prices at previous levels unchanged
                                                    elif math.isnan(Bid['BidPrice'+str(v-1)][i]):
                                                        Appendpre(Bid,v,'Bid',i)
                                                elif tmpp.Price>Bid['BidPrice'+str(v)][i-1] and tmpp.Price<Bid['BidPrice'+str(v-1)][i]:#New price add to this price level
                                                    AppendNew(Bid,v,'Bid',tmpp,[],i)
                                                    changeBid=[1]
                                                   # AppendNew(Bid,v+1,'Bid',[]) ##Append this price level info to the next price level
                                                    if v==5:
                                                        GoToBin(binB,Bid,v,'binB','Bid',[],i)
                                                elif tmpp.Price>=Bid['BidPrice'+str(v-1)][i] and len(changeBid)!=0:#The previous price level add to this level
                                                    AppendNew(Bid,v,'Bid',[],'Previous',i)           
                                                    if v==5:
                                                        GoToBin(binB,Bid,v,'binB','Bid',[],i)
                                                elif tmpp.Price>=Bid['BidPrice'+str(v-1)][i] and len(changeBid)==0:#New order add to the previous some price level, nothing change to later price levels
                                                    Appendpre(Bid,v,'Bid',i)
                                    changeBid=[]                                                                                       
                                elif ((tmpp.Type_left=='LO') | (tmpp.Type_left=='CP')) & (tmpp.Direction_left=='S'):

                                    for v in range(1,6):
                                        Appendpre(Bid,v,'Bid',i)
                                        if tmpp.Price==Ask['AskPrice'+str(v)][i-1]: 
                                            AppendSame(Ask,v,'Ask',tmpp,i)
                                            
                                        elif tmpp.Price>Ask['AskPrice'+str(v)][i-1]:
                                            Appendpre(Ask,v,'Ask',i)                                                           
                                         
                                            if v==5: 
                                                GoToBin(binA,Ask,v,'binA','Ask',tmpp,i)
                                        else:
                                            ### Initialising the first price levels ###
                                            if v==1: 
                                                if math.isnan(Ask['AskPrice'+str(v)][i-1]):
                                                    AppendNew(Ask,v,'Ask',tmpp,[],i)
                                                    
                                                elif tmpp.Price<Ask['AskPrice'+str(v)][i-1]:
                                                    AppendNew(Ask,v,'Ask',tmpp,[],i)   
                                             
                                                    changeAsk=[1]
                                            else: 
                                                if math.isnan(Ask['AskPrice'+str(v)][i-1]): 
                                                    if (not math.isnan(Ask['AskPrice'+str(v-1)][i])) and tmpp.Price>Ask['AskPrice'+str(v-1)][i]:
                                                        AppendNew(Ask,v,'Ask',tmpp,[],i)
                                                    elif (not math.isnan(Ask['AskPrice'+str(v-1)][i])) and tmpp.Price<=Ask['AskPrice'+str(v-1)][i] and len(changeAsk)!=0:
                                                        AppendNew(Ask,v,'Ask',[],'Previous',i)   #Price at previous levels changed
                                                    elif (not math.isnan(Ask['AskPrice'+str(v-1)][i])) and tmpp.Price<=Ask['AskPrice'+str(v-1)][i] and len(changeAsk)==0:
                                                        Appendpre(Ask,v,'Ask',i)  #Prices at previous levels unchanged
                                                    elif math.isnan(Ask['AskPrice'+str(v-1)][i]):
                                                        Appendpre(Ask,v,'Ask',i) 
                                                elif tmpp.Price<Ask['AskPrice'+str(v)][i-1] and tmpp.Price>Ask['AskPrice'+str(v-1)][i]:#New price add to this price level
                                                    AppendNew(Ask,v,'Ask',tmpp,[],i)
                                                    changeAsk=[1]
                                                   # AppendNew(Bid,v+1,'Bid',[]) ##Append this price level info to the next price level
                                                    if v==5:
                                                        GoToBin(binA,Ask,v,'binA','Ask',[],i)
                                                elif tmpp.Price<=Ask['AskPrice'+str(v-1)][i] and len(changeAsk)!=0:#The previous price level add to this level
                                                    AppendNew(Ask,v,'Ask',[],'Previous',i)           
                                                    if v==5:
                                                        GoToBin(binA,Ask,v,'binA','Ask',[],i)
                                                elif tmpp.Price<=Ask['AskPrice'+str(v-1)][i] and len(changeAsk)==0:#New order add to the previous some price level, nothing change to later price levels
                                                    Appendpre(Ask,v,'Ask',i)                                                                           
                                    changeAsk=[]
                                elif (tmpp.Type_trade=='AT') | (tmpp.Type_trade=='UT'):
                        
                                    tmpIdx=dtmp.OrdID_left[:i].index[dtmp.OrdID_left.iloc[:i]==tmpp.OrdID_right].tolist()
                                    if len(tmpIdx) !=0 :  
                                        endIdx=tmpIdx[-1] 
                                        del tmpIdx
                                  
                                        if dtmp.Size_left.iloc[endIdx] != tmpp.Size_right: 
                                            diff=dtmp.Size_left.iloc[endIdx]-tmpp.Size_right
                                            if diff != tmpp.Volume_right:
                                                hidden +=1
                                            for v in range(1,6):                                               
                                                if any(tmpp.OrdID_right in ele for ele in binB['binBID0']):                            
                                                    Appendpre(Bid,v,'Bid',i)
                                                    Appendpre(Ask,v,'Ask',i)     
                                                    x=[OidInd for OidInd, OidEle in enumerate(binB['binBID0']) if tmpp.OrdID_right in OidEle]
                                                    if diff == binB['binBSize0'][x[0]]:
                                                        del binB['binBPrice0'][x[0]]
                                                        del binB['binBSize0'][x[0]]
                                                        del binB['binBID0'][x[0]]
                                                    elif diff < binB['binBSize0'][x[0]]:
                                                        if tmpp.MsgType=='P':
                                                            binB['binBSize0'][x[0]]=binB['binBSize0'][x[0]]-diff
                                                        elif tmpp.MsgType=='M':
                                                            binB['binBSize0'][x[0]]=binB['binBSize0'][x[0]]-diff
                                                            binB['binBID0'][x[0]]=binB['binBID0'][x[0]].replace(tmpp.OrdID_right,'')
                                                    del x
                                                elif any(tmpp.OrdID_right in ele for ele in binA['binAID0']):                            
                                                    Appendpre(Bid,v,'Bid',i)
                                                    Appendpre(Ask,v,'Ask',i)
                                                    x=[OidInd for OidInd, OidEle in enumerate(binA['binAID0']) if tmpp.OrdID_right in OidEle]
                                                    if diff==binA['binASize0'][x[0]]:
                                                        del binA['binAPrice0'][x[0]]
                                                        del binA['binASize0'][x[0]]
                                                        del binA['binAID0'][x[0]]
                                                    elif diff<binA['binASize0'][x[0]]:
                                                        if tmpp.MsgType=='P':
                                                            binA['binASize0'][x[0]]=binA['binASize0'][x[0]]-diff
                                                        elif tmpp.MsgType=='M':
                                                            binA['binASize0'][x[0]]=binA['binASize0'][x[0]]-diff
                                                            binA['binAID0'][x[0]]=binA['binAID0'][x[0]].replace(tmpp.OrdID_right,'')
                                                elif tmpp.OrdID_right in Bid['BidID'+str(v)][i-1]:
                                                    if diff<Bid['BidSize'+str(v)][i-1]:
                                                        Bid['BidPrice'+str(v)].append(Bid['BidPrice'+str(v)][i-1])
                                                        Bid['BidSize'+str(v)].append(Bid['BidSize'+str(v)][i-1]-diff)
                                                        if tmpp.MsgType=='P':
                                                            Bid['BidID'+str(v)].append(Bid['BidID'+str(v)][i-1])
                                                        elif tmpp.MsgType=='M':
                                                            Bid['BidID'+str(v)].append(Bid['BidID'+str(v)][i-1].replace(tmpp.OrdID_right,''))
                                                        Appendpre(Ask,v,'Ask',i)
                                                        
                                                    elif diff>=Bid['BidSize'+str(v)][i-1]:
                                                        
                                                        if v==5:
                                                            ytmp=np.nanmax(np.unique(binB['binBPrice0']))
                                                            Bid['BidPrice'+str(v)].append(ytmp)
                                                            Bid['BidSize'+str(v)].append(np.nansum(list(itertools.compress(binB['binBSize0'], binB['binBPrice0']==ytmp)))) #BidPrice shouldn't be nan!!
                                                            Bid['BidID'+str(v)].append(';'.join(list(itertools.compress(binB['binBID0'], binB['binBPrice0']==ytmp))))
                                                            binB['binBID0']=[binB['binBID0'][ID] for ID, ele in enumerate(binB['binBPrice0']) if ele!=ytmp]
                                                            binB['binBSize0']=[binB['binBSize0'][ID] for ID, ele in enumerate(binB['binBPrice0']) if ele!=ytmp]
                                                            binB['binBPrice0']=[binB['binBPrice0'][ID] for ID, ele in enumerate(binB['binBPrice0']) if ele!=ytmp]
                                                            del ytmp  
                                                        else:
                                                            changeBid=[1]
                                                            AppendNew(Bid,v,'Bid',[],'Next',i)                                        
                                                        Appendpre(Ask,v,'Ask',i)
                                                elif tmpp.OrdID_right in Ask['AskID'+str(v)][i-1]:
                                                    if diff<Ask['AskSize'+str(v)][i-1]:
                                                        Ask['AskPrice'+str(v)].append(Ask['AskPrice'+str(v)][i-1])
                                                        Ask['AskSize'+str(v)].append(Ask['AskSize'+str(v)][i-1]-diff)
                                                        if tmpp.MsgType=='P':
                                                            Ask['AskID'+str(v)].append(Ask['AskID'+str(v)][i-1])
                                                        elif tmpp.MsgType=='M':
                                                            Ask['AskID'+str(v)].append(Ask['AskID'+str(v)][i-1].replace(tmpp.OrdID_right,''))
                                                        Appendpre(Bid,v,'Bid',i)
                                                        
                                                    elif diff>=Ask['AskSize'+str(v)][i-1]:
                                                        
                                                        if v==5:
                                                            ytmp=np.nanmin(np.unique(binA['binAPrice0']))
                                                            Ask['AskPrice'+str(v)].append(ytmp)
                                                            Ask['AskSize'+str(v)].append(np.nansum(list(itertools.compress(binA['binASize0'], binA['binAPrice0']==ytmp)))) #BidPrice shouldn't be nan!!
                                                            Ask['AskID'+str(v)].append(';'.join(list(itertools.compress(binA['binAID0'], binA['binAPrice0']==ytmp))))
                                                            binA['binAID0']=[binA['binAID0'][ID] for ID, ele in enumerate(binA['binAPrice0']) if ele!=ytmp]
                                                            binA['binASize0']=[binA['binASize0'][ID] for ID, ele in enumerate(binA['binAPrice0']) if ele!=ytmp]
                                                            binA['binAPrice0']=[binA['binAPrice0'][ID] for ID, ele in enumerate(binA['binAPrice0']) if ele!=ytmp]
                                                            del ytmp  
                                                        else:
                                                            changeAsk=[1]
                                                            AppendNew(Ask,v,'Ask',[],'Next',i)                                        
                                                        Appendpre(Bid,v,'Bid',i)
                                                elif len(changeBid)!=0:
                                                    if v==5:
                                                        ytmp=np.nanmax(np.unique(binB['binBPrice0']))
                                                        Bid['BidPrice'+str(v)].append(ytmp)
                                                        Bid['BidSize'+str(v)].append(np.nansum(list(itertools.compress(binB['binBSize0'], binB['binBPrice0']==ytmp)))) #BidPrice shouldn't be nan!!
                                                        Bid['BidID'+str(v)].append(';'.join(list(itertools.compress(binB['binBID0'], binB['binBPrice0']==ytmp))))
                                                        binB['binBID0']=[binB['binBID0'][ID] for ID, ele in enumerate(binB['binBPrice0']) if ele!=ytmp]
                                                        binB['binBSize0']=[binB['binBSize0'][ID] for ID, ele in enumerate(binB['binBPrice0']) if ele!=ytmp]
                                                        binB['binBPrice0']=[binB['binBPrice0'][ID] for ID, ele in enumerate(binB['binBPrice0']) if ele!=ytmp]
                                                        del ytmp
                                                    else:                                        
                                                        AppendNew(Bid,v,'Bid',[],'Next',i)
                                                    Appendpre(Ask,v,'Ask',i)
                                                elif len(changeAsk)!=0:
                                                    if v==5:
                                                        ytmp=np.nanmin(np.unique(binA['binAPrice0']))
                                                        Ask['AskPrice'+str(v)].append(ytmp)
                                                        Ask['AskSize'+str(v)].append(np.nansum(list(itertools.compress(binA['binASize0'], binA['binAPrice0']==ytmp)))) #BidPrice shouldn't be nan!!
                                                        Ask['AskID'+str(v)].append(';'.join(list(itertools.compress(binA['binAID0'], binA['binAPrice0']==ytmp))))
                                                        binA['binAID0']=[binA['binAID0'][ID] for ID, ele in enumerate(binA['binAPrice0']) if ele!=ytmp]
                                                        binA['binASize0']=[binA['binASize0'][ID] for ID, ele in enumerate(binA['binAPrice0']) if ele!=ytmp]
                                                        binA['binAPrice0']=[binA['binAPrice0'][ID] for ID, ele in enumerate(binA['binAPrice0']) if ele!=ytmp]
                                                        del ytmp
                                                    else:
                                                        AppendNew(Ask,v,'Ask',[],'Next',i)
                                                    Appendpre(Bid,v,'Bid',i)
                                                elif (tmpp.OrdID_right not in Ask['AskID'+str(v)][i-1]) and (tmpp.OrdID_right not in Bid['BidID'+str(v)][i-1]) and (all(tmpp.OrdID_right not in ele for ele in binA['binAID0'])) and (all(tmpp.OrdID_right not in ele for ele in binB['binBID0'])) and len(changeAsk)==0 and len(changeBid)==0:                           
                                                    Appendpre(Bid,v,'Bid',i)
                                                    Appendpre(Ask,v,'Ask',i) 
                                            changeAsk=[]
                                            changeBid=[]
                                            dtmp.Size_left.iloc[endIdx] = tmpp.Size_right
                                            del diff
                                        else:
                                            for v in range(1,6):
                                                Appendpre(Bid,v,'Bid',i)
                                                Appendpre(Ask,v,'Ask',i)
                                                
                                                hidden=hidden+1
                                    else:
                                        for v in range(1,6):
                                            Appendpre(Bid,v,'Bid',i)
                                            Appendpre(Ask,v,'Ask',i) 
                                elif tmpp.MsgType=='E' or tmpp.MsgType=='D':                                      
                                    for v in range(1,6): 
                                        if any(tmpp.OrdID_right in ele for ele in binB['binBID0']):                            
                                            Appendpre(Bid,v,'Bid',i)
                                            Appendpre(Ask,v,'Ask',i) 
                                            x=[ID for ID, ele in enumerate(binB['binBID0']) if tmpp.OrdID_right in ele]
                                            x=x[0]
                                            binB['binBSize0'][x]= binB['binBSize0'][x]-tmpp.Size_right
                                            if binB['binBSize0'][x]==0:
                                                del binB['binBPrice0'][x]
                                                del binB['binBSize0'][x]
                                                del binB['binBID0'][x]
                                            else: 
                                                binB['binBID0'][x]=binB['binBID0'][x].replace(tmpp.OrdID_right,'')
                                            del x
                                        
                                        elif any(tmpp.OrdID_right in ele for ele in binA['binAID0']):                             
                                            Appendpre(Bid,v,'Bid',i)
                                            Appendpre(Ask,v,'Ask',i) 
                                            x=[ID for ID, ele in enumerate(binA['binAID0']) if tmpp.OrdID_right in ele]
                                            x=x[0] 
                                            binA['binASize0'][x]= binA['binASize0'][x]-tmpp.Size_right
                                            if binA['binASize0'][x]==0:
                                                del binA['binAPrice0'][x]
                                                del binA['binASize0'][x]
                                                del binA['binAID0'][x]
                                            else: 
                                                binA['binAID0'][x]=binA['binAID0'][x].replace(tmpp.OrdID_right,'')
                                            del x
                                            
                                        elif tmpp.OrdID_right in Bid['BidID'+str(v)][i-1]:
                                            Appendpre(Ask,v,'Ask',i)
                                            if tmpp.Size_right<Bid['BidSize'+str(v)][i-1]:
                                                Bid['BidPrice'+str(v)].append(Bid['BidPrice'+str(v)][i-1])
                                                Bid['BidSize'+str(v)].append(Bid['BidSize'+str(v)][i-1]-tmpp.Size_right)
                                                Bid['BidID'+str(v)].append(Bid['BidID'+str(v)][i-1].replace(tmpp.OrdID_right,''))
                                            elif tmpp.Size_right>=Bid['BidSize'+str(v)][i-1]:                                    
                                                if v==5:
                                                    ytmp=np.nanmax(np.unique(binB['binBPrice0']))
                                                    Bid['BidPrice'+str(v)].append(ytmp)
                                                    Bid['BidSize'+str(v)].append(np.nansum(list(itertools.compress(binB['binBSize0'], binB['binBPrice0']==ytmp))))
                                                    Bid['BidID'+str(v)].append(';'.join(list(itertools.compress(binB['binBID0'], binB['binBPrice0']==ytmp))))
                                                    binB['binBID0']=[binB['binBID0'][ID] for ID, ele in enumerate(binB['binBPrice0']) if ele!=ytmp]
                                                    binB['binBSize0']=[binB['binBSize0'][ID] for ID, ele in enumerate(binB['binBPrice0']) if ele!=ytmp]
                                                    binB['binBPrice0']=[binB['binBPrice0'][ID] for ID, ele in enumerate(binB['binBPrice0']) if ele!=ytmp]
                                                    del ytmp
                                                else:
                                                    changeBid=[1]
                                                    AppendNew(Bid,v,'Bid',[],'Next',i)                                     
                                        elif tmpp.OrdID_right in Ask['AskID'+str(v)][i-1]:
                                            Appendpre(Bid,v,'Bid',i)
                                            if tmpp.Size_right<Ask['AskSize'+str(v)][i-1]:
                                                Ask['AskPrice'+str(v)].append(Ask['AskPrice'+str(v)][i-1])
                                                Ask['AskSize'+str(v)].append(Ask['AskSize'+str(v)][i-1]-tmpp.Size_right)
                                                Ask['AskID'+str(v)].append(Ask['AskID'+str(v)][i-1].replace(tmpp.OrdID_right,''))                                   
                                            elif tmpp.Size_right>=Ask['AskSize'+str(v)][i-1]:
                                                
                                                if v==5:
                                                    ytmp=np.nanmin(np.unique(binA['binAPrice0']))
                                                    Ask['AskPrice'+str(v)].append(ytmp)
                                                    Ask['AskSize'+str(v)].append(np.nansum(list(itertools.compress(binA['binASize0'], binA['binAPrice0']==ytmp))))
                                                    Ask['AskID'+str(v)].append(';'.join(list(itertools.compress(binA['binAID0'], binA['binAPrice0']==ytmp))))
                                                    binA['binAID0']=[binA['binAID0'][ID] for ID, ele in enumerate(binA['binAPrice0']) if ele!=ytmp]
                                                    binA['binASize0']=[binA['binASize0'][ID] for ID, ele in enumerate(binA['binAPrice0']) if ele!=ytmp]
                                                    binA['binAPrice0']=[binA['binAPrice0'][ID] for ID, ele in enumerate(binA['binAPrice0']) if ele!=ytmp]
                                                    del ytmp
                                                else:
                                                    changeAsk=[1]
                                                    AppendNew(Ask,v,'Ask',[],'Next',i)
                                        elif len(changeBid)!=0:
                                            if v==5:
                                                ytmp=np.nanmax(np.unique(binB['binBPrice0']))
                                                Bid['BidPrice'+str(v)].append(ytmp)
                                                Bid['BidSize'+str(v)].append(np.nansum(list(itertools.compress(binB['binBSize0'], binB['binBPrice0']==ytmp))))
                                                Bid['BidID'+str(v)].append(';'.join(list(itertools.compress(binB['binBID0'], binB['binBPrice0']==ytmp))))
                                                binB['binBID0']=[binB['binBID0'][ID] for ID, ele in enumerate(binB['binBPrice0']) if ele!=ytmp]
                                                binB['binBSize0']=[binB['binBSize0'][ID] for ID, ele in enumerate(binB['binBPrice0']) if ele!=ytmp]
                                                binB['binBPrice0']=[binB['binBPrice0'][ID] for ID, ele in enumerate(binB['binBPrice0']) if ele!=ytmp]
                                                del ytmp
                                            else:
                                                AppendNew(Bid,v,'Bid',[],'Next',i)
                                            Appendpre(Ask,v,'Ask',i)
                                        elif len(changeAsk)!=0:
                                            if v==5:
                                                ytmp=np.nanmin(np.unique(binA['binAPrice0']))
                                                Ask['AskPrice'+str(v)].append(ytmp)
                                                Ask['AskSize'+str(v)].append(np.nansum(list(itertools.compress(binA['binASize0'], binA['binAPrice0']==ytmp))))
                                                Ask['AskID'+str(v)].append(';'.join(list(itertools.compress(binA['binAID0'], binA['binAPrice0']==ytmp))))
                                                binA['binAID0']=[binA['binAID0'][ID] for ID, ele in enumerate(binA['binAPrice0']) if ele!=ytmp]
                                                binA['binASize0']=[binA['binASize0'][ID] for ID, ele in enumerate(binA['binAPrice0']) if ele!=ytmp]
                                                binA['binAPrice0']=[binA['binAPrice0'][ID] for ID, ele in enumerate(binA['binAPrice0']) if ele!=ytmp]
                                                del ytmp
                                            else:
                                                AppendNew(Ask,v,'Ask',[],'Next',i)
                                            Appendpre(Bid,v,'Bid',i)
                                        elif (tmpp.OrdID_right not in Ask['AskID'+str(v)][i-1]) and (tmpp.OrdID_right not in Bid['BidID'+str(v)][i-1]) and (all(tmpp.OrdID_right not in ele for ele in binA['binAID0'])) and (all(tmpp.OrdID_right not in ele for ele in binB['binBID0'])) and len(changeAsk)==0 and len(changeBid)==0:                            
                                            Appendpre(Bid,v,'Bid',i)
                                            Appendpre(Ask,v,'Ask',i) 
                                    changeAsk=[]
                                    changeBid=[]
                                elif dtmp.MsgType.iloc[i]=='Z':
                                    tmpIdx=[ID for ID, ele in enumerate(dtmp.OrdID_left.iloc[:i-1]) if tmpp.OrdID_right in ele]
                                    if len(tmpIdx) != 0:
                                        endIdx=tmpIdx[-1]
                                        del tmpIdx 
                                        for v in range(1,6):
                                            if any(tmpp.OrdID_right in ele for ele in binB['binBID0']):                            
                                                Appendpre(Bid,v,'Bid',i)
                                                Appendpre(Ask,v,'Ask',i)
                        
                                                diff=dtmp.Size_left.iloc[endIdx]-tmpp.Size_right
                                                x=[ID for ID,ele in enumerate(binB['binBID0']) if tmpp.OrdID_right in ele]
                                                x=x[0]
                                                binB['binBSize0'][x]=binB['binBSize0'][x]-diff
                                                del x,diff
                                                dtmp.Size_left.iloc[endIdx]=tmpp.Size_right
                                            elif any(tmpp.OrdID_right in ele for ele in binA['binAID0']):                            
                                                Appendpre(Bid,v,'Bid',i)
                                                Appendpre(Ask,v,'Ask',i)
                                                
                                                diff=dtmp.Size_left.iloc[endIdx]-tmpp.Size_right
                                                x=[ID for ID,ele in enumerate(binA['binAID0']) if tmpp.OrdID_right in ele]
                                                x=x[0]
                                                binA['binASize0'][x]=binA['binASize0'][x]-diff
                                                del x,diff
                                                dtmp.Size_left.iloc[endIdx]=tmpp.Size_right
                                            elif tmpp.OrdID_right in Bid['BidID'+str(v)][i-1]:
                                                Bid['BidPrice'+str(v)].append(Bid['BidPrice'+str(v)][i-1])
                                                diff=dtmp.Size_left.iloc[endIdx]-tmpp.Size_right
                                                Bid['BidSize'+str(v)].append(Bid['BidSize'+str(v)][i-1]-diff)
                                                del diff
                                                dtmp.Size_left.iloc[endIdx]=tmpp.Size_right
                                                Bid['BidID'+str(v)].append(Bid['BidID'+str(v)][i-1])
                                                
                                                Appendpre(Ask,v,'Ask',i)
                                                
                                            elif tmpp.OrdID_right in Ask['AskID'+str(v)][i-1]:                                                  
                                                Appendpre(Bid,v,'Bid',i)
                                                  
                                                Ask['AskPrice'+str(v)].append(Ask['AskPrice'+str(v)][i-1])
                                                diff=dtmp.Size_left.iloc[endIdx]-tmpp.Size_right
                                                Ask['AskSize'+str(v)].append(Ask['AskSize'+str(v)][i-1]-diff)
                                                del diff
                                                dtmp.Size_left.iloc[endIdx]=tmpp.Size_right
                                                Ask['AskID'+str(v)].append(Ask['AskID'+str(v)][i-1])
                                            else:
                                                Appendpre(Bid,v,'Bid',i)
                                                Appendpre(Ask,v,'Ask',i)
                                    else: 
                                        for v in range(1,6):
                                            Appendpre(Bid,v,'Bid',i)
                                            Appendpre(Ask,v,'Ask',i)
                                elif tmpp.Type_left=='MO': ###INACCURATE!! I need to create a new variable to record MO!
                                    for v in range(1,6):
                                        if v==1:
                                            if tmpp.Direction_left=='B':
                                                AppendSame(Bid,v,'Bid',tmpp,i)
                                       # BidPrice.append(BidPrice[i-1])
                                       # BidSize.append(BidSize[i-1]+tmpp.Size_left)
                                       # BOrdID.append(';'.join([BOrdID[i-1], tmpp.OrdID_left]))
                                                Appendpre(Ask,v,'Ask',i) 
                                            elif tmpp.Direction_left=='S':
                                                AppendSame(Ask,v,'Ask',tmpp,i)
                                                Appendpre(Bid,v,'Bid',i)
                                        else:
                                            Appendpre(Bid,v,'Bid',i)
                                            Appendpre(Ask,v,'Ask',i)
                                elif dtmp.MsgType.iloc[i]=='T':
                                    for v in range(1,6): 
                                        Appendpre(Bid,v,'Bid',i)
                                        Appendpre(Ask,v,'Ask',i)
                                else:
                    
                                    for v in range(1,6): 
                                        Appendpre(Bid,v,'Bid',i)
                                        Appendpre(Ask,v,'Ask',i)
                                if dtmp.Time.notnull().iloc[i]:
                                    Time.append(tmpp.Time)
                                    time_S.append(tmpp.time_S_left)
                                else:
                                    Time.append(tmpp.Time_left)
                                    time_S.append(tmpp.time_S_right)                        
                                if dtmp.Direction_left.notnull().iloc[i]:
                                    direction.append(tmpp.Direction_left)
                                else:
                                    direction.append(tmpp.Direction_right)
                                    
                                    
                                    
                            RIC=pd.Series([r.Var1]*len(dtmp),name='RIC')
                            date=pd.Series([d]*len(dtmp),name='date')
                            Time=pd.Series(Time,name='Time')
                            time_S=pd.Series(time_S,name='time_S',dtype='float64')
                            Type=pd.Series(dtmp.MsgType,name='Type')
                            Price=pd.Series(dtmp.Price_trade,name='Price',dtype='float64')
                            Volume=pd.Series(dtmp.Volume_right,name='Volume')
                            BidPrice1=pd.Series(Bid['BidPrice1'],name='BidPrice1',dtype='float64')
                            BidSize1=pd.Series(Bid['BidSize1'],name='BidSize1',dtype='float64')
                            AskPrice1=pd.Series(Ask['AskPrice1'],name='AskPrice1',dtype='float64')
                            AskSize1=pd.Series(Ask['AskSize1'],name='AskSize1',dtype='float64')
                            BidPrice2=pd.Series(Bid['BidPrice2'],name='BidPrice2',dtype='float64')
                            BidSize2=pd.Series(Bid['BidSize2'],name='BidSize2',dtype='float64')
                            AskPrice2=pd.Series(Ask['AskPrice2'],name='AskPrice2',dtype='float64')
                            AskSize2=pd.Series(Ask['AskSize2'],name='AskSize2',dtype='float64')
                            BidPrice3=pd.Series(Bid['BidPrice3'],name='BidPrice3',dtype='float64')
                            BidSize3=pd.Series(Bid['BidSize3'],name='BidSize3',dtype='float64')
                            AskPrice3=pd.Series(Ask['AskPrice3'],name='AskPrice3',dtype='float64')
                            AskSize3=pd.Series(Ask['AskSize3'],name='AskSize3',dtype='float64')
                            BidPrice4=pd.Series(Bid['BidPrice4'],name='BidPrice4',dtype='float64')
                            BidSize4=pd.Series(Bid['BidSize4'],name='BidSize4',dtype='float64')
                            AskPrice4=pd.Series(Ask['AskPrice4'],name='AskPrice4',dtype='float64')
                            AskSize4=pd.Series(Ask['AskSize4'],name='AskSize4',dtype='float64')
                            BidPrice5=pd.Series(Bid['BidPrice5'],name='BidPrice5',dtype='float64')
                            BidSize5=pd.Series(Bid['BidSize5'],name='BidSize5',dtype='float64')
                            AskPrice5=pd.Series(Ask['AskPrice5'],name='AskPrice5',dtype='float64')
                            AskSize5=pd.Series(Ask['AskSize5'],name='AskSize5',dtype='float64')
                            Qualifier=pd.Series(dtmp.Type_trade,name='Qualifier')
                            direction=pd.Series(direction,name='direction')
                  
                        
                                  
                            panel=pd.concat([RIC,date,Time,time_S,Type,Price,Volume,BidPrice1,BidSize1,AskPrice1,AskSize1,BidPrice2,BidSize2,AskPrice2,AskSize2,BidPrice3,BidSize3,AskPrice3,AskSize3,BidPrice4,BidSize4,AskPrice4,AskSize4,BidPrice5,BidSize5,AskPrice5,AskSize5,Qualifier,direction],axis=1)
                            Name=dtmp.Name
                            dss=pd.concat([panel,Name],axis=1)
                                
                            dss.to_csv('/mnt/storage/scratch/yy14313/Slope/3LOB/data/LOB_' + str(r.Var1) + '_' + m + '_' + h + '.csv',index=False,index_label=None,header=True,decimal='.')
                            tmpElapsed=time.time()-tt
                            print('time_'+str(r.Var1)+'_'+h+'_'+m+'='+str(tmpElapsed))
                    except ValueError:
                        tmpElapsed=time.time()-tt
                        print('time_'+str(r.Var1)+'_'+h+'_'+m+'='+str(tmpElapsed))
                        pass  
                    except IndexError:
                        tmpElapsed=time.time()-tt
                        print('time_'+str(r.Var1)+'_'+h+'_'+m+'='+str(tmpElapsed)+'indexerror')
                        pass
codes=pd.read_csv('/mnt/storage/scratch/yy14313/Slope/sample/F100sampleall.csv',usecols=['ISIN','Var1'])

for ind, r in codes.iterrows():
    if __name__=='__main__':
        with Pool() as pool:
            pool.map(combine5Levels,months)
    elapsed = time.time()-tt
    print('TotalTime='+str(elapsed)) 