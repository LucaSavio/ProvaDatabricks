import time
from time import gmtime, strftime
from datetime import datetime, timedelta
import json
import pandas as pd
import numpy as np
import pprint
import operator
import math
import datetime as dt 
import pymongo 
import Downtime_Modulare 
import REELCHANGE_FUNZIONE
import STATUSCHANGE_FUNZIONE
import BLADECHANGE_FUNZIONE
import ALERT_FUNZIONE
import ReelChange_SQL
import BladeChange_SQL
from bson.objectid import ObjectId
import bson
from bson.json_util import loads
from pymongo import MongoClient
from bson.json_util import dumps
import CosmosDBQueryHelper as CDQH
from TelemetryHelper import TelemetryHelper
import asyncio

import Status_SQL

Main_customer = 'KAPA OIL KENIA'

def try_parse(object):

    if isinstance(object, (int, np.integer)):
        return  np.int(object)
        
    if isinstance(object, (float, np.float)):
        return  np.float(object)

    if isinstance(object, (str, np.unicode_)):
        return  np.unicode_(object)

    return object

@asyncio.coroutine
async def UpdateLastValuesCollection(pdnew, cosmosHelper, telemetryClient):
    try:
        print("--->UPDATING LAST VALUES")      
        NOW_CLIENTI = time.time()
        clienti = pdnew.drop_duplicates(subset = ["customer","line","machine","variabile"]).reset_index(drop=True)
        #telemetryClient.track_event('PeriniCalcLauncherExecTime','CLIENTI UNIQUE',str(time.time()-NOW_CLIENTI) + ' seconds')
        for row in range(0,clienti.shape[0]):
            NOW_LAST = time.time()
            riga = clienti.loc[row,:]                    
            query_last = "SELECT * FROM container c WHERE c.customer = '%s' AND c.line = '%s' AND c.machine = '%s' AND c.variabile = '%s'" % (str(riga['customer']),str(riga['line']),str(riga['machine']),str(riga['variabile']))
            last = cosmosHelper.lastValuesCollection.SelectOne(query_last)
            #telemetryClient.track_event('PeriniCalcLauncherExecTime','GET LAST',str(time.time()-NOW_LAST) + ' seconds')
            NOW_UPDATE = time.time()
            if not last is None:
                item_temp = last.copy()
                item_temp['val']=try_parse(riga['val'])
                item_temp['time']=np.int(riga['time'])
                if ('date' in riga) and ('date' in item_temp):
                    item_temp['date']=riga['date']
                else:
                    print('date not found: item_temp -> %s riga-> %s') % (str(item_temp),str(riga))
                #lastj = json.dumps(str(last))
                cosmosHelper.lastValuesCollection.ReplaceOne(item_temp)
            else:
                rigaj = json.loads(riga.to_json())
                cosmosHelper.lastValuesCollection.InsertOne(rigaj)
            
        telemetryClient.track_event('UpdateLastValuesCollectionExecTime','UpdateLastValuesCollection',str(time.time()-NOW_UPDATE) + ' seconds')       
        return True
    except Exception as e:
        telemetryClient.track_exception()
        print(e)
        return False


@asyncio.coroutine
async def LauchPeriniCalcMain (pdold,pdnew,last_run,datetime_current_utc,endpoint,primaryKey,cosmosDbName,customerCollName, dataSourceCollName, dataDestCollName,lastvaluesCollName,shiftCollName,debug,debugTime,telemetrykey, customer, cosmosHelper, tc):
    try:
        print("--->LAUNCHING CALC MAIN")   
        NOW_PERINICALC_MAIN = time.time()
        result = True
        if pdold.size > 0:
            print(dt.datetime.now().strftime("%m-%d-%Y %H:%M:%S") + " - OldVector found: " + str(pdold.size) + " records")
            print(dt.datetime.now().strftime("%m-%d-%Y %H:%M:%S") + " - NewData found: " + str(pdnew.size) + " records")
            
            ares = Downtime_Modulare.PERINICALC.MAIN(pdold,pdnew,last_run,datetime_current_utc,endpoint,primaryKey,cosmosDbName,customerCollName, dataSourceCollName, dataDestCollName,lastvaluesCollName,shiftCollName,debug,debugTime,telemetrykey) 
            
            result = result and ares
            ###########################if not ares:
            #    customer["job_last_run"] = pd.to_datetime(customer.get('job_last_run'),utc=True).strftime("%m-%d-%Y %H:%M:%S")
            #    cosmosHelper.customerCollection.ReplaceOne(customer)
            ####################### PER FAR GIRARE LO STORICO DAL 10 SETTEMBRE
        else:
                print(dt.datetime.now().strftime("%m-%d-%Y %H:%M:%S") + " - No OldVector records found")
        tc.track_event('LauchPeriniClalcMainExecTime','LauchPeriniCalcMain',str(time.time()-NOW_PERINICALC_MAIN) + ' seconds')
        return result
    except Exception as e:
            tc.track_exception()
            print(e)
            return False

def PeriniCalcLauncher(endpoint, primaryKey, cosmosDbName, customerCollName, dataSourceCollName, dataDestCollName,lastvaluesCollName,shiftCollName, debug, telemetrykey):
    tc = TelemetryHelper(telemetrykey)
    try:
        #START = time.time()
        #COUNTER = 1
        #while COUNTER == 1:
            
            NOW = time.time()
            cosmosHelper = CDQH.CosmosHelper(endpoint, primaryKey, cosmosDbName, customerCollName, dataSourceCollName, dataDestCollName,lastvaluesCollName,shiftCollName, telemetrykey)
        
            #tc.track_event('PeriniCalcLauncher','Starting Method...')

            if debug==0:
                debugTime=0
            else:
                debugTime=dt.datetime.now().strftime("%m-%d-%Y %H:%M:%S")

            result = True

            #print("Getting all customers")
            #tc.track_event('PeriniCalcLauncher','Start query customer settings...')
            col = list(cosmosHelper.customerCollection.Select("SELECT * FROM container c WHERE c.name = '%s'" % (Main_customer)))
        
            #tc.track_event('PeriniCalcLauncher','End query customer settings.')
            #col = list(cosmosHelper.customerCollection.ReadItems())
            loop = asyncio.new_event_loop();
            asyncio.set_event_loop(loop)
            for c in col:
                datetime_current_utc = pd.to_datetime(pd.Timestamp.utcnow(),utc=True)
                datetime_last_utc =  pd.to_datetime(datetime_current_utc - pd.Timedelta(minutes = 2))
                last_run = datetime_last_utc   #.strftime("%m-%d-%Y %H:%M:%S")
                #datetime_current_utc = t + pd.Timedelta(minutes=15)
                #datetime_last_utc = t
                current_run = datetime_current_utc  #.strftime("%m-%d-%Y %H:%M:%S")
                unixtime_current_utc = int(datetime.timestamp(datetime_current_utc)*1000) 
                unixtime_last_utc = int(datetime.timestamp(datetime_last_utc)*1000)
                print(str(pd.to_datetime(pd.Timestamp.utcnow(),utc=True)) + " - Working on customer: " + c.get('name'))   #ORA DI INIZIO DEL JOB IN UTC
                #tc.track_event('PeriniCalcLauncher','Retrieving settings for every customer...')
                customer_name=c.get('name')
                customer_id = c.get('id')
                ######################## PER LO STORICO 
                #last_run = t 
                if c.get('job_last_run'):   # se è piena, fare un check sul NOT 
                    last_run = pd.to_datetime(c.get('job_last_run'),format='%m-%d-%Y %H:%M:%S',utc=True)
                    unixtime_last_utc = int(datetime.timestamp(pd.to_datetime(last_run,format='%m-%d-%Y %H:%M:%S',utc=True))*1000)
                    print ('JOB_LAST_RUN = ' + pd.to_datetime(c.get('job_last_run'),utc=True).strftime("%m-%d-%Y %H:%M:%S"))
                
                #ADA-TODO: da commentare quando faremo girare il Job in PROD
                last_run = pd.to_datetime('10-03-2019 16:45:00',format='%m-%d-%Y %H:%M:%S',utc=True)
                unixtime_last_utc = int(datetime.timestamp(pd.to_datetime(last_run,format='%m-%d-%Y %H:%M:%S',utc=True))*1000)
                    
          
                ########if debug == 0:           #VERIFICARE SE é CORRETTO AGGORNARE IL JOB LAST RUN SOLO SE NON SIAMO IN DEBUG
                ########    customer = cosmosHelper.customerCollection.SelectOne("SELECT * FROM container c WHERE c.id='" + customer_id + "'")
                ########    customer["job_last_run"] = current_run.strftime("%m-%d-%Y %H:%M:%S")
                ########    cosmosHelper.customerCollection.ReplaceOne(customer)
                ########tc.track_event('PeriniCalcLauncher','Querying new data for each customer...')
                print("--->NEW")
                #NOW_NEW = time.time()
                newdata = list(cosmosHelper.dataSourceCollection.Select("SELECT * FROM container c WHERE c.customer = '" + customer_name + "' AND c.time > " + str(unixtime_last_utc) + " AND c.time < " + str(unixtime_current_utc) + " ORDER BY c.time DESC"))
                if len(newdata) == 0:
                    print('No new data...')
                else:
                    #tc.track_event('PeriniCalcLauncherExecTime','NEW COSMOS QUERY',str(time.time()-NOW_NEW) + ' seconds')
                    pdnew = pd.DataFrame(newdata)#.sort_values(by='time',ascending=False,inplace=True)
                    #tc.track_event('PeriniCalcLauncher','End query')
                    #tc.track_event('PeriniCalcLauncherExecTime','NEW DATAFRAME',str(time.time()-NOW_NEW) + ' seconds')

                    print("--->OLD")
                    #NOW_OLD = time.time()
                    old_col = list(cosmosHelper.lastValuesCollection.Select("SELECT * FROM container c WHERE c.customer = '" + str(customer_name) + "'"))
                    #tc.track_event('PeriniCalcLauncherExecTime','OLD QUERY',str(time.time()-NOW_OLD)+ ' seconds')
                    pdold = pd.DataFrame(old_col)
                    #tc.track_event('PeriniCalcLauncherExecTime','OLD DATAFRAME',str(time.time()-NOW_OLD) + ' seconds')            
            
                    # eseguo in async 
                    # aggiorno la collection Last Values con i valori trovati in new
                    # lancio calcoli e scrivo in aggregated
                
                
                    tasks = UpdateLastValuesCollection(pdnew, cosmosHelper, tc), LauchPeriniCalcMain (pdold,pdnew,last_run,datetime_current_utc,endpoint,primaryKey,cosmosDbName,customerCollName, dataSourceCollName, dataDestCollName,lastvaluesCollName,shiftCollName,debug,debugTime,telemetrykey, c, cosmosHelper, tc)
                    updres, result = loop.run_until_complete(asyncio.gather(*tasks))
                
                    if not updres:
                        print("--->UPDATE LAST VALUES ERROR")
        
            tc.track_event('PeriniCalcTOTALExecTime','PeriniCalcTOTALExecTime',str(time.time()-NOW) + ' seconds')
            loop.close()        
            return result
    except Exception as e:
        tc.track_exception()
        print(e)
        return False


def PeriniReelChangeLauncher(endpoint, primaryKey, cosmosDbName, customerCollName, dataSourceCollName, dataDestCollName,lastvaluesCollName,shiftCollName,debug,StartTimeScheduler,JobLastRunString,telemetrykey):
    tc = TelemetryHelper(telemetrykey)
    try:
        #COUNTER = 1
        #while COUNTER == 1:
            res = REELCHANGE_FUNZIONE.REELCHANGE.REEL(endpoint, primaryKey, cosmosDbName, customerCollName, dataSourceCollName, dataDestCollName,lastvaluesCollName,shiftCollName,debug, StartTimeScheduler,JobLastRunString,telemetrykey)
            return res
    except Exception as e:
        tc.track_exception()
        print(e)
        return False

def PeriniReelChangeLauncher_SQL(endpoint, primaryKey, cosmosDbName, customerCollName, dataSourceCollName, dataDestCollName,lastvaluesCollName,shiftCollName,debug,StartTimeScheduler,JobLastRunString,telemetrykey,cnxn):
    tc = TelemetryHelper(telemetrykey)
    try:
        #COUNTER = 1
        #while COUNTER == 1:
            res = ReelChange_SQL.REELCHANGE_SQL.REEL(endpoint, primaryKey, cosmosDbName, customerCollName, dataSourceCollName, dataDestCollName,lastvaluesCollName,shiftCollName,debug, StartTimeScheduler,JobLastRunString,telemetrykey,cnxn)
            return res
    except Exception as e:
        tc.track_exception()
        print(e)
        return False
    

def PeriniStatusChange(endpoint, primaryKey, cosmosDbName, customerCollName, dataSourceCollName, dataDestCollName,lastvaluesCollName,shiftCollName,debug,StartTimeScheduler,JobLastRunString,telemetrykey):
    tc = TelemetryHelper(telemetrykey)
    try:
        STATUSCHANGE_FUNZIONE.STATUSCHANGE.STATUS(endpoint, primaryKey, cosmosDbName, customerCollName, dataSourceCollName, dataDestCollName,shiftCollName,lastvaluesCollName,debug,StartTimeScheduler,JobLastRunString,telemetrykey)
        return True
    except Exception as e:
        tc.track_exception()
        print(e)
        return False

def PeriniStatusChange_SQL(endpoint, primaryKey, cosmosDbName, customerCollName, dataSourceCollName, dataDestCollName,lastvaluesCollName,shiftCollName,debug,StartTimeScheduler,JobLastRunString,telemetrykey,cnxn):
    tc = TelemetryHelper(telemetrykey)
    try:
        Status_SQL.STATUSCHANGE_SQL.STATUS(endpoint, primaryKey, cosmosDbName, customerCollName, dataSourceCollName, dataDestCollName,shiftCollName,lastvaluesCollName,debug,StartTimeScheduler,JobLastRunString,telemetrykey,cnxn)
        return True
    except Exception as e:
        tc.track_exception()
        print(e)
        return False

def PeriniBladeChangeLauncher(endpoint, primaryKey, cosmosDbName, customerCollName, dataSourceCollName, dataDestCollName,lastvaluesCollName,shiftCollName,debug,StartTimeScheduler,JobLastRunString,telemetrykey):
    tc = TelemetryHelper(telemetrykey)
    try:
        res = BLADECHANGE_FUNZIONE.BLADECHANGE.BLADE(endpoint, primaryKey, cosmosDbName, customerCollName, dataSourceCollName, dataDestCollName,lastvaluesCollName,shiftCollName, debug, StartTimeScheduler,JobLastRunString, telemetrykey)
        return res
    except Exception as e:
        tc.track_exception()
        print(e)
        return False

def PeriniBladeChangeLauncher_SQL(endpoint, primaryKey, cosmosDbName, customerCollName, dataSourceCollName, dataDestCollName,lastvaluesCollName,shiftCollName,debug,StartTimeScheduler,JobLastRunString,telemetrykey,cnxn):
    tc = TelemetryHelper(telemetrykey)
    try:
        res = BladeChange_SQL.BLADECHANGE_SQL.BLADE(endpoint, primaryKey, cosmosDbName, customerCollName, dataSourceCollName, dataDestCollName,lastvaluesCollName,shiftCollName, debug, StartTimeScheduler,JobLastRunString, telemetrykey,cnxn)
        return res
    except Exception as e:
        tc.track_exception()
        print(e)
        return False

def PeriniAlert(endpoint, primaryKey, cosmosDbName, customerCollName, dataSourceCollName, dataDestCollName,lastvaluesCollName,shiftCollName,debug,StartTimeScheduler,telemetrykey):
    import time
    tc = TelemetryHelper(telemetrykey)        
    
    try:
        tic = time.time()
        tc.track_event('Alert','Starting Method...')
        Start = pd.to_datetime(StartTimeScheduler,utc=True)
        StartUnix = int(dt.datetime.timestamp(Start)*1000)
        StartMinus6Hours = pd.to_datetime(Start-pd.Timedelta(hours=6)) 
        StartMinus6HoursUnix = int(dt.datetime.timestamp(StartMinus6Hours)*1000)

        print("Start: " + str(Start) + " StartUnix: " + str(StartUnix))
        print("Start-6H: " + str(StartMinus6Hours) + " Start-6H Unix: " + str(StartMinus6HoursUnix))

        cosmosHelper = CDQH.CosmosHelper(endpoint, primaryKey, cosmosDbName,customerCollName,dataSourceCollName,dataDestCollName,lastvaluesCollName,shiftCollName,telemetrykey)
        cursor6hours = cosmosHelper.dataDestCollection.Select("SELECT * FROM container c WHERE c.date > " + str(StartMinus6HoursUnix) + " AND c.date < " + str(StartUnix))         #mettere il filtro sui dati delle ultime sei ore, aggiungere query StartMinus6HoursUnix
        DF_6_HOURS = pd.DataFrame(list(cursor6hours))
        if DF_6_HOURS.empty:
            print('Data not found')
            pass
        else:
            print('Data found')
            customers = DF_6_HOURS['customer'].unique()
            appended_alert=[]
            for cliente in customers:
                lines = DF_6_HOURS.loc[DF_6_HOURS['customer']==cliente,'line'].unique()
                for linea in lines:
                    machines = DF_6_HOURS.loc[(DF_6_HOURS['customer']==cliente) & (DF_6_HOURS['line']==linea),'machine'].unique()
                    for macchina in machines:
                        DF_STATUS = DF_6_HOURS.loc[(DF_6_HOURS['customer']==cliente) & (DF_6_HOURS['line']==linea) & (DF_6_HOURS['machine']==macchina),:].reset_index(drop=True)
                        if not DF_STATUS.empty:
                            ALERT = ALERT_FUNZIONE.ALERT_CALLER.SINGLE_ALERT(DF_STATUS,cliente,linea,macchina)
                            appended_alert.append(ALERT)
                        else:
                            print('Nessun dato trovato per la combinazione ' + str(cliente) + ' - ' + str(linea) + ' - ' + str(macchina))
            DF_ALERT = pd.concat(appended_alert).reset_index(drop=True,inplace=False).drop(['index'],axis=1,inplace=False)

        toc=time.time()
        print('Time elapsed: ' + str(toc-tic) + ' seconds')
        tc.track_event('Alert','End Method in: '+ str(toc-tic))
        return DF_ALERT.to_json()

    except Exception as e:
        tc.track_exception()
        print(e)
        return False
    
