import json
import pandas as pd
import numpy as np
import pprint
import operator
import math
import asyncio 
import time
import datetime as dt 
import pymongo 
from bson.objectid import ObjectId
import bson
from bson.json_util import loads
import CosmosDBQueryHelper as CDQH
from TelemetryHelper import TelemetryHelper
import json
import uuid

def ResampleAndClassify(m, des_old, des_new, StartTime,EndTime, debug,debugTime, tc,cosmosHelper):
    try:
            
        OLD = {}
        NEW = {}
        FINALE_JOIN = {}
        CONCAT_PIVOT = {}
        Dict_final = {}
        NOW = time.time()
        #print(m)
        OLD[m] = des_old.loc[des_old['machine'] == m,:]
        NEW[m] = des_new.loc[des_new['machine'] == m,:]
        FINALE_JOIN[m]= PERINICALC.Resampler(OLD[m],NEW[m],StartTime,EndTime,m,debug,debugTime, tc)
        Dict_final[m] = PERINICALC.Classifier(FINALE_JOIN[m],m, tc)
        print("Conversion JSON: %s" % (str(dt.datetime.now())))

        return Dict_final[m]
    except Exception as e:
        tc.track_exception()
        #print(str(OLD_DATA.loc[0,"customer"]))
        print(e)
        return None

class PERINICALC(object): 
    

    Tsample = '1S'
    V_Min_REW = 60
    V_Min_LS = 5
    v_Min_WR = 5
    v_Min_BUND = 5
    
    
    ############ IL DATAFRAME UNPIVOT CHE HO OTTENUTO LO BUTTO DENTRO A UNA FUNZIONE CHE FA IL PIVOT: QUESTO COLLOCA I DATI IN ORDINE CRONOLOGICO; A QUESTO PUNTO FACCIO IL RESAMPLING A 1S PER PROPAGARE I DATI, E 
    ############ CONTEMPORANEAMENTE CONTROLLO LA VARIABILE CLOCK PER CAPIRE SE LA MACCHINA E' IN UNO STATO NODATA OPPURE NO, PER FARE QUESTO CREO UN CONTATORE

   
    def Resampler(Old,New,Start,End,Machine,debug,debugTime, tc):      #END E START LE PASSA COME STRING, SE NO NON FUNZIONA UN CAZZO

        #print('START RESAMPLING: %s' % (str(dt.datetime.now())))
        #tc = TelemetryHelper(telemetrykey)
        #tc.track_event('Resampler','Starting Method...')
        StartTime = pd.to_datetime(Start,utc=True)                                         #Quando sarà il momento sostituire con StartTime e EndTime passati da Andrea D.
        ZeroTime = pd.to_datetime(StartTime - pd.Timedelta(seconds=1),utc=True)            #Inizializzo il vettore con i dati vecchi a 1S prima dell'inizio della finestra, in modo da non perdere                                                                                                                                  #eventuali cambiamenti di stato nell'istante StartTime            
        
        #La logica sullo stato NODATA è basata sull'arrivo del segnale STATUS_CLOCK: se il dato arriva la macchina è funzionante, se no bisogna aspettare 20 secondi prima di dire che la macchina è nello stato NODATA
        #Controllo se STATUS_CLOCK nei dati vecchi esiste, lo faccio per ogni cliente e per ogni linea


        if 'REW' in Machine:
            Globa_CLOCK_Variable = 'STATUS_CLOCK'
            Globa_VEL_Variable = 'STATISTIC_VEL_ACTUAL'
        elif 'LS' in Machine:
            Globa_CLOCK_Variable = 'STATUS_CLOCK'
            Globa_VEL_Variable = 'STATISTIC_VEL_ACTUAL'
        elif 'BUND' in Machine:
            Globa_CLOCK_Variable = 'i32LifeBit'   # VERIFICARE IL NOME CORRETTO  i32LifeBit
            Globa_VEL_Variable = 'i16VelACT'
        elif 'WRAP' in Machine:
            Globa_CLOCK_Variable = 'i16LifeBit'   # VERIFICARE IL NOME CORRETTO  i16LifeBit
            Globa_VEL_Variable = 'i16VelACT'
        
        Customer = Old['customer'].unique()
        
        if len(Customer) > 0:
            customer = Customer[0]
            DELTA={}
            FINALE_JOIN2 = pd.DataFrame()
            linee = Old['line'].unique()
            for line in linee:
                DELTA[line]={}
                if Globa_CLOCK_Variable in Old.loc[(Old['line']==line),'variabile'].unique():            
                        #Se c'è la variabile STATUS_CLOCK per una determinata linea allora salvo l'istante in cui è arrivata  (CUSTOMER e MACCHINA sono già filtrati)
                        LAST_STATUS_CLOCK = pd.to_datetime(max(Old.loc[(Old['variabile']==Globa_CLOCK_Variable) & (Old['line']==line),'date']),utc=True)                                                         
                        Old.loc[(Old['variabile']==Globa_CLOCK_Variable) & (Old['line']==line),'val']=1      
                        #E pongo STATUS_CLOCK a 1  
                        DELTA[line]= pd.to_timedelta(ZeroTime - LAST_STATUS_CLOCK,unit='ns').total_seconds()
                        #Controllo quanto tempo è passato fra l'ultimo STATUS_CLOCK e ZeroTime
                        if DELTA[line] <= 20:                    #Se inferiore a 20S
                            DF_ZERO = pd.DataFrame(data={'customer':customer,'line':line,'machine':Machine,'date':ZeroTime,'variabile':['COUNTER','NODATA'],'val':[DELTA[line],0]})
                            Old=pd.concat([Old,DF_ZERO],ignore_index=True)    
                            #aggiungo a ZeroTime una riga COUNTER che vale (ZeroTime-LAST_STATUS_CLOCK_REW) secondi, e una riga NODATA=0 perchè non c'è ancora anomalia
                        else:                               #Se è superiore ho sempre il contatore, e in questo caso NODATA = 1
                            DF_ZERO = pd.DataFrame(data={'customer':customer,'line':line,'machine':Machine,'date':ZeroTime,'variabile':['COUNTER','NODATA'],'val':[DELTA[line],1]})
                            Old=pd.concat([Old,DF_ZERO],ignore_index=True)
                else:                                                     #se non esiste la variabile STATUS_CLOCK
                        DF_ZERO = pd.DataFrame(data={'customer':customer,'line':line,'machine':Machine,'date':ZeroTime,'variabile':['COUNTER','NODATA'],'val':[666,1]})       #666, se si inizializza a np.nan non pivotta il valore!!
                        Old=pd.concat([Old,DF_ZERO],ignore_index=True)    
                        #aggiungo a ZeroTime una riga COUNTER che vale (ZeroTime-LAST_STATUS_CLOCK_REW) secondi, e una riga NODATA = 1 perchè c'è una anomalia

            EndTime = pd.to_datetime(End,utc=True)            #I 20 secondi possono essere cominciati nel batch di dati precedente, devo perciò tenere conto del datetime in cui è arrivato lo STATUS_CLOCK dell'ultimo vettore    

            Old['date']=ZeroTime       #Inizializzo il vettore con i dati vecchi a 4S prima dell'inizio della finestra, in modo da non perdere eventuali cambiamenti di stato nell'istante StartTime
            New['date']=pd.to_datetime(New['date'],utc=True)   #conversione, per non fare casini

            Old.replace("true",1,inplace=True)
            Old.replace("false",0,inplace=True)
            New.replace("true",1,inplace=True)
            New.replace("false",0,inplace=True)

            CONCAT = pd.concat([Old,New]).reset_index(drop=True) 
            CONCAT['val'] =  pd.to_numeric(CONCAT['val'])           #concateno i dati vecchi con quelli nuovi

        
            #Faccio il pivot! La colonna delle variabili scompare, e aggiungo una colonna per variabile con i relativi valori
            CONCAT_PIVOT = pd.pivot_table(CONCAT,values='val',index=['customer','line','machine','date'],columns='variabile').reset_index(drop=False,inplace=False)  

            #Assegno il datetime
            CONCAT_PIVOT['date']=pd.to_datetime(CONCAT_PIVOT['date'],utc=True)

            #Metto l'elenco delle variabili che possono arrivare
            if 'REW' in Machine:
                VARIABLES = ['STATISTIC_ACC_LEVELACT', 'STATISTIC_LOG_PRODUCED', 'STATISTIC_LOG_REJECTED', 'STATISTIC_UNW1_REEL_ACTPERC', 'STATISTIC_UNW2_REEL_ACTPERC', 'STATISTIC_UNW3_REEL_ACTPERC','STATISTIC_UNW4_REEL_ACTPERC','STATISTIC_VEL_ACTUAL',
                        'STATUS_RUN','STATUS_FASTOP','STATUS_STOPBYEMERGENCY','STATUS_STOPBYFAULT','STATISTIC_FIRST_ALARM','STATUS_STOPBYOPERATOR', 'STATUS_STOPBYMATERIAL', 'STATUS_STOPBYEXIT', 'PRODUCTDATA_ACTUALRECIPE_INDICE', 'STATUS_CHANGEPRODUCTONGOING', 'STATUS_CLOCK', 'STATUS_RUN','PRODUCTDATA_CORE_DIAMETER','PRODUCTDATA_LOG_DIAMETER','PRODUCTDATA_PERF_LENGHT','PRODUCTDATA_SHEET_NUMBER']    

            if 'LS' in Machine:
                VARIABLES = ['STATISTIC_VEL_ACTUAL','STATUS_RUN','STATUS_FASTOP','STATUS_STOPBYEMERGENCY','STATUS_STOPBYFAULT','STATISTIC_FIRST_ALARM','STATUS_STOPBYOPERATOR', 'STATUS_STOPBYEXIT', 'STATUS_STOPBYENTRY', 'PRODUCTDATA_ACTUALRECIPE_INDICE', 'STATUS_CHANGEPRODUCTONGOING', 'STATUS_CLOCK', 'STATISTIC_BLADE_ACTPERC','PRODUCTDATA_LOG_DIAMETER','PRODUCTDATA_CORE_DIAMETER'] 
        
            if 'WRAP' in Machine:
                VARIABLES = ['i16LifeBit','i16VelACT','boStopByFault','boRun','boStopByEmergency','i16AlarmCODE','boStopByOperator','boStopByMaterial', 'boStopByExit', 'boStopByEntry', 'i16ActualRecipeIndex', 'boChangeFormatEnable']  
        
            if 'BUND' in Machine:
                VARIABLES = ['i32LifeBit','i16VelACT','boStopByFault','boRun','boStopByEmergency','i16AlarmCODE','boStopByOperator','boStopByMaterial', 'boStopByExit', 'boStopByEntry', 'i16ActualRecipeIndex', 'boChangeFormatEnable']
        
            for variable in VARIABLES:
                if variable in CONCAT_PIVOT.columns:
                    pass
                else:
                    CONCAT_PIVOT[variable]=np.nan
        
            #Divido la velocità per 10
            CONCAT_PIVOT.loc[CONCAT_PIVOT['machine']==Machine,Globa_VEL_Variable]=CONCAT_PIVOT.loc[CONCAT_PIVOT['machine']==
                                                                                                        Machine,Globa_VEL_Variable]/10

            #Divido core e log diameter per 10     #FARE QUALCHE TEST, PER ORA DA ERRORE!!  
        
            if 'LS' in Machine:
                CONCAT_PIVOT.loc[CONCAT_PIVOT.machine.str.contains('LS'),'PRODUCTDATA_CORE_DIAMETER']=CONCAT_PIVOT.loc[CONCAT_PIVOT.machine.str.contains('LS'),'PRODUCTDATA_CORE_DIAMETER']/10
                CONCAT_PIVOT.loc[CONCAT_PIVOT.machine.str.contains('LS'),'PRODUCTDATA_LOG_DIAMETER']=CONCAT_PIVOT.loc[CONCAT_PIVOT.machine.str.contains('LS'),'PRODUCTDATA_LOG_DIAMETER']/10
            elif Machine == 'REW':
                CONCAT_PIVOT.loc[CONCAT_PIVOT['machine']=='REW','PRODUCTDATA_CORE_DIAMETER']=CONCAT_PIVOT.loc[CONCAT_PIVOT['machine']==
                                                                                                            'REW','PRODUCTDATA_CORE_DIAMETER']/10
                CONCAT_PIVOT.loc[CONCAT_PIVOT['machine']=='REW','PRODUCTDATA_LOG_DIAMETER']=CONCAT_PIVOT.loc[CONCAT_PIVOT['machine']==
                                                                                                            'REW','PRODUCTDATA_LOG_DIAMETER']/10
                CONCAT_PIVOT.loc[CONCAT_PIVOT['machine']=='REW','PRODUCTDATA_PERF_LENGHT']=CONCAT_PIVOT.loc[CONCAT_PIVOT['machine']==
                                                                                                            'REW','PRODUCTDATA_PERF_LENGHT']/10

            RANGE = list(pd.date_range(start=ZeroTime,end=EndTime,freq='1S'))
            #print("---> Resample: ZEROTIME= " + str(ZeroTime))
            #print("--->Resample: ENDTIME= " + str(EndTime))
        
            #Inizializzo due dizionari di dizionari:
            DATAFRAME_TOTALE_INIZIALE = {}
            DATAFRAME_TOTALE_FINALE = {}

            for line in CONCAT_PIVOT['line'].unique():    #container per LINEA
                #il dataframe lo costruisco prendendo gli elementi del dataframe CONCAT_PIVOT con i filtri CLIENTE-LINEA-MACCHINA: ogni dataframe di questo tipo ha una lista di date unica e che non si ripete, necessaria per il resampling
                    DATAFRAME_TOTALE_INIZIALE[line]= CONCAT_PIVOT.loc[(CONCAT_PIVOT['line']==line),:]   
                    DF = DATAFRAME_TOTALE_INIZIALE[line].set_index('date',drop=True,inplace=False)

                #Creo il dataframe che fa da scheletro a tutto il resto, e faccio il join con gli elementi selezionati
                    DF_DUMMY = pd.DataFrame(data=RANGE,columns=['date']).set_index('date',drop=True,inplace=False)
                    DF_CLEAN = DF.drop([Globa_CLOCK_Variable,'COUNTER','NODATA'],axis=1, inplace=False)
                    DF_COUNTER = DF[[Globa_CLOCK_Variable,'COUNTER','NODATA']]
                    DF_JOIN_CLEAN = DF_DUMMY.join(DF_CLEAN,how = 'outer')
                    DF_JOIN_COUNTER = DF_DUMMY.join(DF_COUNTER,how='outer')
                    
                    COUNTER = DF_JOIN_COUNTER.iloc[0,1]                          #Inizializzo il contatore
                
                    for i in range(0,DF_JOIN_COUNTER.shape[0]):                  #Espando in modo "personalizzato" solo STATUS_CLOCK e COUNTER
                        if not np.isnan(DF_JOIN_COUNTER.iloc[i,0]):
                            COUNTER = 0
                            DF_JOIN_COUNTER.iloc[i,1] = COUNTER
                        else:
                            DF_JOIN_COUNTER.iloc[i,1] = COUNTER
                            COUNTER = COUNTER + 1

                    #DF_JOIN_COUNTER.to_csv(r'C:\Users\l.savio\Desktop\ProvaMONGO\COUNTER_1S.csv',index=False)

                    NODATA = []          #inizializzo a zero, era già stato calcolato ma per l'istante precedente

                    dict = {True:0,False:1}  
#                    DF_JOIN_COUNTER.loc[DF_JOIN_COUNTER["COUNTER"]<=20]      
                    j=0
                    for i in DF_JOIN_COUNTER.index:
                        NODATA.append(dict[DF_JOIN_COUNTER.loc[i,'COUNTER'] <= 20])
                        if NODATA[j]==1:
                            if j-20 < 0:
                                NODATA[:j]=np.ones(j,int)
                            else:
                                NODATA[j-20:j]=np.ones(20,int)
                        else:
                            pass
                        j=j+1

                        #if DF_JOIN_COUNTER.loc[i,'COUNTER'] <= 20: 
                        #        NODATA.append(0)
                        #else:
                        #    NODATA.append(1)
                    #L = len(NODATA)
        
                    #for j in range(0,L):
                    #    if NODATA[j]==1:
                    #        if j-20 < 0:
                    #            NODATA[:j]=np.ones(j,int)
                    #        else:
                    #            NODATA[j-20:j]=np.ones(20,int)
                    #    else:
                    #        pass

                    DF_JOIN_COUNTER['NODATA']=NODATA
                    DF_COUNTER_RESAMPLED = DF_JOIN_COUNTER.iloc[1:,:].resample('4S').first()     #prima devo levare la prima riga, in modo da usare SOLO i dati all'interno della finestra; poi per il downsampling prendo solo il primo valore

                    DF_NODATA = DF_COUNTER_RESAMPLED #.drop(['COUNTER'],axis=1,inplace=False)   #decommentare per levare il contatore

                        #Ora mi dedico agli altri dati
                    #DF_NODATA = DTClassifier.NoData(CONCAT_PIVOT)
                #Faccio upsampling a 1S per tenere tutti i dati e propagarli, poi l'upsampling a 4S come richiesto
                    DF_FINALE = DF_JOIN_CLEAN.resample('1S').last()
                    DF_FINALE1 = DF_FINALE.fillna(method='ffill',inplace=False)
                    DF_FINALE2 = DF_FINALE1.iloc[1:,:].resample('4S').first()
                #Alla fine per riempire gli eventuali NaN metto un fillna, e poi droppo il datetimeindex per riaverlo come colonna
                    FINALE = DF_FINALE2.fillna(method='ffill',inplace=False)
                    FINALE_JOIN = FINALE.join(DF_NODATA,how='outer').reset_index(drop=False,inplace=False)
                #Se debug è a 0 l'algoritmo è in produzione, se è a 1 la variabile debug del DF finale vale l'orario di inizio dell'elaborazione dati (almeno riesco a distinguere i diversi test)
                    if debug == 0:
                        FINALE_JOIN['debug']=0
                    else:
                        FINALE_JOIN['debug']=debugTime
                #nelle righe con status NODATA == 1 setto la velocità a 0
                    for i in FINALE_JOIN[FINALE_JOIN["NODATA"]==1].index:
                        FINALE_JOIN.loc[i,Globa_VEL_Variable] = 0
                #Aggiungo SPEED_TARGET, LOG_PLANNED
                    SpeedTarget = 400                           #Aggiungere puntamento all'anagrafica per i turni                    
                    if Machine == 'REW':
                        FINALE_JOIN['SPEED_TARGET']=SpeedTarget
                        LOGS = []
                        for row in range(0,FINALE_JOIN.shape[0]):
                            if (FINALE_JOIN.loc[row,'PRODUCTDATA_PERF_LENGHT']*FINALE_JOIN.loc[row,'PRODUCTDATA_SHEET_NUMBER'] != 0):
                                Log = FINALE_JOIN.loc[row,'SPEED_TARGET']/(FINALE_JOIN.loc[row,'PRODUCTDATA_PERF_LENGHT']*FINALE_JOIN.loc[row,'PRODUCTDATA_SHEET_NUMBER']/1000)
                            elif (FINALE_JOIN.loc[row,'SPEED_TARGET'] != 0):
                                Log = np.nan
                            else:
                                Log = 0

                            LOGS.append(Log)
                        FINALE_JOIN['LOGS_PLANNED']=LOGS
                    else:
                        FINALE_JOIN['SPEED_TARGET']=np.nan

                    if FINALE_JOIN2.size > 0:
                        FINALE_JOIN2 = pd.concat([FINALE_JOIN2,FINALE_JOIN]).reset_index(drop=True) 
                    else:
                        FINALE_JOIN2 = FINALE_JOIN
                    
        #print('END RESAMPLING: %s' % (str(dt.datetime.now())))
        return FINALE_JOIN2 


    def Classifier (FINALE_JOIN_RESAMPLE,Machine,tc):
            #print('START CLASSIFYING: %s' % (str(dt.datetime.now())))
            #import json
            #import pandas as pd
            #import numpy as np
            #import pprint
            #import operator
            #import math
            #import datetime as dt 
            #import pymongo 
            #from bson.objectid import ObjectId
            #import bson
            #from bson.json_util import loads
            #from TelemetryHelper import TelemetryHelper
       
            #tc = TelemetryHelper(telemetrykey)
            tc.track_event('Classifier','Starting Method...')
    #Definisco delle variabili "globali" che possono andare bene per ogni macchina, in modo da snellire l'algoritmo
            if 'REW' in Machine:
                Global_VEL_Variable = 'STATISTIC_VEL_ACTUAL'
                Global_STOPBYFAULT = 'STATUS_STOPBYFAULT' 
                Global_STATUSRUN = 'STATUS_RUN'
                Global_STOPBYEMERGENCY ='STATUS_STOPBYEMERGENCY'
                Global_FIRSTALARM = 'STATISTIC_FIRST_ALARM'
                Global_STOPBYOPERATOR = 'STATUS_STOPBYOPERATOR'
                Global_STOPBYMATERIAL = 'STATUS_STOPBYMATERIAL'
                Global_STOPBYEXIT = 'STATUS_STOPBYEXIT'
                Global_LEVELACT ='STATISTIC_ACC_LEVELACT'  # SOLO PER REW
                GLOBAL_RECIPE = 'PRODUCTDATA_ACTUALRECIPE_INDICE'
                GLOBAL_CHANGEPRODUCT = 'STATUS_CHANGEPRODUCTONGOING'
                Global_ALARMCONDITION = 'STATUS_STOPBYEMERGENCY'
                Global_STOPBYUPSTREAM = 'STATUS_STOPBYMATERIAL'
                V_Min = 60
                V_Min_Alarm = 25

            if 'LS' in Machine:
                Global_VEL_Variable = 'STATISTIC_VEL_ACTUAL'
                Global_STOPBYFAULT = 'STATUS_STOPBYFAULT' 
                Global_STATUSRUN = 'STATUS_RUN'
                Global_STOPBYEMERGENCY ='STATUS_STOPBYEMERGENCY'
                Global_FIRSTALARM = 'STATISTIC_FIRST_ALARM'
                Global_STOPBYOPERATOR = 'STATUS_STOPBYOPERATOR'
                Global_STOPBYMATERIAL = 'STATUS_STOPBYMATERIAL'
                Global_STOPBYEXIT = 'STATUS_STOPBYEXIT'
                GLOBAL_RECIPE = 'PRODUCTDATA_ACTUALRECIPE_INDICE'
                GLOBAL_CHANGEPRODUCT = 'STATUS_CHANGEPRODUCTONGOING'
                Global_ALARMCONDITION = 'STATUS_STOPBYEMERGENCY'
                Global_STOPBYUPSTREAM = 'STATUS_STOPBYENTRY'
                V_Min = 5
                V_Min_Alarm = 3
               
            if 'BUND' in Machine:
                Global_VEL_Variable = 'i16VelACT'
                Global_STOPBYFAULT = 'boStopByFault' 
                Global_STATUSRUN = 'boRun'
                Global_STOPBYEMERGENCY ='boStopByEmergency'
                Global_FIRSTALARM = 'i16AlarmCODE'
                Global_STOPBYOPERATOR = 'boStopByOperator'
                Global_STOPBYMATERIAL = 'boStopByMaterial'
                Global_STOPBYEXIT = 'boStopByExit'
                Global_STOPBYENTRY = 'boStopByEntry'
                GLOBAL_RECIPE = 'i16ActualRecipeIndex'
                GLOBAL_CHANGEPRODUCT = 'boChangeFormatEnable'  
                Global_ALARMCONDITION = 'boStopByMaterial'
                Global_STOPBYUPSTREAM = 'boStopByEntry'
                V_Min = 5
                V_Min_Alarm = 3
                
            if 'WRAP' in Machine:
                Global_VEL_Variable = 'i16VelACT'
                Global_STOPBYFAULT = 'boStopByFault' 
                Global_STATUSRUN = 'boRun'
                Global_STOPBYEMERGENCY ='boStopByEmergency'
                Global_FIRSTALARM = 'i16AlarmCODE'
                Global_STOPBYOPERATOR = 'boStopByOperator'
                Global_STOPBYMATERIAL = 'boStopByMaterial'
                Global_STOPBYEXIT = 'boStopByExit'
                Global_STOPBYENTRY = 'boStopByEntry'
                GLOBAL_RECIPE = 'i16ActualRecipeIndex'
                GLOBAL_CHANGEPRODUCT = 'boChangeFormatEnable'
                Global_ALARMCONDITION = 'boStopByMaterial'
                Global_STOPBYUPSTREAM = 'boStopByEntry'
                V_Min = 5
                V_Min_Alarm = 3
              
            dict = {True:1,False:0}  
            #DATAFRAME_TOTALE_FINALE={}
            DF = pd.DataFrame()
            for line in FINALE_JOIN_RESAMPLE['line'].unique():
                DOWNTIME = []
                OPERATOR = []
                MATERIAL = []
                DOWNSTREAM = []
                UPSTREAM = []
                          #dizionario per mappare le variabili con True e False
                FINALE_JOIN = FINALE_JOIN_RESAMPLE.loc[FINALE_JOIN_RESAMPLE['line'] == line].reset_index(drop= True)
                #Logica per gli allarmi 
                for i in range(0,FINALE_JOIN.shape[0]):

                    if (FINALE_JOIN.loc[i,Global_VEL_Variable] <= V_Min):
                        DOWNTIME.append(1)
                        OPERATOR.append(dict[FINALE_JOIN.loc[i,Global_STOPBYOPERATOR] == 1])            #se la condizione è True metto a 1, se no a 0
                
                        if Machine == 'REW':
                            DOWNSTREAM.append(dict[(FINALE_JOIN.loc[i,Global_STOPBYEXIT] == 1) | (FINALE_JOIN.loc[i,Global_LEVELACT] >= 95)])
                            UPSTREAM.append(0)
                        else:
                            DOWNSTREAM.append(dict[FINALE_JOIN.loc[i,Global_STOPBYEXIT] == 1])
                            UPSTREAM.append(dict[FINALE_JOIN.loc[i,Global_STOPBYUPSTREAM] == 1])    

                    else:
                        DOWNTIME.append(0)
                        OPERATOR.append(0)
                        DOWNSTREAM.append(0)
                        UPSTREAM.append(0)

                FINALE_JOIN['DOWNTIME']=DOWNTIME
                PRODUCT = [FINALE_JOIN.loc[0,GLOBAL_RECIPE]]                   
                ALARM = [FINALE_JOIN.loc[0,Global_FIRSTALARM ]]                 

                for j in range(1,FINALE_JOIN.shape[0]):

                    if(FINALE_JOIN.loc[j,'DOWNTIME'] == 1):
                        PRODUCT.append(dict[((FINALE_JOIN.loc[j,GLOBAL_RECIPE]!=FINALE_JOIN.loc[j-1,GLOBAL_RECIPE]) | (FINALE_JOIN.loc[j,GLOBAL_CHANGEPRODUCT]==1)) and not(math.isnan(FINALE_JOIN.loc[j,GLOBAL_RECIPE])) and not(math.isnan(FINALE_JOIN.loc[j-1,GLOBAL_RECIPE]))])

                        ALARM.append(dict[(((FINALE_JOIN.loc[j,Global_STOPBYFAULT] == 1) or (FINALE_JOIN.loc[j,Global_STOPBYEMERGENCY] == 1) or (FINALE_JOIN.loc[j,Global_ALARMCONDITION] == 1)) or ((FINALE_JOIN.loc[j,Global_VEL_Variable] >= V_Min_Alarm) and (FINALE_JOIN.loc[j,Global_FIRSTALARM] != FINALE_JOIN.loc[j-1,Global_FIRSTALARM]))) and (not(math.isnan(FINALE_JOIN.loc[j,Global_FIRSTALARM])) and not(math.isnan(FINALE_JOIN.loc[j-1,Global_FIRSTALARM])) and (FINALE_JOIN.loc[j,Global_FIRSTALARM] != 0) and (FINALE_JOIN.loc[j-1,Global_FIRSTALARM] != 0))])
   
                    else:
                        PRODUCT.append(0)
                        ALARM.append(0)

                FINALE_JOIN['PRODUCT']=PRODUCT
                FINALE_JOIN['DOWNSTREAM']=DOWNSTREAM
                FINALE_JOIN['UPSTREAM']=UPSTREAM
                FINALE_JOIN['OPERATOR']=OPERATOR                  
                FINALE_JOIN['ALARM']=ALARM

                FINALE_JOIN['STATUS']='RUN'
                FINALE_JOIN.loc[FINALE_JOIN['DOWNTIME']==1,'STATUS']='UNKNOWN DOWNTIME'
                FINALE_JOIN.loc[FINALE_JOIN['ALARM']==1,'STATUS']='STOP BY ALARM'
                FINALE_JOIN.loc[FINALE_JOIN['OPERATOR'] == 1,'STATUS']='STOP BY OPERATOR'
                FINALE_JOIN.loc[FINALE_JOIN['UPSTREAM'] == 1,'STATUS']='STOP BY UPSTREAM MACHINE'
                FINALE_JOIN.loc[FINALE_JOIN['DOWNSTREAM'] == 1,'STATUS']='STOP BY DOWNSTREAM MACHINE'
                FINALE_JOIN.loc[FINALE_JOIN['PRODUCT'] == 1,'STATUS']='PRODUCTCHANGE'
                FINALE_JOIN.loc[FINALE_JOIN['NODATA'] == 1,'STATUS']='NODATA'


                #HIERARCHICAL_WARNING=[]
                #for i in range(0,FINALE_JOIN.shape[0]):
                #    if FINALE_JOIN.loc[i,'NODATA'] == 1:
                #        HIERARCHICAL_WARNING.append('NODATA')
                #    else:
                #        if FINALE_JOIN.loc[i,'PRODUCT'] == 1:
                #            HIERARCHICAL_WARNING.append('PRODUCTCHANGE')
                #        else:
                #            if FINALE_JOIN.loc[i,'DOWNSTREAM'] == 1:
                #                HIERARCHICAL_WARNING.append('STOP BY DOWNSTREAM MACHINE')
                #            else:
                #                if FINALE_JOIN.loc[i,'UPSTREAM'] == 1:
                #                    HIERARCHICAL_WARNING.append('STOP BY UPSTREAM MACHINE')
                #                else:
                #                    if FINALE_JOIN.loc[i,'OPERATOR'] == 1:
                #                        HIERARCHICAL_WARNING.append('STOP BY OPERATOR')
                #                    else:
                #                        if FINALE_JOIN.loc[i,'ALARM']==1:
                #                            HIERARCHICAL_WARNING.append('STOP BY ALARM')
                #                        else:
                #                            if FINALE_JOIN.loc[i,'DOWNTIME']==1:
                #                                HIERARCHICAL_WARNING.append('UNKNOWN DOWNTIME')
                #                            else:
                #                                HIERARCHICAL_WARNING.append('RUN')

                #FINALE_JOIN['STATUS']=HIERARCHICAL_WARNING

                DF = pd.concat([DF,FINALE_JOIN],ignore_index=True)

            #DF = []

            #for line in DATAFRAME_TOTALE_FINALE:
            #        for i in range(0,DATAFRAME_TOTALE_FINALE[line].shape[0]):
            #            DF.append(DATAFRAME_TOTALE_FINALE[line].loc[i,:])

        #Costruisco un dataframe con la lista di dataframe che ho ottenuto 

            DF['Years']=DF['date'].dt.strftime('%Y')
            DF['Months']=DF['date'].dt.strftime('%m')
            DF['Days']=DF['date'].dt.strftime('%d')
            DF['Hours']=DF['date'].dt.strftime('%H')
            DF['Minutes']=DF['date'].dt.strftime('%M')
            DF['Seconds']=DF['date'].dt.strftime('%S')

            #Aggiungere puntamento all'anagrafica per i turni

            #DF_SHIFTS =pd.DataFrame(data={'ShiftID':['Shift1','Shift2','Shift3'],'ValidFrom':['6','14','22'],'ValidEnd':['14','22','6'],'PlannedStartTime':['6','14','22'],'PlannedEndTime':['14','22','6']})
                        
            DF['SHIFT']='Shift3'
            DF.loc[(((DF['Hours']).astype(int))>=6) & (((DF['Hours']).astype(int)<14)) ,'SHIFT']='Shift1'
            DF.loc[(((DF['Hours']).astype(int))>=14)  & (((DF['Hours']).astype(int)<22)) ,'SHIFT']='Shift2'

            #DF.loc[row,'Hours']) >= 6) and (int(DF.loc[row,'Hours']) < 14)
            #for row in range(0,DF.shape[0]):
            #      if (int(DF.loc[row,'Hours']) >= 6) and (int(DF.loc[row,'Hours']) < 14):
            #          shift.append('Shift1')
            #      elif (int(DF.loc[row,'Hours']) >= 14) and (int(DF.loc[row,'Hours']) < 22):
            #          shift.append('Shift2')
            #      else:
            #          shift.append('Shift3')

            #DF['SHIFT']=shift
            DF['REEL_CHANGE_1']=0         #inizializzo le variabili, che poi verranno riempite dai programmi schedulati successivamente
            DF['REEL_CHANGE_2']=0
            DF['REEL_CHANGE_3']=0
            DF['REEL_CHANGE_4']=0
            DF['BLADE_CHANGE']=0
            DF['STATUS_CHANGE']=-99
            DF['ALARM_CHANGE']=0
            DF['debugREEL']=-99
            DF['debugBLADE']=-99
            DF['debugSTATUS']=-99

            #print('END CLASSIFYING: %s' % (str(dt.datetime.now())))
            return  DF


    def finder_by_id(collection, _id): #funzione per cercare per _id
        return [i for i in collection.find({"_id": ObjectId(str(_id))})]


    def MAIN(OLD_DATA,NEW_DATA,StartTime,EndTime,endpoint,primaryKey,cosmosDbName,customerCollName, dataSourceCollName, dataDestCollName,lastvalueCollName,shiftCollName,debug,debugTime,telemetrykey, jsonPathFolder,jsonPathFolder_BUNDWRAP, execguid):
        
        tc = TelemetryHelper(telemetrykey)

        try:
            
            tc.track_event('Main','Starting Method...')
            cosmosHelper = CDQH.CosmosHelper(endpoint, primaryKey, cosmosDbName, customerCollName, dataSourceCollName, dataDestCollName,lastvalueCollName,shiftCollName,telemetrykey)
     
            des_old = OLD_DATA#.loc[(OLD_DATA['machine'].str.contains('LS')) | (OLD_DATA['machine']=='REW'),:].reset_index(drop=True) #deserializer(OLD_DATA)    
            des_new = NEW_DATA#.loc[(NEW_DATA['machine'].str.contains('LS')) | (NEW_DATA['machine']=='REW'),:].reset_index(drop=True) #deserializer(NEW_DATA)
            #machinecheck = set(des_new['machine'].unique()+des_old['machine'].unique())
            customer = des_new['customer'].unique()
            machines = des_new['machine'].unique()
            OLD = {}
            NEW = {}
            FINALE_JOIN = {}
            #DATAFRAME_TOTALE_FINALE = {}
            CONCAT_PIVOT = {}
            Dict_final = {}
            #array_json = {}
            
            nestloop = asyncio.get_event_loop()
            if not isinstance(nestloop, asyncio.SelectorEventLoop):
                nestloop = asyncio.SelectorEventLoop() # for subprocess' pipes on Windows
                asyncio.set_event_loop(nestloop)
            
            #nestloop = asyncio.new_event_loop();
            #asyncio.set_event_loop(nestloop)
            tasks = []
            result=pd.DataFrame()
            result_BUNDWRAP=pd.DataFrame()
            for m in machines:
                if ("REW" in m) or ("LS" in m): 
                    result=pd.concat([result,ResampleAndClassify(m, des_old, des_new, StartTime,EndTime, debug,debugTime, tc, cosmosHelper)],ignore_index=True)
                if ("BUND" in m) or ("WRAP" in m):
                    result_BUNDWRAP=pd.concat([result_BUNDWRAP,ResampleAndClassify(m, des_old, des_new, StartTime,EndTime, debug,debugTime, tc, cosmosHelper)],ignore_index=True)
                
            #jsonPathFolder_BUNDWRAP = r"C:\Users\s.manai\Desktop\PERINIDATA_BUNDWRAP"
            if result_BUNDWRAP.size !=0:
                result_json_BUNDWRAP = loads(result_BUNDWRAP.to_json(orient='records'))
                strCust = str(customer).replace("/","")
                with open(r'%s\%s.json' % (jsonPathFolder_BUNDWRAP, execguid + str(strCust)), 'w') as outfile:
                    json.dump(result_json_BUNDWRAP, outfile)
            if result.size !=0:
                result_json = loads(result.to_json(orient='records'))
                strCust = str(customer).replace("/","")
                with open(r'%s\%s.json' % (jsonPathFolder, execguid + str(strCust)), 'w') as outfile:
                    json.dump(result_json, outfile)

            tc.track_event('Main','End Method...')
            return True
        except Exception as e:
            tc.track_exception()
            print(str(OLD_DATA.loc[0,"customer"]))
            print(e)
            return False

    
