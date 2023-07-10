import time, sched, os, shutil, smtplib, ssl, traceback
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import sqlalchemy
import pyodbc 
from six.moves import urllib
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from dateutil.relativedelta import relativedelta
import warnings
warnings.filterwarnings('ignore')

def db_reconnect():
    params = urllib.parse.quote_plus("DRIVER={SQL Server};SERVER=Gcaaod04.utcapp.com;DATABASE=OPS_DA;UID=S-OPS_DA_Prod;PWD=6ux@J2ms_Adm6$$")
    cnxn = pyodbc.connect(driver='{SQL Server}',server='Gcaaod04.utcapp.com',database='OPS_DA',uid='S-OPS_DA_Prod',pwd='6ux@J2ms_Adm6$$')
    engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params) 
    engine.connect()
    return cnxn

def push_to_db():
    TimeChk = 0
    FileChk = 0
    Counter = 0
    Message = ''
    SrcPath = r'\\yyz0sv02.goodrich.root.local\dept\In_House_Metrics\TABLEAU_REPORTS\PLANvsACTUAL'
    DestPath = r'\\yyz0sv02.goodrich.root.local\dept\In_House_Metrics\TABLEAU_REPORTS\Milestone_OTD\Backup'
    Src1 = SrcPath + os.path.sep + r'COOIS_Headers.txt'
    Src2 = SrcPath + os.path.sep + r'ZPPWIP_V4.txt'
    Src3 = SrcPath + os.path.sep + r'COOIS.txt'
    Src4 = SrcPath + os.path.sep + r'COOIS_Components.txt'
    Src5 = SrcPath + os.path.sep + r'COOIS_Sequences.txt'
    print("      " + datetime.now().strftime('%Y-%m-%d %H:%M:%S: Analysing source files ...'))
    
    while True:
        try:
            FTimeStamp = int(os.path.getmtime(Src4))
            COOIS_FileSize = os.path.getsize(Src3) / (1024 * 1024)
            print("      " + datetime.now().strftime('%Y-%m-%d %H:%M:%S: COOIS_FileSize'), COOIS_FileSize)
        except:
            print("      " + datetime.now().strftime('%Y-%m-%d %H:%M:%S: Failed to get source file Timestamp. Chk log file for errors. Retry in 1 minute ...'))
            logf = open("log.txt", "a")
            logf.write("Failed at- %s" % datetime.now())
            traceback.print_exc(file=logf)
            logf.close()    
            time.sleep(60)
        else:
            CTime = int(datetime.timestamp(datetime.now()))
            print("      " + datetime.now().strftime('%Y-%m-%d %H:%M:%S:'), 'Source files last updated before {} seconds'.format(CTime - FTimeStamp))
            if (CTime - FTimeStamp) < 3300 and COOIS_FileSize > 20:
                break
            else:
                print("      " + datetime.now().strftime('%Y-%m-%d %H:%M:%S: Waiting for source files to update. Retry in 1 minute ...'))
                time.sleep(60)
                
    print("      " + datetime.now().strftime('%Y-%m-%d %H:%M:%S: Starting source file copy ...'))
    try:
        # Copy files from server to backup folder
        BkupFolder = datetime.now().strftime('%Y%m%d%H%M%S')
        #BkupFolder = '20230127140139'
        Dest1 = DestPath + os.path.sep + BkupFolder + os.path.sep + r'COOIS_Headers.txt'
        Dest2 = DestPath + os.path.sep + BkupFolder + os.path.sep + r'ZPPWIP_V4.txt'
        Dest3 = DestPath + os.path.sep + BkupFolder + os.path.sep + r'COOIS.txt'
        Dest4 = DestPath + os.path.sep + BkupFolder + os.path.sep + r'COOIS_Components.txt'
        Dest5 = DestPath + os.path.sep + BkupFolder + os.path.sep + r'COOIS_Sequences.txt'
        os.makedirs(os.path.dirname(Dest1), exist_ok=True)
        while FileChk == 0 and Counter < 5:
            print("      " + 'os.path.exists(Src1)',os.path.exists(Src1))
            if os.path.exists(Src1) == True:
                shutil.copy2(Src1, Dest1)
                FileChk1 = 1
            else:
                FileChk1 = 0
            print("      " + 'os.path.exists(Src2)',os.path.exists(Src2))
            if os.path.exists(Src2) == True:
                shutil.copy2(Src2, Dest2)
                FileChk2 = 1
            else:
                FileChk2 = 0
            print("      " + 'os.path.exists(Src3)',os.path.exists(Src3))
            if os.path.exists(Src3) == True:
                shutil.copy2(Src3, Dest3)
                FileChk3 = 1
            else:
                FileChk3 = 0
            print("      " + 'os.path.exists(Src4)',os.path.exists(Src4))
            if os.path.exists(Src4) == True:
                shutil.copy2(Src4, Dest4)
                FileChk4 = 1
            else:
                FileChk4 = 0
            print("      " + 'os.path.exists(Src5)',os.path.exists(Src5))
            if os.path.exists(Src5) == True:
                shutil.copy2(Src5, Dest5)
                FileChk5 = 1
            else:
                FileChk5 = 0                
            FileChk = FileChk1*FileChk2*FileChk3*FileChk4*FileChk5
            if FileChk == 0:
                Counter += 1
                print("      " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"), ' File copy failed. Retry number {} (max 4) in 1 minute ...'.format(Counter))
                time.sleep(60)
        Message += '<p style="color:green;">' + datetime.now().strftime('%Y-%m-%d %H:%M:%S: Files Copied Successfully from source to destination and backup folders.</p>')
        print("      " + datetime.now().strftime('%Y-%m-%d %H:%M:%S: File Copy Successful ...'))
    except:
        logf = open("log.txt", "a")
        logf.write("Failed at- %s" % datetime.now())
        traceback.print_exc(file=logf)
        logf.close()
        Error = traceback.format_exc()
        Message += '<p style="color:red;">' + datetime.now().strftime('%Y-%m-%d %H:%M:%S: Failed to Copy Files.</p>') + Error
        print("      " + datetime.now().strftime('%Y-%m-%d %H:%M:%S: Failed to Copy Files ...'))
    
    def To_Datetime(DateStr,TimeStr):
        Time_Sec = 0
        DT_Object = np.nan
        if pd.notnull(TimeStr):
            try:
                TimeList = TimeStr.split(':')
                Time_Sec = int(TimeList[0])*3600 + int(TimeList[1])*60 + int(TimeList[2])
            except Exception as e:
                print("      " + 'TimeStr: ', TimeStr)
                print("      " + str(e))
                Time_Sec = 0
                
        if pd.notnull(DateStr):
            Date = datetime.strptime(DateStr, '%m/%d/%Y')
            if Time_Sec > 0:
                DT_Object = Date + timedelta(seconds=Time_Sec)
            else:
                DT_Object = Date
        return DT_Object
    try:
        Orderslist= []
        with open(Dest3) as f:
            for Line in f.readlines():
                Llist = Line.split('\t')[1:]
                for i in range(len(Llist)):
                    if 'PRD Header' not in Llist:
                        Llist[i] = Llist[i].replace('"', '').strip()
                    else:
                        Llist[i] = Llist[i].replace('\n', '')
                Orderslist.append(Llist)
        Header = Orderslist[3]
        Orderslist = Orderslist[4:]
        df_coois = pd.DataFrame(Orderslist)
        df_coois.columns = Header
        df_coois.dropna(subset=['Order'],inplace=True)
        df_coois.columns = pd.io.parsers.base_parser.ParserBase({'names':df_coois.columns, 'usecols':None})._maybe_dedup_names(df_coois.columns)
        df_coois = df_coois.infer_objects()
        df_coois['Order'] = df_coois['Order'].astype('int64')
        
        print("      " + datetime.now().strftime('%Y-%m-%d %H:%M:%S: Starting MileStone OTD Data Update ...'))
        moddate = time.ctime(os.path.getmtime(Dest3))
        Message += '<p style="color:blue;">Latest Source Data Timestamp is: ' + str(moddate) + '</p>'
        print("      " , '1  read master csv file')
        try:
            master = pd.read_csv(r'\\yyz0sv02.goodrich.root.local\dept\In_House_Metrics\TABLEAU_REPORTS\Master Files\Master_MileStones_v8.csv', delimiter=',',engine='python', encoding= 'unicode_escape')
        except Exception as e:
            print("      " , '1  read master error')
            raise ValueError(str(e))
        print("      " , '1  master - clean')
        master['heatKey']=master['Mach PN'].astype(str)+master['Heat_Treat_Activity'].astype(str)
        master['nitalKey']=master['Mach PN'].astype(str)+master['Nital_Etch_Activity'].astype(str)
        master['primeKey']=master['Mach PN'].astype(str)+master['Prime/Bush_Activity'].astype(str)
        ops_df=df_coois.copy()
        ops_df=ops_df[['Order','Activity','LatestStrt','LatestFin.','Yield','Actl Start','Act. start','Act.finish','Act.finish.1','Act.finish.2']]
        ops_df['Activity']=ops_df['Activity'].apply(lambda x: str(x).lstrip("0"))
        
        print("      " , '1  read zppwip data')
        
        try:
            #master = pd.read_csv(r'\\yyz0sv02.goodrich.root.local\dept\In_House_Metrics\TABLEAU_REPORTS\Master Files\Master_MileStones_v8.csv', delimiter=',',engine='python', encoding= 'unicode_escape')
            zppwip=pd.read_csv(Dest2,delimiter='\t',skiprows=5,engine='python',warn_bad_lines=True, error_bad_lines=False,encoding='cp1252')
        
        except Exception as e:
            print("      " , '1  read zppwip error')
            raise ValueError(str(e))
            
        if 'Serial No.' in zppwip.columns:
            zppwip['Serial Number'] = zppwip['Serial No.']
        zppwip=zppwip[['Order','Serial Number', 'Current Op']]
        zppwip=zppwip.dropna(subset=['Order'])
        ops_df['Serial Number']=ops_df['Order'].map(zppwip[['Order','Serial Number']].set_index('Order').to_dict()['Serial Number'])
        ops_df['Current Op']=ops_df['Order'].map(zppwip[['Order','Current Op']].set_index('Order').to_dict()['Current Op'])
        
        
        print("       ", '1  read hd_df data')
        
        try:
            #master = pd.read_csv(r'\\yyz0sv02.goodrich.root.local\dept\In_House_Metrics\TABLEAU_REPORTS\Master Files\Master_MileStones_v8.csv', delimiter=',',engine='python', encoding= 'unicode_escape')
            hd_df=pd.read_csv(Dest1,delimiter='\t',skiprows=3,engine='python',warn_bad_lines=True, error_bad_lines=False,encoding='cp1252')
        
        except Exception as e:
            print("       ", '1  read hd_df error')
            raise ValueError(str(e))
        
        if 'SerNo' in hd_df.columns:
            hd_df['Serial no.'] = hd_df['SerNo']
        hd_df=hd_df[['Order','Material','Order Text','Nxt Asy St','Leg Rel Dt','Serial no.','Act. Start','Act.finish','Alert Text']]
        hd_df=hd_df.dropna(subset=['Order'])    
        ops_df['Material']=ops_df['Order'].map(hd_df[['Order','Material']].set_index('Order').to_dict()['Material'])
        ops_df['Serial no.']=ops_df['Order'].map(hd_df[['Order','Serial no.']].set_index('Order').to_dict()['Serial no.'])   
        ops_df=ops_df[ops_df['Material'].notna()]
        ops_df['Order Text']=ops_df['Order'].map(hd_df[['Order','Order Text']].set_index('Order').to_dict()['Order Text'])
        ops_df['Nxt Asy St']=ops_df['Order'].map(hd_df[['Order','Nxt Asy St']].set_index('Order').to_dict()['Nxt Asy St'])
        ops_df['Leg Rel Dt']=ops_df['Order'].map(hd_df[['Order','Leg Rel Dt']].set_index('Order').to_dict()['Leg Rel Dt'])
        ops_df['ActKey']=ops_df['Material'].astype(str)+ops_df['Activity'].astype(str)
        ops_df['Activity type']=ops_df['ActKey'].apply(lambda x: 'Heat Treat' if x in master['heatKey'].values else ('Nital Etch' if x in master['nitalKey'].values else ('Prime' if x in master['primeKey'].values else None)))
        ops_df=ops_df[ops_df['Activity type'].notna()]
        ops_df['Material Description']=ops_df['Material'].map(master[['Mach PN','Material Description']].set_index('Mach PN').to_dict()['Material Description'])
        ops_df['Mat Group']=ops_df['Material'].map(master[['Mach PN','Mat Group']].set_index('Mach PN').to_dict()['Mat Group'])
        ops_df['Prog']=ops_df['Material'].map(master[['Mach PN','Prog']].set_index('Mach PN').to_dict()['Prog'])
        ops_df['Cell']=ops_df['Material'].map(master[['Mach PN','Cell']].set_index('Mach PN').to_dict()['Cell']) 
        ops_df['Alert Text']=ops_df['Order'].map(hd_df[['Order','Alert Text']].set_index('Order').to_dict()['Alert Text'])
        ops_df['Time stamp']=moddate
        ops_df['Time stamp']=pd.to_datetime(ops_df['Time stamp'])
        ToDB = True
        Counter = 0
        while ToDB and Counter<10:
            try:
                
                ops_df.to_excel(os.path.join(BeforePushDB,f'ops_df_{datetime.now().strftime("%d_%m_%Y_%H_%M_%S")}.xlsx'))
                if Push_Milestone_Consolidated:
                    print("      " + datetime.now().strftime('%Y-%m-%d %H:%M:%S: MileStone OTD Data DB Push Start...'))
                    ops_df.to_sql(name='Milestone_Consolidated',schema='SIOP',index=False,con=engine,if_exists='append',chunksize=36,method='multi')
                    print("      " + datetime.now().strftime('%Y-%m-%d %H:%M:%S: MileStone OTD Data Update Successful ...'))
                    Message += '<p style="color:green;">' + datetime.now().strftime('%Y-%m-%d %H:%M:%S: Milestone Consolidated Data successfully updated.</p>')
                else:
                    print("      " + datetime.now().strftime('%Y-%m-%d %H:%M:%S: MileStone OTD Data DB Push SKIP...'))
                    Message += '<p style="color:green;">' + datetime.now().strftime('%Y-%m-%d %H:%M:%S: Milestone Consolidated Data - SKIPPED.</p>')
                Counter = 0
                ToDB = False
            except Exception as e:
                # Print any error messages to stdout
                print("      " + str(e))
                logf = open("log.txt", "a")
                logf.write("Failed at- %s" % datetime.now())
                traceback.print_exc(file=logf)
                logf.close()
                Counter += 1
                time.sleep(1)
                continue
        if Counter == 10:
            Message += '<p style="color:red;">' + datetime.now().strftime('%Y-%m-%d %H:%M:%S: Failed to update Milestone Consolidated Data.</p>')
            print("      " + datetime.now().strftime('%Y-%m-%d %H:%M:%S: MileStone OTD Data Update Failed ...'))
        
        print("      " + datetime.now().strftime('%Y-%m-%d %H:%M:%S: Starting COOIS Components Data Update ...'))
        moddate = time.ctime(os.path.getmtime(Dest4))
        
        try:
            coois_components=pd.read_csv(Dest4,delimiter='\t',engine='python',skiprows=3,quotechar='"', error_bad_lines=False, encoding = 'iso-8859-1')
            
            
        except Exception as e:
            print("      " , '2  read coois_components')
            raise ValueError(str(e))
        coois_components.columns = ['Unnamed1','Order','Material','Item Component List','Requirement date','Required Quantity','Quantity Withdrawn','Base Unit of Measure','Material Description','Batch','Sequence','Activity','Reservation item','Plant','Storage Location','Type','System status','Final Issue','Procurement Type','Bulk Material', 'Deleted', 'Phantom']
        
        
        
        print("      " , '1  read zppwip data')
        
        try:
            #master = pd.read_csv(r'\\yyz0sv02.goodrich.root.local\dept\In_House_Metrics\TABLEAU_REPORTS\Master Files\Master_MileStones_v8.csv', delimiter=',',engine='python', encoding= 'unicode_escape')
            zppwip=pd.read_csv(Dest2,delimiter='\t',engine='python',skiprows=6,encoding='cp1252')
            
        except Exception as e:
            print("      " , '1  read zppwip error')
            raise ValueError(str(e))
        zppwip.columns = ['Unnamed1','Plant','Order','Unnamed2','Type','Base_qty.','Serial Number','Material','Material Description','Profit Center','Stat','Release','Act. start','Bsc start','Basic fin.','EarlSchDte','Earl. fin.','Cost Ctr','Current Wo','Current Operation/Activity','Next operation/workcenter','Total Hour','Total Cost','Created on','LatestFin.','LatestStrt','Op. start',' PrTime','SetupT','Pr.Superv.','PldDelDate','Profit Cen','Days Last','Typ','ProdS','MRP ctrlr','Finish day','Priority','Open Qty','Work Center Description','Work Center Description.1','TRLT','Controller_name','Tot_Ops','O/P left','Interop','Prio Desc','MRP Area','System Status Line','Nxt Asy St','Leg Rel Dt','ValCl']
        
        coois_components=coois_components.loc[coois_components['Deleted'] != 'X']
        coois_components=coois_components[['Order','Material','Material Description','Item Component List','Requirement date','Quantity Withdrawn','Required Quantity','Phantom','Bulk Material','Final Issue']]
        coois_components.columns=['Order','Material','Material_Description','Item Component List','Requirement date','Quantity_Withdrawn','Required_Quantity','Phantom','Bulk Material','Final Issue']
        coois_components.dropna(subset=['Item Component List'],inplace=True)
        coois_components = coois_components[coois_components['Item Component List'].apply(lambda x: type(x) != str or str(x).isdigit())]
        
        
        try:
            coois_components['Quantity_Withdrawn'] = coois_components['Quantity_Withdrawn'].str.replace(',', '.')
            coois_components['Quantity_Withdrawn'] = coois_components['Quantity_Withdrawn'].str.replace(r'\.(?=.*?\.)', '')
        except:
            pass
        
        try:
            coois_components['Required_Quantity'] = coois_components['Required_Quantity'].str.replace(',', '.')
            coois_components['Required_Quantity'] = coois_components['Required_Quantity'].str.replace(r'\.(?=.*?\.)', '')              
        except:
            pass
        
        zppwip=zppwip[['Order','Material','Material Description','Serial Number','Profit Center','Current Operation/Activity']]
        zppwip.columns=['Order','Material','Material_Description','Serial_Number','Profit_Center','Current_Operation']
    
        coois_components['Time stamp']=moddate
        coois_components['Time stamp']=pd.to_datetime(coois_components['Time stamp'])
        zppwip['Time stamp']=moddate
        zppwip['Time stamp']=pd.to_datetime(zppwip['Time stamp'])
        ToDB = True
        Counter = 0
        while ToDB and Counter<10:
            try:
                
                coois_components.to_excel(os.path.join(BeforePushDB,f'coois_components_{datetime.now().strftime("%d_%m_%Y_%H_%M_%S")}.xlsx'))
                if Push_COOIS_Components_V1:
                    print("      " + datetime.now().strftime('%Y-%m-%d %H:%M:%S: COOIS Components Data DB Push Start ...'))
                    coois_components.to_sql(name='COOIS_Components_V1',schema='SIOP',index=False,con=engine,if_exists='append',chunksize=36,method='multi')
                    Message += '<p style="color:green;">' + datetime.now().strftime('%Y-%m-%d %H:%M:%S: COOIS Components Data successfully updated.</p>')
                    print("      " + datetime.now().strftime('%Y-%m-%d %H:%M:%S: COOIS Components Data Update Successful ...'))
                else:
                    print("      " + datetime.now().strftime('%Y-%m-%d %H:%M:%S: COOIS Components Data DB Push SKIP...'))
                    Message += '<p style="color:green;">' + datetime.now().strftime('%Y-%m-%d %H:%M:%S: COOIS Components Data - SKIPPED.</p>')
                
                Counter = 0
                ToDB = False
            except Exception as e:
                # Print any error messages to stdout
                print("      " + str(e))
                logf = open("log.txt", "a")
                logf.write("Failed at- %s" % datetime.now())
                traceback.print_exc(file=logf)
                logf.close()
                Counter += 1
                time.sleep(1)
                continue            
        if Counter == 10:
            Message += '<p style="color:red;">' + datetime.now().strftime('%Y-%m-%d %H:%M:%S: Failed to update COOIS Components Data.</p>')
            print("      " + datetime.now().strftime('%Y-%m-%d %H:%M:%S: COOIS Components Data Update Failed ...'))
            
        ToDB = True
        Counter = 0
        while ToDB and Counter<10:
            try:
                
                zppwip.to_excel(os.path.join(BeforePushDB,f'zppwip_{datetime.now().strftime("%d_%m_%Y_%H_%M_%S")}.xlsx'))
                if Push_COOIS_Components_ZPPWIP_V1:
                    print("      " + datetime.now().strftime('%Y-%m-%d %H:%M:%S: COOIS Components ZPPWIP Data DB Push Start...'))
                    zppwip.to_sql(name='COOIS_Components_ZPPWIP_V1',schema='SIOP',index=False,con=engine,if_exists='append',chunksize=36,method='multi')
                    Message += '<p style="color:green;">' + datetime.now().strftime('%Y-%m-%d %H:%M:%S: COOIS Components ZPPWIP Data successfully updated.</p>')
                    print("      " + datetime.now().strftime('%Y-%m-%d %H:%M:%S: COOIS Components ZPPWIP Data Update Successful ...'))
                else:
                    Message += '<p style="color:green;">' + datetime.now().strftime('%Y-%m-%d %H:%M:%S: COOIS Components ZPPWIP Data - SKIPPED</p>')
                    print("      " + datetime.now().strftime('%Y-%m-%d %H:%M:%S: COOIS Components ZPPWIP Data DB Push SKIP...'))
                    
                Counter = 0
                ToDB = False
            except Exception as e:
                # Print any error messages to stdout
                print("      " + str(e))
                logf = open("log.txt", "a")
                logf.write("Failed at- %s" % datetime.now())
                traceback.print_exc(file=logf)
                logf.close()
                Counter += 1
                time.sleep(1)
                continue
        if Counter == 10:
            Message += '<p style="color:red;">' + datetime.now().strftime('%Y-%m-%d %H:%M:%S: Failed to update COOIS Components ZPPWIP Data.</p>')
            print("      " + datetime.now().strftime('%Y-%m-%d %H:%M:%S: COOIS Components ZPPWIP Data Update Failed ...'))
            
        print("      " + datetime.now().strftime('%Y-%m-%d %H:%M:%S: Starting Past Due Ops Data Update ...'))
        moddatetime = datetime.fromtimestamp(os.path.getmtime(Dest3))
        moddate = datetime.combine(moddatetime,datetime.min.time())
        # Extract Data from COOIS ops file
        df=df_coois.copy()
        df=df[~df['System Status'].str.contains('LKD')]
        df=df[~df['System Status'].str.contains('DLT')]
        df['Projected Start Date'] = df.apply(lambda x: To_Datetime(x['LatestStrt'], x['LatestStrt.1']),axis=1)
        df['Projected Finish Date'] = df.apply(lambda x: To_Datetime(x['LatestFin.'], x['LatestFin..1']),axis=1)

        try:
            df['Seq. cat.'].fillna(0, inplace=True)
        except:
            pass
        
        df['Seq. cat.'] = df['Seq. cat.'].astype(int)
        sequenced_df = df[df['Seq. cat.'] > 0].reset_index(drop = True)
        df = df[df['Seq. cat.'] == 0].reset_index(drop = True)
        
        # Processing sequenced data
        print("      " , '2  read dfs data')
        try:
            dfs=pd.read_csv(Dest5, delimiter='\t',skiprows=3,engine='python',encoding='cp1252')
        
        except Exception as e:
            print("      " , '2  read dfs')
            raise ValueError(str(e))
        dfs = dfs[dfs['Seq. cat.'] > 0]
        print("      " + dfs.columns)
        try:
            dfs.dropna(subset = ['Descriptn.'], inplace = True)
        except Exception as e:
            print("      " + str(e))
            try:
                dfs.dropna(subset = ['Sequence description'], inplace = True)
                dfs.rename(columns={'Sequence description':'Descriptn.'}, inplace=True)
    
            except:
                pass
        dfs = dfs[['Order', 'Sequence', 'Seq. cat.', 'Descriptn.', 'Branch op.', 'Return op.']]
        dfs = dfs[dfs['Descriptn.'].str.contains('-')].reset_index(drop = True)
    
        def get_start(x):
#                descp_split = x['Descriptn.'].split(' ')
#                front = 0
#                end = 0
#                for i in range(0, len(descp_split)):
#            
#                    if(i < len(descp_split) - 1 and descp_split[i+1] == 'TO'):
#                        front = descp_split[i]
#                    elif(descp_split[i-1] == 'TO'):
#                        end = descp_split[i]
#                       
#                return str(front) + '-' + str(end)
            descp_split = x['Descriptn.'].split(' ')
            for i in range(0, len(descp_split)):
                if('-' in descp_split[i]):
                    sVal = descp_split[i-1]
                    if(i < len(descp_split)-1):
                        eVal = descp_split[i+1]
                    else:
                        eVal = ""
                    break
            val = sVal + '-' + eVal        
            val.replace("OP", "").replace("OPERATIONS","").replace("OPS","")
            return val                                       
    
        dfs['range'] = dfs.apply(get_start, axis = 1)
        dfs['start'] = dfs['range'] .apply(lambda x: x.split('-')[0])
        dfs['end'] = dfs['range'] .apply(lambda x: x.split('-')[1])
        dfs = dfs[pd.to_numeric(dfs['start'], errors='coerce').notnull()]
        dfs['end'] = dfs.apply(lambda x: x['end'] if x['end'] != '' else x['start'], axis = 1)
        
        sequence_master_DF = pd.DataFrame()
        for i in range(0, len(dfs)):
            temp_df = dfs.iloc[i,:]
            try:
                int(temp_df['start'])
                int(temp_df['end']) + 1
            except:
                continue
            for i in range(int(temp_df['start']), int(temp_df['end']) + 1):
                temp_df['Activity'] = i
                sequence_master_DF = sequence_master_DF.append(temp_df)
            
        sequence_master_DF = sequence_master_DF[['Order', 'Activity', 'Branch op.', 'Sequence']].reset_index(drop=True)
        sequence_master_DF = sequence_master_DF.fillna(0)
        sequence_master_DF = sequence_master_DF.astype(int)
        sequence_master_DF['OrdActKey'] = sequence_master_DF['Order'].astype(str) + "-" + sequence_master_DF['Activity'].astype(str)
        sequence_master_DF =sequence_master_DF[['OrdActKey', 'Branch op.', 'Sequence']].reset_index(drop=True)
        
        # Joining the sequence master DF with sequence df
        sequenced_df['OrdActKey'] = sequenced_df['Order'].astype(str) + "-" + sequenced_df['Activity'].astype(str)
        sequenced_df = sequenced_df.join(sequence_master_DF.set_index('OrdActKey'), on = 'OrdActKey', how = 'inner').reset_index(drop = True)
        sequenced_df.drop('OrdActKey', axis=1, inplace = True)
        sequenced_df.rename(columns = {'Activity':'Rework Op.', 'Branch op.': 'Activity'}, inplace = True)
        sequenced_df['Activity'] = sequenced_df['Activity'].astype(str).apply(lambda x: x.zfill(4))
        
        # Adding dummy columns in df
        df['Rework Op.'] = ""
        df['Sequence'] = 0
        
        # Concatinating the sequenced DF with actual df
        df = pd.concat([df, sequenced_df])            
        #dropcols = ['PRD Header','Op. Qty','LatestStrt','LatestStrt.1','LatestFin.','LatestFin..1',' Confirm.','PlanDate','             Processing','Fcst durtn','PReq', 'PO', 'PReq. Item', 'Purch.doc.','Act. start','Act.finish.1','Act.finish.2','Earl. fin.','EarlFnTime','Erl. start','EarlStTime']
        #df.drop(columns= dropcols, inplace=True)
        
        CNames = ['PRD Header','Op. Qty','LatestStrt','LatestStrt.1','LatestFin.','LatestFin..1','Confirm.','PlanDate','Processing','Fcst durtn','PReq', 'PO', 'PReq. Item', 'Purch.doc.','Act. start','Act.finish.1','Act.finish.2','Earl. fin.','EarlFnTime','Erl. start','EarlStTime', 'Vendor', 'Info rec.']
        
        if('Queue time.1' in df.columns):
            CNames.append('Queue time.1')                
        
        DropCNames = []
        for CName in CNames:
            CN = [col for col in df.columns if CName in col]
            DropCNames += CN
        df.drop(columns= DropCNames, inplace=True)
        
        # Extract Data from COOIS HEADER file 
        
        print("      " , '2  read dfh data')
        try:
            dfh=pd.read_csv(Dest1, delimiter='\t',skiprows=3,engine='python',encoding='cp1252')
        
        except Exception as e:
            print("      " , '2  read dfh')
            raise ValueError(str(e))
        if 'Serial no.' in dfh.columns:
            dfh['SerNo'] = dfh['Serial no.']
        dfh.to_csv('coois_headers.csv')
        dfh = dfh[dfh['Order Type'] == 'ZP03']
        dfh=dfh[~dfh['System Status'].str.contains('LKD')]
        dfh=dfh[~dfh['System Status'].str.contains('DLT')]
        dfh = dfh.rename(columns={ "System Status": "System Status_CH"})
        dfh=dfh[['Order','Material','Material description','Order Type','SerNo','Profit Ctr','System Status_CH', 'Bsc start','Basic fin.','Commit Dt',
                 'Order Text', 'Alert Text', 'Leg Rel Dt', 'Nxt Asy St']]   
        
        # Extract Data from ZPPWIP file
        
        print("      " , '2  read zppwip data')
        try:
            zppwip=pd.read_csv(Dest2, delimiter='\t',skiprows=6,engine='python',warn_bad_lines=True, error_bad_lines=False,encoding='cp1252')
        
        except Exception as e:
            print("      " , '2  read zppwip')
            raise ValueError(str(e))
        if 'Serial No.' in zppwip.columns:
            zppwip['Serial Number'] = zppwip['Serial No.']
        zppwip_master = zppwip
        zppwip.to_csv('zppwipdata.csv')
        zppwip = zppwip[(zppwip['Type'] == 'ZP03') & (zppwip['MRP ctrlr'] != 'LOS')]
        zppwip['Bsc start_ZppWip'] = pd.to_datetime(zppwip['Bsc start'])
        zppwip['Basic fin._ZppWip'] = pd.to_datetime(zppwip['Basic fin.'])
        zppwip=zppwip[['Order','Serial Number', 'Bsc start_ZppWip','Basic fin._ZppWip','Current Op']]
        dfzh=dfh.join(zppwip.set_index('Order'),on='Order',how='left')
        df=df.join(dfzh.set_index('Order'),on='Order',how='left')
        
        # Extract Data from Milestones Master file 
        
        
        try:
            df_master=pd.read_csv(r'\\yyz0sv02.goodrich.root.local\dept\In_House_Metrics\TABLEAU_REPORTS\Master Files\Master_MileStones_v8.csv',encoding='iso-8859-1')
        
        except Exception as e:
            print("      " , '3  read df_master')
            raise ValueError(str(e))
            
        df_master['Material']=df_master['Mach PN']
        df_master['Program']=df_master['Prog']   
        df_master = df_master[['Material','Cell','Program','Heat_Treat_Activity','Nital_Etch_Activity','Prime/Bush_Activity']]
        df=df.join(df_master.set_index('Material'),on='Material',how='left')
        
        # Extract Data from Work Center Master file
        
        try:
            df_wc=pd.read_csv(r'\\yyz0sv02.goodrich.root.local\dept\In_House_Metrics\TABLEAU_REPORTS\Master Files\WorkCenter_Master_v2.csv', encoding='iso-8859-1')
        
        except Exception as e:
            print("      " , '4  read df_wc')
            raise ValueError(str(e))
            
        df_wc['Work cntr.']=df_wc['Work Center']
        df_wc['Cell_WC']=df_wc['Cell']
        df_wc=df_wc[['Work cntr.','Description','Functional Group','planner','DL Cell','Cell_WC']]
        df=df.join(df_wc.set_index('Work cntr.'),on='Work cntr.',how='left')
        
        if('Std Value.2' in df.columns):
           df.rename(columns={'Std Value.2': 'Std Value.1','Std Value.1': ' Std Value'}, inplace = True)
        
        df.rename(columns={' Std Value': 'Std Value2'}, inplace = True)
        df['Actl Start']= pd.to_datetime(df['Actl Start'])
        df['Time stamp']= moddate
        df.columns = df.columns.str.lstrip()    
        df.drop(['Conf. act..1'], axis=1, inplace=True)
        pd_orders=df[df['Basic fin._ZppWip'] < moddate]
        pd_ops=df[df['Projected Start Date'] < moddate]
        pd_ops=pd_ops[pd_ops['Yield'].astype(str)=='0']
        pd_ops=pd_ops[pd_ops['Actl Start'].isnull()]
        df = pd.concat([pd_ops, pd_orders],ignore_index=True).drop_duplicates().reset_index(drop=True) 
        df['MRP Ctrl'] = df['Order'].map(zppwip_master[['Order','MRP ctrlr']].set_index('Order').to_dict()['MRP ctrlr'])
        df = df[~df['MRP Ctrl'].str.contains('LOS', na=False)]
        df.drop(['MRP Ctrl'],axis=1,inplace=True)
        df.reset_index(drop=True, inplace=True)

        ToDB = True
        Counter = 0
        while ToDB and Counter<10:
            try:
                
                df.to_excel(os.path.join(BeforePushDB,f'df_{datetime.now().strftime("%d_%m_%Y_%H_%M_%S")}.xlsx'))
                if Push_Past_Due_Ops_V1:
                    print("      " + datetime.now().strftime('%Y-%m-%d %H:%M:%S: Past Due Ops Data DB Push Start...'))
                    df.to_sql(name='Past_Due_Ops_V1',schema='SIOP',index=False,con=engine,if_exists='append',chunksize=36,method='multi')
                    Message += '<p style="color:green;">' + datetime.now().strftime('%Y-%m-%d %H:%M:%S: Past Due Ops Data successfully updated.</p>')
                    print("      " + datetime.now().strftime('%Y-%m-%d %H:%M:%S: Past Due Ops Data Update Successful ...'))
                else: 
                    Message += '<p style="color:green;">' + datetime.now().strftime('%Y-%m-%d %H:%M:%S: Past Due Ops Data - SKIPPED.</p>')
                    print("      " + datetime.now().strftime('%Y-%m-%d %H:%M:%S: Past Due Ops Data DB Push SKIP...'))
                Counter = 0
                ToDB = False
            except Exception as e:
                # Print any error messages to stdout
                print("      " + e)
                logf = open("log.txt", "a")
                logf.write("Failed at- %s" % datetime.now())
                traceback.print_exc(file=logf)
                logf.close()
                Counter += 1
                time.sleep(1)
                continue
        if Counter == 10:
            Message += '<p style="color:red;">' + datetime.now().strftime('%Y-%m-%d %H:%M:%S: Failed to update Past Due Ops Data.</p>')
            print("      " + datetime.now().strftime('%Y-%m-%d %H:%M:%S: Past Due Ops Data Update Failed ...'))

        
        print("      " + datetime.now().strftime('%Y-%m-%d %H:%M:%S: Starting WC Dispatch Priority WIP Data Update ...'))
        dfcop = df_coois.copy()
        
        dfcop.columns = ['PRD Header Material', 'Order', 'Activity', 'Work center',
                         'Operation short text', 'Operation Quantity (MEINH)',
                         'ActStartDateExecution', 'ActStartTimeExecution',
                         'ActFinishDateExecutn', 'ActFinishTimeExecutn', 'Queue_time_1',
                         'System Status', 'Actual op. finish', 'Confirmation',
                         'EarlFinishDateExecutn', 'EarlFinishTimeExecutn',
                         'EarlStartDateExecutn', 'LatstFinishDateExectn',
                         'LatstFinTimeExecution', 'Planned Date', 'EarlStartTimeExecutn',
                         'LatstStartDateExecutn', 'LatstStartTimeExecutn', 'Queue_time_2',
                         'Control key', 'Standard value 1 (VGE01)', 'Standard value 2 (VGE02)',
                         'Standard value 4 (VGE04)', 'Setup time (RSTZE)', 'Move time (TRAZE)',
                         'Confirmed activ. 1 (ILE01)', 'Work center description',
                         'Processing time (BEAZE)', 'Confirmed yield (MEINH)',
                         'Prog duration from confirmation (PDAE)', 'Confirmed activ. 2 (ILE02)',
                         'Purchase requisition', 'Purch. Order Exists', 'Purch.requ.item',
                         'Purchasing document', 'SubOp', 'Vendor', 'Info Record', 'Sequence Category']
        
        dfcop['Standard value 1 (VGE01)'] = dfcop['Standard value 1 (VGE01)'].str.replace('"', '')
        dfcop['Standard value 1 (VGE01)'] = dfcop['Standard value 1 (VGE01)'].str.replace(',', '.')
        dfcop['Standard value 1 (VGE01)'] = dfcop['Standard value 1 (VGE01)'].str.replace(r'\.(?=.*?\.)', '')
        dfcop['Standard value 2 (VGE02)'] = dfcop['Standard value 2 (VGE02)'].str.replace('"', '')
        dfcop['Standard value 2 (VGE02)'] = dfcop['Standard value 2 (VGE02)'].str.replace(',', '.')
        dfcop['Standard value 2 (VGE02)'] = dfcop['Standard value 2 (VGE02)'].str.replace(r'\.(?=.*?\.)', '')
        dfcop['Standard value 4 (VGE04)'] = dfcop['Standard value 4 (VGE04)'].str.replace('"', '')
        dfcop['Standard value 4 (VGE04)'] = dfcop['Standard value 4 (VGE04)'].str.replace(',', '.')
        dfcop['Standard value 4 (VGE04)'] = dfcop['Standard value 4 (VGE04)'].str.replace(r'\.(?=.*?\.)', '')
        dfcop['Confirmed activ. 1 (ILE01)'] = dfcop['Confirmed activ. 1 (ILE01)'].str.replace('"', '')
        dfcop['Confirmed activ. 1 (ILE01)'] = dfcop['Confirmed activ. 1 (ILE01)'].str.replace(',', '.')
        dfcop['Confirmed activ. 1 (ILE01)'] = dfcop['Confirmed activ. 1 (ILE01)'].str.replace(r'\.(?=.*?\.)', '')
        
        
        
        print("      " , '4  read dfz data')
        try:
            dfz=pd.read_csv(Dest2,delimiter='\t',skiprows=5,engine='python',warn_bad_lines=True, error_bad_lines=False,encoding='cp1252')
        
        except Exception as e:
            print("      " , '4  read dfz')
            raise ValueError(str(e))
        dfz.drop(['Unnamed: 0', 'Unnamed: 3'], axis=1, inplace=True)
        
        dfz.columns  = ['Prod Plant', 'Order', 'Order Type', 'Base quantity', 'Serial Number',
                        'Material', 'Material Description', 'Profit Center', 'Status',
                        'Actual release date', 'Actual start date', 'Basic start date',
                        'Basic finish date', 'Earliest start date', 'Earliest finish date',
                        'Cost Center', 'Current Work Center', 'Current Operation/Activity',
                        'Next operation/workcenter', 'Total Hours', 'Total Cost', 'Created on',
                        'Latest finish date', 'Latest start date', 'Operation start',
                        'Processing time', 'Setup time', 'Production Supervisor',
                        'Delivery date from planned order', 'Planning Profit center',
                        'Days Last moved', 'MRP Type', 'Prodn Supervisor', 'MRP controller',
                        'Basic finish days', 'Order priority', 'Open Quantity',
                        'Work Center Description', 'Work Center Description.1',
                        'Tot. repl. lead time', 'MRP controller name', 'Tot Ops',
                        'Operations remaining in order', 'Interoperation',
                        'Priority description', 'MRP Area', 'System Status Line',
                        'Next Assembly Start Date', 'Legacy Release Date','Valuation Class']
        
        
        
        print("      " , '4  read dfh data')
        try:
            dfh=pd.read_csv(Dest1,delimiter='\t',skiprows=3,engine='python',warn_bad_lines=True, error_bad_lines=False,encoding='cp1252')
        
        except Exception as e:
            print("      " , '4  read dfh')
            raise ValueError(str(e))
        dfh.drop('Unnamed: 0', axis=1, inplace=True)
        
        dfh.columns = ['FAI Status', 'FAI Type', 'Order', 'Material Number', 'Order Type',
                       'MRP controller', 'Prodn Supervisor', 'Plant', 'Basic start date',
                           'Basic finish date', 'Message Type', 'System Status',
                           'Material description', 'Actual start date', 'Release date (actual)',
                           'Release date plan.', 'Release date (scheduled)', 'Actual finish date',
                           'Priority', 'Serial no.', 'Change date', 'Last changed by',
                           'Changed at', 'User Status', 'MRP Area', 'Av.chk: committ.diff.',
                           'Av.chk: committ.fact. (PROZENT)', 'Batch', 'Created on', 'Created At',
                           'Entered by', 'Confirmed finish date', 'Actual Finish Time',
                           'Expected variance (GMEIN)', 'Scheduled finish date',
                           'Basic finish time', 'Scheduled Fin. Time', 'Committed date',
                           'Start date (sched)', 'ActualStartTime', 'Basic start time',
                           'Scheduled start time', 'Quantity Delivered (GMEIN)',
                           'Confirmed scrap (GMEIN)', 'Confirmed quantity (GMEIN)', 'Description',
                           'Type avail.chck', 'Storage Location', 'ExpectYieldVariance (GMEIN)',
                           'Planner group', 'Planning plant', 'Profit Center',
                           'Production Process No', 'WBS Element', 'Inspection Lot',
                           'Identification', 'Reference order', 'Reservation', 'BOM change number',
                           'Status Profile', 'BOM alternative', 'BOM usage', 'BOM material',
                           'BOM number', 'BOM status', 'BOM category',
                           'Last operation confirmation date', 'Notification',
                           'Results analysis key', 'Commit Date', 'Order Text', 'Alert Text',
                           'Legacy Release Date', 'Next Assembly Start Date']
        
        # The code should be copied from here
        dfd = dfcop.copy()
        dfd = dfcop[~dfcop['System Status'].str.contains('LKD')]
        dfd = dfd[~dfd['System Status'].str.contains('DLT')]
         
        cols1 = ['Order', 'Activity', 'Work center','Work center description','ActStartDateExecution','ActFinishDateExecutn','ActFinishTimeExecutn','LatstStartDateExecutn', 'LatstStartTimeExecutn','LatstFinishDateExectn','LatstFinTimeExecution','Confirmed activ. 1 (ILE01)','Confirmed yield (MEINH)','Standard value 1 (VGE01)','Standard value 2 (VGE02)','Standard value 4 (VGE04)', 'SubOp', 'Operation short text'] 
        
        dfd = dfd[cols1]
        
        zppwip = dfz[['Order','Material', 'Material Description','Serial Number','Profit Center','Order Type', 'Current Work Center']]
        dfd=dfd.join(zppwip.set_index('Order'),on='Order',how='left')
        
        headers = dfh[['Order','Priority','Order Text','Serial no.','Prodn Supervisor','Alert Text', 'Legacy Release Date', 'Next Assembly Start Date', 'Commit Date']]
        dfd=dfd.join(headers.set_index('Order'),on='Order',how='left')
        
        dfd['Program'] =  dfd['Profit Center'].str[2:].str[:-2]    
        dfd['Booked Hours'] = dfd['Confirmed activ. 1 (ILE01)']
        dfd['Labour Hours'] = dfd['Standard value 1 (VGE01)']
        dfd['Setup Time'] = dfd['Standard value 2 (VGE02)']
        dfd['Machine Hours'] = dfd['Standard value 4 (VGE04)']
        dfd['Aircraft No'] = dfd['Serial no.']
        dfd['Operator'] = ' '
        dfd['LatstStartDateExecutn'] = pd.to_datetime(dfd['LatstStartDateExecutn'])
        dfd = dfd[dfd['LatstStartDateExecutn']<= datetime.now()+relativedelta(days=28)]
        dfd['LatstFinishDateExectn'] = pd.to_datetime(dfd['LatstFinishDateExectn'])
        dfd.sort_values(by= ['Order','Activity'], inplace=True)
        dfd = dfd.reset_index()
        dfd['PrevOpYield'] = dfd['Confirmed yield (MEINH)'].shift(1)
        dfd['PrevOpYield'] = (dfd['PrevOpYield'].fillna(0)).astype(int)
        dfd['PrevOrder'] = dfd['Order'].shift(1)
        dfd['Confirmed yield (MEINH)'] = (dfd['Confirmed yield (MEINH)'].fillna(0)).astype(int)
        
        def operation_status(x):
            
            if(x['Confirmed yield (MEINH)'] > 0):
                val = "COMPLETED"
            elif((x['Confirmed yield (MEINH)'] == 0) and (x['PrevOpYield'] > 0) and (not(pd.isnull(x['ActStartDateExecution'])))):
                val = "IN PROGRESS"
            elif((x['Confirmed yield (MEINH)'] == 0) and (x['PrevOpYield'] > 0)):
                val = "READY"
            elif((x['Confirmed yield (MEINH)'] == 0) and (x['PrevOpYield'] == 0)):
                if(x['Order'] == x['PrevOrder']):
                    val = "Previous Op. not yielded"
                else:
                    val = "READY"
            
            return val
        
        dfd['OperationStatus'] = dfd.apply(operation_status,axis=1)
        
        
        try:
            master = pd.read_csv(r'\\yyz0sv02.goodrich.root.local\dept\In_House_Metrics\TABLEAU_REPORTS\PLANvsACTUAL\Master_Codes\Master_MileStones_v7.csv', delimiter=',',engine='python', encoding= 'unicode_escape')
        
        except Exception as e:
            print("      " , '5  read df_wc')
            raise ValueError(str(e))
            
        def nearest_milestone_date(x):
            if(x['Material'] in master['Mach PN'].values):
                master_filter = master[master['Mach PN']==x['Material']]
            else:
                return None
            
            cell_value = master_filter['Cell'].values[0]        
            HT_Val = master_filter['Heat_Treat_Activity'].values[0]
            NE_Val = master_filter['Nital_Etch_Activity'].values[0]
            PB_Val = master_filter['Prime/Bush_Activity'].values[0]
            
            if(cell_value == 'Machine'):
                HT_Bool = ((HT_Val!="-") and (not(pd.isnull(HT_Val))))
                NE_Bool = ((NE_Val!="-") and (not(pd.isnull(NE_Val))))
                
                if(HT_Bool and NE_Bool):
                    if(int(x['Activity']) <= int(HT_Val)):
                        val = x['Next Assembly Start Date']
                    elif((int(x['Activity']) > int(HT_Val)) and (int(x['Activity']) <= int(NE_Val))):
                        val = x['Legacy Release Date']
                    elif(int(x['Activity']) > int(NE_Val)):
                        val = x['Commit Date']
                    else:
                        val = None
                        
                elif(HT_Bool):
                    if(int(x['Activity']) <= int(HT_Val)):
                        val = x['Next Assembly Start Date']
                    elif(int(x['Activity']) > int(HT_Bool)):
                        val = x['Commit Date']
                    else:
                        val = None
                
                elif(NE_Bool):
                    if(int(x['Activity']) <= int(NE_Bool)):
                        val = x['Legacy Release Date']
                    elif(int(x['Activity']) > int(NE_Bool)):
                        val = x['Commit Date']
                    else:
                        val = None
    
                else:
                    val = None                              
                
            elif(cell_value == 'Surface Finishing'):           
                if(not(pd.isnull(PB_Val))):
                    if(not(pd.isnull(NE_Val)) and (NE_Val!="-") and int(NE_Val) == '654321'):
                        if(int(x['Activity']) <= int(PB_Val)):
                            val = x['Alert Text']
                        elif(int(x['Activity']) > int(PB_Val)):
                            val = x['Commit Date']
                        else:
                            val = None
                    else:
                        if(not(pd.isnull(NE_Val)) and (NE_Val!="-")):
                            if(int(x['Activity']) <= int(NE_Val)):
                                val = x['Legacy Release Date']
                            elif((int(x['Activity']) > int(NE_Val)) and (int(x['Activity']) <= int(PB_Val))):
                                val = x['Alert Text']
                            elif((int(x['Activity']) > int(NE_Val))):
                                val = x['Commit Date']                                 
                            else:
                                val = None
                        else:
                            val = None
                else:
                    val = None
    
            elif(cell_value == 'Machine + Surface Finishing'):
                HT_Bool = ((HT_Val!="-") and (not(pd.isnull(HT_Val))))
                NE_Bool = ((NE_Val!="-") and (not(pd.isnull(NE_Val))))            
                PB_Bool = ((PB_Val!="-") and (not(pd.isnull(PB_Val))))
                
                if(HT_Bool and NE_Bool and PB_Bool):
                    if(int(x['Activity']) <= int(HT_Val)):
                        val = x['Next Assembly Start Date']
                    elif((int(x['Activity']) > int(HT_Val)) and (int(x['Activity']) <= int(NE_Val))):
                        val = x['Legacy Release Date']
                    elif((int(x['Activity']) > int(NE_Val)) and (int(x['Activity']) <= int(PB_Val))):
                        val = x['Alert Text']
                    elif(int(x['Activity']) > int(PB_Val)):
                        val = x['Commit Date']
                    else:
                        val = None
                        
                elif(HT_Bool and NE_Bool):
                    if(int(x['Activity']) <= int(HT_Val)):
                        val = x['Next Assembly Start Date']
                    elif((int(x['Activity']) > int(HT_Val)) and (int(x['Activity']) <= int(NE_Val))):
                        val = x['Legacy Release Date']  
                    elif(int(x['Activity']) > int(NE_Val)):
                        val = x['Commit Date']
                    else:
                        val = None
    
                elif(HT_Bool and PB_Bool):
                    if(int(x['Activity']) <= int(HT_Val)):
                        val = x['Next Assembly Start Date']
                    elif((int(x['Activity']) > int(HT_Val)) and (int(x['Activity']) <= int(PB_Val))):
                        val = x['Alert Text']  
                    elif(int(x['Activity']) > int(PB_Val)):
                        val = x['Commit Date']
                    else:
                        val = None                    
    
                elif(NE_Bool and PB_Bool):
                    if(int(x['Activity']) <= int(NE_Val)):
                        val = x['Legacy Release Date']  
                    elif((int(x['Activity']) > int(NE_Val)) and (int(x['Activity']) <= int(PB_Val))):
                        val = x['Alert Text']  
                    elif(int(x['Activity']) > int(PB_Val)):
                        val = x['Commit Date']
                    else:
                        val = None 
                        
                elif(HT_Bool):
                    if(int(x['Activity']) <= int(HT_Val)):
                        val = x['Next Assembly Start Date'] 
                    elif(int(x['Activity']) > int(HT_Val)):
                        val = x['Commit Date']
                    else:
                        val = None
    
                elif(NE_Bool):
                    if(int(x['Activity']) <= int(NE_Val)):
                        val = x['Legacy Release Date']   
                    elif(int(x['Activity']) > int(NE_Bool)):
                        val = x['Commit Date']
                    else:
                        val = None             
    
                elif(PB_Bool):
                    if(int(x['Activity']) <= int(PB_Val)):
                        val = x['Alert Text']   
                    elif(int(x['Activity']) > int(PB_Val)):
                        val = x['Commit Date']
                    else:
                        val = None 
                else:
                    val = None
                        
            else:
                val=None                
                
            return val
        
        dfd['Nearest Milestone Date'] = dfd.apply(nearest_milestone_date, axis=1)
        dfd['Nearest Milestone Date'] = pd.to_datetime(dfd['Nearest Milestone Date'])
        dfd['Confirmed yield (MEINH)'] = dfd['Confirmed yield (MEINH)'].astype(str)
        finalcols = ['Order', 'Activity', 'Work center','Work center description','LatstStartDateExecutn', 'LatstStartTimeExecutn','LatstFinishDateExectn','LatstFinTimeExecution','Confirmed yield (MEINH)','Material', 'Material Description','Serial Number','Program','Aircraft No','Operator','Booked Hours','Labour Hours','Setup Time','Machine Hours','Order Type','Prodn Supervisor','OperationStatus','Nearest Milestone Date', 'Operation short text', 'Current Work Center']
        
        
        dfd= dfd[finalcols]
        current_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        dfd['Timestamp'] = current_time
        
        # Changes - # Include only Yield = 0 and Machine Hours > 0.1
        pd.to_numeric(dfd['Machine Hours'], errors='coerce').astype(float) 
        dfd[['Booked Hours','Labour Hours','Machine Hours']] = dfd[['Booked Hours','Labour Hours','Machine Hours']].replace(np.nan, 0.0) 
        dfd['Setup Time'] = dfd['Setup Time'].replace(['nan'],'0.0')
        dfd = dfd[pd.to_numeric(dfd['Machine Hours'], errors='coerce').astype(float) > 0.1]
        dfd = dfd[ dfd['Confirmed yield (MEINH)'].astype(int) == 0]
        dfd[['Booked Hours','Labour Hours','Machine Hours']].info()
        dfd = dfd.reset_index(drop=True)
        
        # Changes to include Milestone Data
        
        try:
            dmm = pd.read_csv(r'\\yyz0sv02.goodrich.root.local\dept\In_House_Metrics\TABLEAU_REPORTS\PLANvsACTUAL\Master_Codes\Master_MileStones_v7.csv', delimiter=',',engine='python', encoding= 'unicode_escape')
        
        except Exception as e:
            print("      " , '6  read dmm')
            raise ValueError(str(e))
            
        dmm = dmm[['Mach PN', 'Material Description', 'Mat Group', 'Prog', 'Cell']]
        dmm.rename(columns = {'Mach PN':'Material','Material Description' : 'MS_Material_Description','Mat Group':'MS_Mat_Group','Prog' : 'MS_Program','Cell':'MS_Cell' }, inplace = True)
        
        dfd=dfd.join(dmm.set_index('Material'),on='Material',how='left')
        dfd = dfd.drop_duplicates()
        
        # Changes to include Work Center Data
        
        print("      " , '6  read dwc data')
        try:
            dwc = pd.read_csv(r'\\yyz0sv02.goodrich.root.local\dept\In_House_Metrics\TABLEAU_REPORTS\PLANvsACTUAL\Master_Codes\WorkCenter_Master_v2.csv', delimiter = ',', engine = 'python',encoding='cp1252')
        
        except Exception as e:
            print("      " , '6  read dwc')
            raise ValueError(str(e))
        dwc.rename(columns = {'Work Center':'Work center','planner' : 'Planner','Cell':'WC_Cell' }, inplace = True)
        dwc = dwc[['Work center','Planner','Functional Group','WC_Cell']]
        
        dfd=dfd.join(dwc.set_index('Work center'),on='Work center',how='left')
        dfd = dfd.drop_duplicates()
        
        dfd['Material Description'] = dfd.apply(lambda x : x['Material Description'] if pd.isnull(x['MS_Mat_Group']) else x['MS_Mat_Group'] , axis = 1)
        dfd['Program'] = dfd.apply(lambda x : x['Program'] if pd.isnull(x['MS_Program']) else x['MS_Program'] , axis = 1)
          
        
        print("      " , '6  read df_cell_map data')
        try:
            df_cell_map=pd.read_csv(r'\\yyz0sv02.goodrich.root.local\dept\In_House_Metrics\TABLEAU_REPORTS\PLANvsACTUAL\Master_Codes\CellMap.csv',encoding='cp1252')
        
        except Exception as e:
            print("      " , '6  read df_cell_map')
            raise ValueError(str(e))
        dfd['New_cell']=dfd['Work center'].map(df_cell_map[['New_Work_center','New_Cell']].set_index('New_Work_center').to_dict()['New_Cell'] )
        
        DType = {'Standard value 1 (VGE01)': sqlalchemy.types.Float(precision=3, asdecimal=True), 
                 'Standard value 2 (VGE02)':  sqlalchemy.types.Float(precision=3, asdecimal=True),
                 'Standard value 4 (VGE04)': sqlalchemy.types.Float(precision=3, asdecimal=True),
                 'Confirmed activ. 1 (ILE01)': sqlalchemy.types.Float(precision=3, asdecimal=True)}
        
        dfd['Work center'].fillna("-", inplace=True)
        dfd['Work center'] = dfd['Work center'].astype(str)
        
        final_DF = pd.DataFrame()
        
        # Obtaining WC Data
        query = "SELECT * FROM [OPS_DA].[SIOP].[Deburr_WC_Data];"
        cur4 = cnxn.cursor()
        MaxTryCount = 20
        TryCount = 0
        TryExecute = True
        while TryExecute and TryCount < MaxTryCount:    
            try:
                cur4.execute(query)
                TryExecute = False
            except:
                logf = open("log.txt", "a")
                logf.write("Failed at- %s" % datetime.now())
                traceback.print_exc(file=logf)
                logf.close()
                TryCount += 1
                cnxn1 = db_reconnect()
                cur4 = cnxn1.cursor()
                time.sleep(3)
            else:
                data = cur4.fetchall()
                data_tuple_list = [tuple(x) for x in data]
                
                WC_data = pd.DataFrame(data_tuple_list, columns=['WORK CENTER GROUP', 'WORK CENTERS', 'PRIORITY_DATE', 'RTD', 'ID', 'Confirmation_WC', 'Watch_WC', 'Op_Text_Marker'])
       
#            time.sleep(5)
#            for i in range(0,10):
#                try:
#                    WC_data = pd.read_sql('SELECT * FROM [OPS_DA].[SIOP].[Deburr_WC_Data]', cnxn)
#                    break
#                except:
#                    time.sleep(5)
#                    continue
        
        for i in range(0,len(WC_data)):
            if(WC_data.loc[i, 'PRIORITY_DATE'] == 'Nearest Milestone Date'):
                sort_list = ['Nearest Milestone Date','LatstStartDateExecutn']    
            else:
                sort_list = ['LatstStartDateExecutn']   
            WC_List = WC_data.loc[i, 'WORK CENTERS'].split(',')
            Op_Text_Marker = WC_data.loc[i, 'Op_Text_Marker']                
            temp_df = pd.DataFrame()
            for j in WC_List:
                j = j.strip()
                temp_df = temp_df.append(dfd[(dfd['Work center'] == j)])
                if(Op_Text_Marker != None):
                    temp_df['mask'] = temp_df['Operation short text'].apply(lambda x: 1 if ((str.lower(x).startswith(str.lower(Op_Text_Marker)))  or (str.lower(x).startswith(str.lower('-'.join(Op_Text_Marker.split(' ')))))) else 0)
                    temp_df = temp_df[temp_df['mask'] == 1]                    
                if(WC_data.loc[i, 'WORK CENTER GROUP'] == 'Nital Etch'):
                    temp_df = temp_df[~temp_df['Program'].str.contains('ROS', na=False)]
                    temp_df = temp_df[temp_df['Program'] != '0F22']                        
            
            temp_df.sort_values(sort_list,inplace=True)    
            temp_df['Priority'] = range(1,len(temp_df)+1)
            final_DF = final_DF.append(temp_df)
        
        
        final_DF.drop(['Operation short text', 'mask'], axis = 1, inplace = True)
        final_DF['STATUS'] = final_DF.apply(lambda x: "READY" if (x['OperationStatus'] != "Previous Op. not yielded") else "NOT AVAILABLE", axis=1)
        final_DF['ID'] = range(1,len(final_DF)+1)
        final_DF = final_DF.reset_index(drop=True)
        final_DF['AssignedStation'] = ""
        final_DF['RTD'] = np.nan
        final_DF['RTD'] = final_DF['RTD'].astype(str)
        final_DF['WATCH TEXT'] = " "
        
        #station_list = ["STATION 1", "STATION 2", "STATION 3", "STATION 4", "STATION 5", "STATION 6", "STATION 7", "STATION 8", "STATION 9", "STATION 10"]
        #dfd["Work Station"] = station_list*(int(len(dfd)/len(station_list))) + station_list[0:(len(dfd)%(len(station_list)))]
        
        finalcols = ['Order', 'Activity', 'Work center', 'Work center description', 'LatstStartDateExecutn', 'LatstStartTimeExecutn',
                     'LatstFinishDateExectn', 'LatstFinTimeExecution', 'Confirmed yield (MEINH)', 'Material', 'Material Description',
                     'Serial Number', 'Program', 'Aircraft No', 'Operator', 'Booked Hours', 'Labour Hours', 'Setup Time', 'Machine Hours', 'Order Type',
                     'Prodn Supervisor', 'OperationStatus', 'Nearest Milestone Date', 'Timestamp', 'MS_Material_Description',
                     'MS_Mat_Group', 'MS_Program', 'MS_Cell', 'Planner', 'Functional Group', 'WC_Cell', 'New_cell', 'Priority', 'STATUS', 'ID', 'AssignedStation', 'RTD', 'WATCH TEXT', 'Current Work Center']
        
        final_DF = final_DF[finalcols]
        
        ToDB = True
        Counter = 0
        while ToDB and Counter<10:
            try:
                if Push_WORK_CENTER_DISPATCH_PRIORITY_WIP:
                    print("      " + datetime.now().strftime('%Y-%m-%d %H:%M:%S: WC Dispatch Priority WIP Data - DB Push Start...'))
                    final_DF.to_sql(name='WORK_CENTER_DISPATCH_PRIORITY_WIP',schema='SIOP',index=True,dtype=DType,con=engine,if_exists='replace',chunksize=40,method='multi')
                    Message += '<p style="color:green;">' + datetime.now().strftime('%Y-%m-%d %H:%M:%S: WC Dispatch Priority WIP Data successfully updated.</p>')
                    print("      " + datetime.now().strftime('%Y-%m-%d %H:%M:%S: WC Dispatch Priority WIP Data Update Successful ...'))
                else:
                    Message += '<p style="color:green;">' + datetime.now().strftime('%Y-%m-%d %H:%M:%S: WC Dispatch Priority WIP Data - SKIPPED.</p>')
                    print("      " + datetime.now().strftime('%Y-%m-%d %H:%M:%S: WC Dispatch Priority WIP Data - DB Push SKIP...'))
                Counter = 0
                ToDB = False
            except Exception as e:
                print("      " + e)
                logf = open("log.txt", "a")
                logf.write("Failed at- %s" % datetime.now())
                traceback.print_exc(file=logf)
                logf.close()
                Counter += 1
                time.sleep(1)
                continue
        if Counter == 10:
            Message += '<p style="color:red;">' + datetime.now().strftime('%Y-%m-%d %H:%M:%S: Failed to update WC Dispatch Priority WIP Data.</p>')
            print("      " + datetime.now().strftime('%Y-%m-%d %H:%M:%S: WC Dispatch Priority WIP Data Update Failed ...'))
    except:
        logf = open("log.txt", "a")
        logf.write("Failed at- %s" % datetime.now())
        traceback.print_exc(file=logf)
        logf.close()
        Error = traceback.format_exc()
        Message += '<p style="color:red;">' + datetime.now().strftime('%Y-%m-%d %H:%M:%S: Failed to update Data.</p>') + Error
    return Message  
    
def GetRunTime():
    StartofDay = int(datetime.timestamp(datetime.combine(datetime.now(),datetime.min.time())))
    RunTime = StartofDay + (((12+2+(1/60)))*3600) #Run @ 3PM + 54000
    return RunTime;

def DB_Update():
    Message = push_to_db()
    print("      " + datetime.now().strftime('%Y-%m-%d %H:%M:%S: End of Run ...\n\n'))
    PostMessage(Message)
    
def ScheduleUpdate():
    RunTime = GetRunTime()
    scheduler = sched.scheduler(time.time, time.sleep)
    scheduler.enterabs(RunTime, 1, DB_Update)
    scheduler.run()
    
def PostMessage(Message):
    
    ReceiverMail = ['alibaig.firasat@collins.com', 'raghuram.alla@collins.com', 'Prarthana.BaswarajSangshettyPatil@collins.com']
    
    Subject = '[Daily Data Update] [LG Oakville] ' + datetime.now().strftime('[%Y-%m-%d %H:%M:%S]')
    
    # Create the HTML version of your message
    Head = '''<html><body><p style="font-family:'Cambria';font-size:15px">
            This is an automated message.<br>
            Please do not reply to this message.<br><br>'''
    Body = Message
           
    Tail = '<br><br>################# End of Message #################</p></body></html>'
    
    MsgContent = Head + Body + Tail
    if True:
        SendMail(ReceiverMail, Subject, MsgContent)
    
def SendMail(ReceiverMail, Subject, MsgContent):
    # SMTP server and Port information
    SMTPServers = ['QUSNWADY.utcapp.com', 'QUSNWAE9.utcapp.com', 'QUSNWADV.utcapp.com', 'QUSNWADW.utcapp.com', 'QUSNWADX.utcapp.com', 'QUSMNA5K.utcapp.com', 'QUSMNA5L.utcapp.com', 'QUSMNA5M.utcapp.com', 'QUSMNA60.utcapp.com', 'uusnwa7g.corp.utc.com', 'mailhub.utc.com']
    Port = 25
    
    # LG Ops Oakville MailBox 
    SenderMail = 'LGOps.Oakville@collins.com'
    
    Message = MIMEText(MsgContent, 'html')
    Message['Subject'] = Subject
    Message['From'] = SenderMail
    Message['To'] = ','.join(ReceiverMail)
    
    # Create a secure SSL context
    Context = ssl.create_default_context()
    
    # Try to connect to server and send email
    for SMTPServer in SMTPServers:
        try: 
            Server = smtplib.SMTP(SMTPServer,Port) # Establish the connection    
            Server.starttls(context=Context) # Secure the connection
            # Send Email Message
            Server.sendmail(SenderMail, ReceiverMail, Message.as_string())
            Server.quit()
            break
        except Exception as e:
            # Print any error messages to stdout
            print("      " + e)
            logf = open("log.txt", "a")
            logf.write("Failed at- %s" % datetime.now())
            traceback.print_exc(file=logf)
            logf.close()
            continue

def GetIfWeShouldProcessDataToday():
    Milestone_Consolidated_Time = pd.read_sql('SELECT  max([Time stamp]) as MaxTime FROM  [OPS_DA].[SIOP].[Milestone_Consolidated]', cnxn)
    Milestone_Consolidated_MaxTime = Milestone_Consolidated_Time['MaxTime'][0]

    COOIS_Components_V1_Time = pd.read_sql('SELECT  max([Time stamp]) as MaxTime FROM  [OPS_DA].[SIOP].[COOIS_Components_V1]', cnxn)
    COOIS_Components_V1_MaxTime = COOIS_Components_V1_Time['MaxTime'][0]

    COOIS_Components_ZPPWIP_V1_Time = pd.read_sql('SELECT  max([Time stamp]) as MaxTime FROM  [OPS_DA].[SIOP].[COOIS_Components_ZPPWIP_V1]', cnxn)
    COOIS_Components_ZPPWIP_V1_MaxTime = COOIS_Components_ZPPWIP_V1_Time['MaxTime'][0]

    Past_Due_Ops_V1_Time = pd.read_sql('SELECT  max([Time stamp]) as MaxTime FROM  [OPS_DA].[SIOP].[Past_Due_Ops_V1]', cnxn)
    Past_Due_Ops_V1_MaxTime = Past_Due_Ops_V1_Time['MaxTime'][0]

    WORK_CENTER_DISPATCH_PRIORITY_WIP_Time = pd.read_sql('SELECT  max([Timestamp]) as MaxTime FROM  [OPS_DA].[SIOP].[WORK_CENTER_DISPATCH_PRIORITY_WIP]', cnxn)
    WORK_CENTER_DISPATCH_PRIORITY_WIP_MaxTime = WORK_CENTER_DISPATCH_PRIORITY_WIP_Time['MaxTime'][0]
    WORK_CENTER_DISPATCH_PRIORITY_WIP_MaxTime = pd.to_datetime(WORK_CENTER_DISPATCH_PRIORITY_WIP_MaxTime, errors='coerce', format='%Y-%m-%d %H:%M:%S')
    
    ToDayDate = datetime.now().date()
    Push_Milestone_Consolidated = Milestone_Consolidated_MaxTime < ToDayDate
    Push_COOIS_Components_V1 = COOIS_Components_V1_MaxTime < ToDayDate
    Push_COOIS_Components_ZPPWIP_V1 = COOIS_Components_ZPPWIP_V1_MaxTime < ToDayDate
    Push_Past_Due_Ops_V1 = Past_Due_Ops_V1_MaxTime< ToDayDate
    Push_WORK_CENTER_DISPATCH_PRIORITY_WIP = WORK_CENTER_DISPATCH_PRIORITY_WIP_MaxTime < ToDayDate if WORK_CENTER_DISPATCH_PRIORITY_WIP_MaxTime else True
    
    Txt = Milestone_Consolidated_MaxTime.strftime('%Y-%m-%d %H:%M:%S ') + ': Milestone_Consolidated \n' 
    Txt += COOIS_Components_V1_MaxTime.strftime('%Y-%m-%d %H:%M:%S ') + ': COOIS_Components_V1 \n'
    Txt += COOIS_Components_ZPPWIP_V1_MaxTime.strftime('%Y-%m-%d %H:%M:%S ') + ': COOIS_Components_ZPPWIP_V1 \n'
    Txt += Past_Due_Ops_V1_MaxTime.strftime('%Y-%m-%d %H:%M:%S ') + ': Past_Due_Ops_V1 \n' 
    Txt += WORK_CENTER_DISPATCH_PRIORITY_WIP_MaxTime.strftime('%Y-%m-%d %H:%M:%S ') if WORK_CENTER_DISPATCH_PRIORITY_WIP_MaxTime else 'No past data' + ': WORK_CENTER_DISPATCH_PRIORITY_WIP \n' 
    
    print(Txt)
    return Push_Milestone_Consolidated, Push_COOIS_Components_V1, Push_COOIS_Components_ZPPWIP_V1, Push_Past_Due_Ops_V1, Push_WORK_CENTER_DISPATCH_PRIORITY_WIP, Txt

RunNow = True
DeleteTodayData = True
while True:
    DTNow = int(datetime.now().timestamp())
    SchTime = int(datetime.timestamp(datetime.combine(datetime.now(),datetime.min.time())))  + (((12+2+(1/60)))*3600) #Run @ 3PM + 54000
    #SchTime = int(datetime.now().timestamp()) - 100
    TryPushToDB = True
    if (DTNow > SchTime and DTNow-SchTime < 100) or RunNow :
        print("starting script execution at: "+str(datetime.now()))
        ReTryCount = 0
        while TryPushToDB:
            ReTryCount += 1
            try:
                print("      " + datetime.now().strftime('%Y-%m-%d %H:%M:%S: Initiate SQL ...'))
                                
                params = urllib.parse.quote_plus("DRIVER={SQL Server};SERVER=Gcaaod04.utcapp.com;DATABASE=OPS_DA;UID=S-OPS_DA_PROD;PWD=6ux@J2ms_Adm6$$")
                #params = urllib.parse.quote_plus("DRIVER={SQL Server};SERVER=ginbad01.utcain.com;DATABASE=OPS_DA;UID=S-OPS_DA_Prod;PWD=D1ehasbeencasT!1")
                cnxn = pyodbc.connect(driver='{SQL Server}',server='Gcaaod04.utcapp.com',database='OPS_DA',uid='S-OPS_DA_Prod',pwd='6ux@J2ms_Adm6$$')
                engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params) 
                engine.connect()
                
                BeforePushDB = r'\\yyz0sv02.goodrich.root.local\dept\In_House_Metrics\TABLEAU_REPORTS\Milestone_OTD\BeforePushDB'

                
                print("      " + datetime.now().strftime('%Y-%m-%d %H:%M:%S: Initialize and Check Files to Process ...'))
                Push_Milestone_Consolidated, Push_COOIS_Components_V1, Push_COOIS_Components_ZPPWIP_V1, Push_Past_Due_Ops_V1, Push_WORK_CENTER_DISPATCH_PRIORITY_WIP, Txt = GetIfWeShouldProcessDataToday()
                #time.sleep(60)
                if Push_Milestone_Consolidated or Push_COOIS_Components_V1 or Push_COOIS_Components_ZPPWIP_V1 or Push_Past_Due_Ops_V1 or Push_WORK_CENTER_DISPATCH_PRIORITY_WIP:
                    Message = push_to_db()
                else:
                    Message = "All Reports are up latest with Today TimeStamp - skip script run  \n\n" + Txt
                    TryPushToDB = False
                    RunNow = False
                print('=============================================')
                print(Message)
                print('=============================================')
                if "color:red;" not in Message:
                    TryPushToDB = False
                else:
                    print("      " + datetime.now().strftime('%Y-%m-%d %H:%M:%S: Wait for 60 Seconds and retry ...\n\n'))
                    time.sleep(60)

            except Exception as e:
                print('Error in push_to_db',str(datetime.now()),str(e))
                Message = str(e)
                time.sleep(120)
            if ReTryCount >= 10:
                TryPushToDB = False
            
        PostMessage(Message)
        time.sleep(200)
        RunNow = False
        print("completed script execution at: "+str(datetime.now()))
    else:
        time.sleep(1)
    
    #FailNow = Yesss
