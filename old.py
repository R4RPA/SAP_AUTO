import win32com.client
import subprocess
from datetime import datetime, timedelta
import time


import pandas as pd
import pyodbc
from sqlalchemy import create_engine
from six.moves import urllib
import smtplib, ssl, traceback
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os, os.path
# import psutil
import string
import numpy as np

import generic_functions
import data_functions_iw39
import data_functions_shipset
import sap_functions_iw39
import sap_functions_shipset
import sap_engine
from logger_master import setup_logger

import warnings
warnings.filterwarnings('ignore')

logger = setup_logger('sap_functions_iw39')


def saplogin(flag, UserID, Password):

    session = sap_engine.saplogin(UserID, Password)

    if flag == 'IW39':
        if Download_IW39_Operations:
            sap_functions_iw39.Download_IW39_Operations(session)
        else:
            logger.info('IW39 Skip')

        if Download_ZMRO_OPS:
            sap_functions_iw39.Download_ZMRO_OPS(session)
        else:
            logger.info('ZMRO_OPS Skip')

        if Download_ZMRO_SALES:
            sap_functions_iw39.Download_ZMRO_SALES(session)
        else:
            logger.info('ZMRO_SALES Skip')

        if Download_IW47:
            sap_functions_iw39.Download_IW47(session)
        else:
            logger.info('IW47 Skip')

        if Download_REWORK_VENDOR:
            sap_functions_iw39.Download_REWORK_VENDOR(session)
        else:
            logger.info('REWORK_VENDOR Skip')

    elif flag == 'SHIPSET':
        if Download_ZMRO_SALES_ShipSet:
            sap_functions_shipset.Download_ZMRO_SALES_ShipSet(session)
        else:
            logger.info('Shipset ZMROSALES Skip')

        if Download_CJI3:
            sap_functions_shipset.Download_CJI3(session)
        else:
            logger.info('Shipset CJI3 Skip')

    logger.info('SAP execution completed')

    generic_functions.cleanup_env()
    generic_functions.kill_excel()

    logger.info("Closed SAP Final")


def Pre_Prcoess_IW39_Orders():
    Process_Orders = Download_IW39_Orders_REWORK_SHIPSET
    Process_Operations = Download_IW39_Operations_REWORK_SHIPSET
    processor = data_functions_iw39.PreProcessIW39Orders()
    processor.main(Process_Orders, Process_Operations)


def IW39_orders_ops_processing():
    processor = data_functions_iw39.IW39DataProcessor()
    processor.main()


def FPY_Data_Processing():
    processor = data_functions_iw39.FPYDataProcessor()
    processor.main()


def rework_hours_processing():
    process_rework_hours_data = Download_LG_MIAMI_REWORK_HOURS
    process_rework_vendor_hours_data = Download_LG_MIAMI_REWORK_VENDOR_HOURS
    processor = data_functions_iw39.ReworkDataProcessor()
    processor.main(process_rework_hours_data, process_rework_vendor_hours_data)


def processShipSet():
    processor = data_functions_shipset.PreProcessIW39Orders()
    processor.main()


def SendMailToAdmin(flag):
    print("      ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "started SendMailToAdmin")
    # SMTP server and Port information
    ReceiverMail = ['Prarthana.BaswarajSangshettyPatil@collins.com', 'raghuram.alla@collins.com',
                    'Trinadh.Pentela@collins.com', 'jaganmohanreddy.kalukurthi@collins.com']
    SMTPServers = ['QUSNWADY.utcapp.com', 'QUSNWAE9.utcapp.com', 'QUSNWADV.utcapp.com', 'QUSNWADW.utcapp.com',
                   'QUSNWADX.utcapp.com', 'QUSMNA5K.utcapp.com', 'QUSMNA5L.utcapp.com', 'QUSMNA5M.utcapp.com',
                   'QUSMNA60.utcapp.com', 'uusnwa7g.corp.utc.com', 'mailhub.utc.com']
    Port = 25

    # LG Ops Oakville MailBox
    SenderMail = 'PlanningData.LGMiamiOps@collins.com'
    if flag == "IW39":
        MsgContent = 'Hi,\n\n This is an automated message. Please do not reply to this message.\n\nData related to planning files, IW47, IW39, New Orders & Operations has been updated and processed. \n\nRegards,\nPlanning Data Tool'
        Subject = 'Planning Files Updated'
    else:
        MsgContent = 'Hi,\n\n This is an automated message. Please do not reply to this message.\n\nData related to Shipset Operations has been updated and processed. \n\nRegards,\nShipset Data Tool'
        Subject = 'Shipset data updated'

    Message = MIMEMultipart()
    Message['Subject'] = Subject
    Message['From'] = SenderMail
    Message['To'] = ','.join(ReceiverMail)
    Message.attach(MIMEText(MsgContent))

    # Create a secure SSL context
    Context = ssl.create_default_context()

    # Try to connect to server and send email
    for SMTPServer in SMTPServers:
        try:
            Server = smtplib.SMTP(SMTPServer, Port)  # Establish the connection
            Server.starttls(context=Context)  # Secure the connection
            # Send Email Message
            Server.sendmail(SenderMail, ReceiverMail, Message.as_string())
            Server.quit()
            break
        except Exception as e:
            # Print any error messages to stdout
            print("      ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'), e)
            logf = open("log.txt", "a")
            logf.write("Failed at- %s" % datetime.now())
            traceback.print_exc(file=logf)
            logf.close()
            continue
    print("      ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "completed SendMailToAdmin")


def SendSapClosedMail(flag, UserID):
    # SMTP server and Port information
    print("      ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "started SendSapClosedMail")
    ReceiverMail = ['raghuram.alla@collins.com', 'jaganmohanreddy.kalukurthi@collins.com',
                    'Prarthana.BaswarajSangshettyPatil@collins.com']
    SMTPServers = ['QUSNWADY.utcapp.com', 'QUSNWAE9.utcapp.com', 'QUSNWADV.utcapp.com', 'QUSNWADW.utcapp.com',
                   'QUSNWADX.utcapp.com', 'QUSMNA5K.utcapp.com', 'QUSMNA5L.utcapp.com', 'QUSMNA5M.utcapp.com',
                   'QUSMNA60.utcapp.com', 'uusnwa7g.corp.utc.com', 'mailhub.utc.com']
    Port = 25

    # LG Ops Oakville MailBox
    SenderMail = 'PlanningData.LGMiamiOps@collins.com'
    MsgContent = "Hi,\n\n This is an automated message. Please do not reply to this message.\n\n SAP Closed for {0}/{1} . \n\nRegards,\nPlanning Data Tool".format(
        flag, UserID)
    Subject = 'SAP closed - {0}/{1}'.format(flag, UserID)

    Message = MIMEMultipart()
    Message['Subject'] = Subject
    Message['From'] = SenderMail
    Message['To'] = ','.join(ReceiverMail)
    Message.attach(MIMEText(MsgContent))

    # Create a secure SSL context
    Context = ssl.create_default_context()

    # Try to connect to server and send email
    for SMTPServer in SMTPServers:
        try:
            Server = smtplib.SMTP(SMTPServer, Port)  # Establish the connection
            Server.starttls(context=Context)  # Secure the connection
            # Send Email Message
            Server.sendmail(SenderMail, ReceiverMail, Message.as_string())
            Server.quit()
            break
        except Exception as e:
            # Print any error messages to stdout
            print("      ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'), e)
            logf = open("log.txt", "a")
            logf.write("Failed at- %s" % datetime.now())
            traceback.print_exc(file=logf)
            logf.close()
            continue
    print("      ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "completed SendSapClosedMail")




def CheckFilesDownloaded_IW39():
    IW39_Operations = download_path + '\IW39_Operations.xlsx'
    ZMRO_OPS = download_path + '\ZMRO_OPS.xlsx'
    ZMRO_SALES = download_path + '\ZMRO_SALES.xlsx'
    IW47 = download_path + '\IW47.xlsx'
    REWORK_VENDOR = download_path + '\REWORK_VENDOR.xlsx'
    IW39_Orders_REWORK_SHIPSET = download_path + '\IW39_Orders_REWORK_SHIPSET.xlsx'
    IW39_Operations_REWORK_SHIPSET = download_path + '\IW39_Operations_REWORK_SHIPSET.xlsx'
    LG_MIAMI_IW39_ORD = download_path + '\LG_MIAMI_IW39_ORD.xlsx'
    LG_MIAMI_IW39_ORD_UNQ = download_path + '\LG_MIAMI_IW39_ORD_UNQ.xlsx'
    LG_MIAMI_REWORK_HOURS = download_path + '\LG_MIAMI_REWORK_HOURS.xlsx'
    LG_MIAMI_REWORK_VENDOR_HOURS = download_path + '\LG_MIAMI_REWORK_VENDOR_HOURS.xlsx'
    LG_MIAMI_FPY = download_path + '\LG_MIAMI_FPY.xlsx'

    ToDay = datetime.now().date()
    IW39_Operations_Date = datetime.fromtimestamp(os.path.getmtime(IW39_Operations)).date()
    ZMRO_OPS_Date = datetime.fromtimestamp(os.path.getmtime(ZMRO_OPS)).date()
    ZMRO_SALES_Date = datetime.fromtimestamp(os.path.getmtime(ZMRO_SALES)).date()
    IW47_Date = datetime.fromtimestamp(os.path.getmtime(IW47)).date()
    REWORK_VENDOR_Date = datetime.fromtimestamp(os.path.getmtime(REWORK_VENDOR)).date()
    IW39_Orders_REWORK_SHIPSET_Date = datetime.fromtimestamp(os.path.getmtime(IW39_Orders_REWORK_SHIPSET)).date()
    IW39_Operations_REWORK_SHIPSET_Date = datetime.fromtimestamp(
        os.path.getmtime(IW39_Operations_REWORK_SHIPSET)).date()
    LG_MIAMI_IW39_ORD_Date = datetime.fromtimestamp(os.path.getmtime(LG_MIAMI_IW39_ORD)).date()
    LG_MIAMI_IW39_ORD_UNQ_Date = datetime.fromtimestamp(os.path.getmtime(LG_MIAMI_IW39_ORD_UNQ)).date()
    LG_MIAMI_REWORK_HOURS_Date = datetime.fromtimestamp(os.path.getmtime(LG_MIAMI_REWORK_HOURS)).date()
    LG_MIAMI_REWORK_VENDOR_HOURS_Date = datetime.fromtimestamp(os.path.getmtime(LG_MIAMI_REWORK_VENDOR_HOURS)).date()
    LG_MIAMI_FPY_Date = datetime.fromtimestamp(os.path.getmtime(LG_MIAMI_FPY)).date()

    Download_IW39_Operations = IW39_Operations_Date < ToDay
    Download_ZMRO_OPS = ZMRO_OPS_Date < ToDay
    Download_ZMRO_SALES = ZMRO_SALES_Date < ToDay
    Download_IW47 = IW47_Date < ToDay
    Download_REWORK_VENDOR = REWORK_VENDOR_Date < ToDay
    Download_IW39_Orders_REWORK_SHIPSET = IW39_Orders_REWORK_SHIPSET_Date < ToDay
    Download_IW39_Operations_REWORK_SHIPSET = IW39_Operations_REWORK_SHIPSET_Date < ToDay
    Download_LG_MIAMI_IW39_ORD = LG_MIAMI_IW39_ORD_Date < ToDay
    Download_LG_MIAMI_IW39_ORD_UNQ = LG_MIAMI_IW39_ORD_UNQ_Date < ToDay
    Download_LG_MIAMI_REWORK_HOURS = LG_MIAMI_REWORK_HOURS_Date < ToDay
    Download_LG_MIAMI_REWORK_VENDOR_HOURS = LG_MIAMI_REWORK_VENDOR_HOURS_Date < ToDay
    Download_LG_MIAMI_FPY = LG_MIAMI_FPY_Date < ToDay
    print("IW47_Date", IW47_Date)
    return (Download_IW39_Operations, Download_ZMRO_OPS, Download_ZMRO_SALES, Download_IW47, Download_REWORK_VENDOR,
            Download_IW39_Orders_REWORK_SHIPSET, Download_IW39_Operations_REWORK_SHIPSET, Download_LG_MIAMI_IW39_ORD,
            Download_LG_MIAMI_IW39_ORD_UNQ, Download_LG_MIAMI_REWORK_HOURS, Download_LG_MIAMI_REWORK_VENDOR_HOURS,
            Download_LG_MIAMI_FPY)


def CheckFilesDownloaded_Shipset():
    ZMRO_SALES = SAP_PATH + '\ZMRO_SALES.xlsx'
    CJI3 = SAP_PATH + '\CJI3.xlsx'
    LG_MIAMI_SHIPSET_HOURS = SAP_PATH + '\LG_MIAMI_SHIPSET_HOURS.xlsx'

    ToDay = datetime.now().date()
    ZMRO_SALES_Date = datetime.fromtimestamp(os.path.getmtime(ZMRO_SALES)).date()
    CJI3_Date = datetime.fromtimestamp(os.path.getmtime(CJI3)).date()
    LG_MIAMI_SHIPSET_HOURS_Date = datetime.fromtimestamp(os.path.getmtime(LG_MIAMI_SHIPSET_HOURS)).date()

    Download_CJI3 = CJI3_Date < ToDay
    Download_ZMRO_SALES = ZMRO_SALES_Date < ToDay
    Download_LG_MIAMI_SHIPSET_HOURS = LG_MIAMI_SHIPSET_HOURS_Date < ToDay

    return (Download_ZMRO_SALES, Download_CJI3, Download_LG_MIAMI_SHIPSET_HOURS)


def cleanupEnv():
    print("      ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "Cleaning up the Environment...")
    os.system("taskkill /f /im saplogon.exe")
    # os.system("taskkill /f /im cmd.exe")


# from pathlib import Path
def killexcel():
    # Kill Excel if filed othwewise
    try:
        os.system('taskkill /IM EXCEL.exe /T /F')
    except:
        True


RunNow = False
while True:
    DTNow = int(datetime.now().timestamp())
    SchTime = int(datetime.timestamp(datetime.combine(datetime.now(), datetime.min.time()))) + (
                (6 + 0 + (1 / 60)) * 3600)  # Run @ 9AM + 32400 Takes 5-7 hrs
    # SchTime = int(datetime.now().timestamp()) - 100
    if (DTNow > SchTime and DTNow - SchTime < 30) or RunNow:
        print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "starting script execution at: ")
        TrySAP = True
        TrySAP_Count = 0
        while TrySAP == True:

            print("      ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "Initiate base paths...")
            # PATH DEFINITION
            download_path = r'D:\PROJECT_DATA\SCHEDULING_APPLICATION\IW39_DAILY_UPDATE\IW39_Archive'
            folder_path = r'D:\PROJECT_DATA\SCHEDULING_APPLICATION\IW39_DAILY_UPDATE'
            SITE_INPUTS = r'D:\PROJECT_DATA\SCHEDULING_APPLICATION\IW39_DAILY_UPDATE\Site_Inputs'
            RFID_Path = r'D:\PROJECT_DATA\SCHEDULING_APPLICATION\IW39_DAILY_UPDATE\RFID_Data'
            IW39_DATA_UPDATE = r'\\yyz0sv02.goodrich.root.local\dept\In_House_Metrics\TABLEAU_REPORTS\Trinadh\Miami_ProgramFiles\IW39_DATA'
            NEW_DATA_UPDATE = r'\\yyz0sv02.goodrich.root.local\dept\In_House_Metrics\TABLEAU_REPORTS\Trinadh\Miami_ProgramFiles\NEW_ORDERS&OPERATIONS_DATA'
            alphabet_string = string.ascii_uppercase
            alphabet_list = list(alphabet_string)
            alphabet_list = alphabet_list + ["AA", "AB", "AC", "AD", "AE", "AF", "AG", "AH", "AI", "AJ", "AK", "AL"]
            params = urllib.parse.quote_plus(
                "DRIVER={SQL Server};SERVER=GUSALD2r.utcapp.com;DATABASE=MG_Digital;UID=MG_DigitalRW;PWD=Falconine21!")
            cnxn = pyodbc.connect(driver='{SQL Server}', server='GUSALD2r.utcapp.com', database='MG_Digital',
                                  uid='MG_DigitalRW', pwd='Falconine21!')
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            engine.connect()
            cnxn_exec = engine.connect()
            week_monday = datetime.now() - timedelta(days=datetime.now().weekday())
            week_sunday = week_monday + timedelta(days=6)
            # for Shipset
            SAP_PATH = r'\\blrfs\COE_RMS_MP_Analytics\CoE_ANALYTICS\Projects\Operations\LG MIAMI\Shipset Hours\SAP_DOWNLOAD'
            SITE_INPUTS2 = r'\\blrfs\COE_RMS_MP_Analytics\CoE_ANALYTICS\Projects\Operations\LG MIAMI\Shipset Hours\SITE_INPUTS'
            RAW_DATA_PUSH = r'\\blrfs\COE_RMS_MP_Analytics\CoE_ANALYTICS\Projects\Operations\LG MIAMI\Shipset Hours\RAW_DATA_PUSH'
            BACKUP = r'\\blrfs\COE_RMS_MP_Analytics\CoE_ANALYTICS\Projects\Operations\LG MIAMI\Shipset Hours\BACKUP'
            Shipset_Hours = r'\\blrfs\COE_RMS_MP_Analytics\CoE_ANALYTICS\Projects\Operations\LG MIAMI\Shipset Hours'
            print("      ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "Start program...")

            try:
                Stage = 'CHECK Files'
                (
                Download_IW39_Operations, Download_ZMRO_SALES, Download_ZMRO_OPS, Download_IW47, Download_REWORK_VENDOR,
                Download_IW39_Orders_REWORK_SHIPSET, Download_IW39_Operations_REWORK_SHIPSET,
                Download_LG_MIAMI_IW39_ORD, Download_LG_MIAMI_IW39_ORD_UNQ,
                Download_LG_MIAMI_REWORK_HOURS, Download_LG_MIAMI_REWORK_VENDOR_HOURS,
                Download_LG_MIAMI_FPY) = CheckFilesDownloaded_IW39()

                (Download_ZMRO_SALES_ShipSet, Download_CJI3,
                 Download_LG_MIAMI_SHIPSET_HOURS) = CheckFilesDownloaded_Shipset()

                Stage = 'IW39 SAP'
                if (
                        Download_IW39_Operations or Download_ZMRO_OPS or Download_ZMRO_SALES or Download_REWORK_VENDOR) and TrySAP_Count < 2:
                    saplogin('IW39', "8073196", "DMBlogon4")  # Uncomment this one
                    SendSapClosedMail('IW39', "8073196")  # Uncomment this one
                    TrySAP_Count = 0
                elif (
                        Download_IW39_Operations or Download_ZMRO_OPS or Download_ZMRO_SALES or Download_REWORK_VENDOR) and TrySAP_Count >= 2:
                    print("      ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'SAP Error Stop Now')
                    break
                else:
                    print("      ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'IW39 SAP Skip ')

                Stage = 'IW39 Process'
                if Download_IW39_Orders_REWORK_SHIPSET or Download_IW39_Operations_REWORK_SHIPSET:
                    Pre_Prcoess_IW39_Orders()  # Uncomment this one
                    TrySAP_Count = 0
                else:
                    print("      ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'Pre_Prcoess_IW39_Orders Skip ')

                Stage = 'IW39 ORD Process'
                if Download_LG_MIAMI_IW39_ORD or Download_LG_MIAMI_IW39_ORD_UNQ:
                    IW39_orders_ops_processing()  # Uncomment this one
                    TrySAP_Count = 0
                else:
                    print("      ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'IW39_orders_ops_processing Skip ')

                Stage = 'IW39 FPY Process'
                if Download_LG_MIAMI_FPY:
                    FPY_Data_Processing()  # Uncomment this one
                    TrySAP_Count = 0
                else:
                    print("      ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'FPY_Data_Processing Skip ')

                Stage = 'IW39 RWK Process'
                if Download_LG_MIAMI_REWORK_HOURS or Download_LG_MIAMI_REWORK_VENDOR_HOURS:
                    rework_hours_processing()  # Uncomment this one
                    SendMailToAdmin('IW39')  # Uncomment this one
                    TrySAP_Count = 0
                else:
                    print("      ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'rework_hours_processing Skip ')

                Stage = 'SHIPSET SAP'
                # Not considering if Download_CJI3 is failed, since it is failnig due to isufficient memory
                if (Download_ZMRO_SALES_ShipSet or Download_CJI3) and TrySAP_Count < 2:
                    saplogin('SHIPSET', "8289787", "PAns0464")  # Uncomment this one
                    SendSapClosedMail('SHIPSET', "8289787")  # Uncomment this one
                    TrySAP_Count = 0
                elif (Download_ZMRO_SALES_ShipSet) and TrySAP_Count >= 2:
                    print("      ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'SAP Error Stop Now')
                    break
                else:
                    print("      ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'SHIPSET SAP Skip ')

                Stage = 'SHIPSET Process'
                if Download_LG_MIAMI_SHIPSET_HOURS:
                    processShipSet()  # Uncomment this one
                    SendMailToAdmin('SHIPSET')  # Uncomment this one
                    TrySAP_Count = 0
                else:
                    print("      ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'processShipSet Skip ')

                TrySAP = False
            except Exception as e:
                print("      ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'), Stage, 'Error', str(e))
                TrySAP_Count += 1
                if TrySAP_Count >= 5:
                    break
                else:
                    if Stage == 'SHIPSET SAP':
                        ToDay = datetime.now().date()
                        ZMRO_SALES = SAP_PATH + '\ZMRO_SALES.xlsx'
                        ZMRO_SALES_Date = datetime.fromtimestamp(os.path.getmtime(ZMRO_SALES)).date()
                        Download_ZMRO_SALES = ZMRO_SALES_Date < ToDay
                        if not Download_ZMRO_SALES:
                            SendSapClosedMail('SHIPSET', "8289787")  # Uncomment this one
                        else:
                            print("      ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'Retry after 1 min ' + Stage)
                            time.sleep(60)
                    else:
                        print("      ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'Retry after 1 min ' + Stage)
                        time.sleep(60)
                cleanupEnv()

        # cleanupEnv()                      #Uncomment this one
        killexcel()  # Uncomment this one
        print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "Process completed at :")
        print("\n============================================================================\n")
        RunNow = False
    else:
        time.sleep(2)
