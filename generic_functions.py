
import smtplib, ssl, traceback
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os, os.path

import time
from datetime import datetime
from logger_master import setup_logger
logger = setup_logger('generic_functions')
import pandas as pd

# Function to push data to db
def push_data_db(engine, df, db_name, folder_path, if_exists='replace', max_retry=30):
    """Retries writing to the database if an error occurs."""
    logger.info(f'push {db_name} to database - START')
    retry_count = 0
    retry = True
    while retry and retry_count < max_retry:
        try:
            _write_to_db_and_excel(engine, df, db_name, folder_path, if_exists)
            retry = False
        except Exception as e:
            logger.error(f'Retry count: {retry_count}, Error: {str(e)}')
            retry_count += 1
            time.sleep(10)
    if retry:
        raise ValueError(f"Max retry limit exceeded: {max_retry}")
    logger.info(f'push {db_name} to database - END')


def get_dataframe_using_sql(query, cnxn):
    logger.info(f'get data from sql query "{query}"')
    DataDownload = True
    ReTryCount = 0
    while DataDownload:
        ReTryCount += 1
        try:
            logger.info('Try to get data from SQL')
            df = pd.read_sql(query, cnxn)
            return df
        except Exception as e:
            logger.error(str(e))
            if ReTryCount >= 10:
                raise ValueError(str(e))
            time.sleep(60)


def send_email_update(flag, UserID=None):
    logger.info(f'send email, flag {flag}')
    Subject = ''
    MsgContent = ''
    if flag == "IW39_Process":
        MsgContent = 'Hi,\n\n This is an automated message. Please do not reply to this message.\n\nData related to planning files, IW47, IW39, New Orders & Operations has been updated and processed. \n\nRegards,\nPlanning Data Tool'
        Subject = 'Planning Files Updated'
    elif flag == "IW39_Process":
        MsgContent = 'Hi,\n\n This is an automated message. Please do not reply to this message.\n\nData related to Shipset Operations has been updated and processed. \n\nRegards,\nShipset Data Tool'
        Subject = 'Shipset data updated'
    elif flag == "SAP_Closed":
        MsgContent = "Hi,\n\n This is an automated message. Please do not reply to this message.\n\n SAP Closed for {0}/{1} . \n\nRegards,\nPlanning Data Tool".format(flag, UserID)
        Subject = 'SAP closed - {0}/{1}'.format(flag, UserID)

    email_engine(Subject, MsgContent)
    logger.info(f'completed email, flag {flag}')

def email_engine(subject, msgContent):
    logger.info("started SendMailToAdmin")
    # SMTP server and Port information
    SenderMail = 'PlanningData.LGMiamiOps@collins.com'
    ReceiverMail = ['Prarthana.BaswarajSangshettyPatil@collins.com', 'raghuram.alla@collins.com',
                    'Trinadh.Pentela@collins.com', 'jaganmohanreddy.kalukurthi@collins.com']
    SMTPServers = ['QUSNWADY.utcapp.com', 'QUSNWAE9.utcapp.com', 'QUSNWADV.utcapp.com', 'QUSNWADW.utcapp.com',
                   'QUSNWADX.utcapp.com', 'QUSMNA5K.utcapp.com', 'QUSMNA5L.utcapp.com', 'QUSMNA5M.utcapp.com',
                   'QUSMNA60.utcapp.com', 'uusnwa7g.corp.utc.com', 'mailhub.utc.com']
    Port = 25

    Message = MIMEMultipart()
    Message['Subject'] = subject
    Message['From'] = SenderMail
    Message['To'] = ','.join(ReceiverMail)
    Message.attach(MIMEText(msgContent))

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
            tb_str = traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__)
            tb_str = ''.join(tb_str)  # This converts the list of strings into one single string
            logger.info(f"Error in SendMailToAdmin: {str(e)}. Traceback: {tb_str}")
            continue


def _write_to_db_and_excel(engine, df, db_name, folder_path, if_exists):
    """Writes the dataframe to database and excel file."""
    chunk_limit = int((2100/len(df.columns))*0.9)
    df.to_sql(name=db_name, schema='dbo', index=False, con=engine, if_exists=if_exists, chunksize=chunk_limit, method='multi')
    df.to_excel(os.path.join(folder_path, f'{db_name}.xlsx'), index=False)

def cleanup_env():
    logger.debug("Cleaning up the Environment...")
    os.system("taskkill /f /im saplogon.exe")


def kill_excel():
    try:
        logger.debug("kill excel")
        os.system('taskkill /IM EXCEL.exe /T /F')
    except:
        pass


def CheckValidFileTimeStamp(path, fileName):
    try:
        FilePath = os.path.join(path, fileName)
        ToDay = datetime.now().date()
        FileDate = datetime.fromtimestamp(os.path.getmtime(FilePath)).date()
        Download_File = FileDate < ToDay
    except Exception as e:
        if 'cannot find the file specified' not in str(e):
            logger.error(f'CheckValidFileTimeStamp Error: {str(e)}')
        Download_File = True

    return Download_File