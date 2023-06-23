import win32com.client
import subprocess
import time
import os
from dotenv import load_dotenv
from logger_master import setup_logger

# Setup logger
logger = setup_logger('sap_engine')
# load constants
load_dotenv()
SAP_EXE = os.getenv('SAP_EXE')


def saplogin(UserID, Password):
    """
        Logs into SAP system.
        Args:
        UserID: str
            SAP user ID
        Password: str
            SAP password
        Returns:
        session object if successful, None otherwise.
    """
    try:
        logger.debug(" : Cycle Start : Opening SAP - ")
        subprocess.Popen(SAP_EXE)
        time.sleep(10)
        SapGuiAuto = win32com.client.GetObject('SAPGUI')

        if not isinstance(SapGuiAuto, win32com.client.CDispatch):
            logger.error('Could not get SAPGUI object.')
            return None

        application = SapGuiAuto.GetScriptingEngine

        if not isinstance(application, win32com.client.CDispatch):
            SapGuiAuto = None
            logger.error('Could not get scripting engine.')
            return None

        logger.debug('Trying to connect to P01')
        connection = application.OpenConnection("(01) P01 - PROD ECC", True)

        if not isinstance(connection, win32com.client.CDispatch):
            application = None
            SapGuiAuto = None
            logger.error('Could not open connection.')
            return None

        session = connection.Children(0)

        if not isinstance(session, win32com.client.CDispatch):
            connection = None
            application = None
            SapGuiAuto = None
            logger.error('Could not create session.')
            return None

        logger.debug('login sap')
        session.findById("wnd[0]/usr/txtRSYST-BNAME").text = UserID
        session.findById("wnd[0]/usr/pwdRSYST-BCODE").text = Password
        session.findById("wnd[0]").sendVKey(0)

        try:
            # if incorrect password entered earlier
            session.findById("wnd[1]/tbar[0]/btn[0]").press()
        except Exception as e:
            logger.error(f'Error occurred while pressing button 0. ERROR: {e}')
            # Continue to next command even if error occurs here

        session.findById("wnd[1]/usr/btnBUTTON_1").press()

        session.findById("wnd[0]").maximize()

        return session
    except Exception as e:
        logger.error(f'Error occurred while starting SAP. ERROR: {e}')

        return None

