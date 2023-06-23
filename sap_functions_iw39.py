import os
from datetime import datetime, timedelta
import time
import win32com.client
import generic_functions
import pandas as pd
from dotenv import load_dotenv
from logger_master import setup_logger

logger = setup_logger('sap_functions_iw39')

# load constants
load_dotenv()
download_path = os.getenv('download_path')


def Download_IW39_Operations(session):
    logger.info('IW39 Start')
    session.findById("wnd[0]/tbar[0]/okcd").text = "/nIW39"
    session.findById("wnd[0]").sendVKey(0)

    logger.info('IW39 set Filters')
    session.findById("wnd[0]/usr/ctxtAUART-LOW").showContextMenu()
    session.findById("wnd[0]/usr").selectContextMenuItem("%019")
    session.findById("wnd[1]/usr/tabsTAB_STRIP/tabpSIVA/ssubSCREEN_HEADER:SAPLALDB:3010/tblSAPLALDBSINGLE/ctxtRSCSEL_255-SLOW_I[1,0]").text = "ZCHD"
    session.findById("wnd[1]/usr/tabsTAB_STRIP/tabpSIVA/ssubSCREEN_HEADER:SAPLALDB:3010/tblSAPLALDBSINGLE/ctxtRSCSEL_255-SLOW_I[1,1]").text = "XSUB"
    session.findById("wnd[1]/usr/tabsTAB_STRIP/tabpSIVA/ssubSCREEN_HEADER:SAPLALDB:3010/tblSAPLALDBSINGLE/ctxtRSCSEL_255-SLOW_I[1,2]").text = "ZINT"
    session.findById("wnd[1]/tbar[0]/btn[8]").press()
    session.findById("wnd[0]/usr/ctxtIWERK-LOW").text = "2405"
    session.findById("wnd[0]/tbar[1]/btn[8]").press()

    logger.info('export IW39_Orders start')
    LoopCount = 0
    ExportComplete = False
    while not ExportComplete:
        try:
            generic_functions.kill_excel()
            session.findById("wnd[0]/tbar[1]/btn[16]").press()
            session.findById("wnd[1]/tbar[0]/btn[0]").press()
            session.findById("wnd[1]/usr/subSUBSCREEN_STEPLOOP:SAPLSPO5:0150/sub:SAPLSPO5:0150/radSPOPLI-SELFLAG[0,0]").select()
            session.findById("wnd[1]/usr/subSUBSCREEN_STEPLOOP:SAPLSPO5:0150/sub:SAPLSPO5:0150/radSPOPLI-SELFLAG[0,0]").setFocus()
            session.findById("wnd[1]/tbar[0]/btn[0]").press()
            session.findById("wnd[1]/tbar[0]/btn[0]").press()
            excel = win32com.client.Dispatch("Excel.Application")
            for wb in excel.Workbooks:
                if wb.Name == 'Worksheet in Basis (1)':
                    if "IW39_Orders.xlsx" in os.listdir(download_path):
                        os.remove(os.path.join(download_path, 'IW39_Orders.xlsx'))
                        time.sleep(3)
                    wb.SaveAs(os.path.join(download_path, 'IW39_Orders.xlsx'))
                    logger.info('export IW39_Orders complete')
                    ExportComplete = True
            session.findById("wnd[1]/tbar[0]/btn[0]").press()
        except Exception as e:
            logger.error('export IW39_Orders failed')
            logger.error(str(e))
            LoopCount += 1
            if LoopCount > 5:
                break

    time.sleep(10)
    generic_functions.kill_excel()

    logger.info('IW39_Operations data - start')
    session.findById("wnd[0]/usr/cntlGRID1/shellcont/shell").setCurrentCell(-1, "")
    session.findById("wnd[0]/usr/cntlGRID1/shellcont/shell").selectAll()
    session.findById("wnd[0]/tbar[1]/btn[43]").press()

    logger.info('IW39_Operations data')
    LoopCount = 0
    ExportComplete = False
    while not ExportComplete:
        try:
            generic_functions.kill_excel()
            session.findById("wnd[0]/tbar[1]/btn[16]").press()
            session.findById("wnd[1]/tbar[0]/btn[0]").press()
            session.findById("wnd[0]/tbar[1]/btn[16]").press()
            session.findById("wnd[1]/usr/subSUBSCREEN_STEPLOOP:SAPLSPO5:0150/sub:SAPLSPO5:0150/radSPOPLI-SELFLAG[0,0]").select()
            session.findById("wnd[1]/usr/subSUBSCREEN_STEPLOOP:SAPLSPO5:0150/sub:SAPLSPO5:0150/radSPOPLI-SELFLAG[0,0]").setFocus()
            session.findById("wnd[1]/tbar[0]/btn[0]").press()
            session.findById("wnd[1]/tbar[0]/btn[0]").press()
            excel = win32com.client.Dispatch("Excel.Application")
            for wb in excel.Workbooks:
                if wb.Name == 'Worksheet in Basis (1)':
                    if "IW39_Operations.xlsx" in os.listdir(download_path):
                        try:
                            os.remove(os.path.join(download_path, 'IW39_Operations.xlsx'))
                        except:
                            pass
                        time.sleep(3)
                    wb.SaveAs(os.path.join(download_path, 'IW39_Operations.xlsx'))
                    logger.info('IW39_Operations data downloaded')
                    ExportComplete = True
            session.findById("wnd[1]/tbar[0]/btn[0]").press()
        except Exception as e:
            logger.error('export IW39_Operations failed')
            logger.error(str(e))
            LoopCount += 1
            if LoopCount > 5:
                break

    time.sleep(10)
    generic_functions.kill_excel()
    generic_functions.cleanup_env()
    logger.info('IW39 END')


def Download_ZMRO_OPS(session):
    logger.info('ZMRO_OPS prepare Order.txt')
    df = pd.read_excel(os.path.join(download_path, "IW39_Orders.xlsx"))
    result_df = df[['Order']]
    Order_txt = result_df['Order'].to_list()
    with open(os.path.join(download_path, 'Order.txt'), 'w') as file:
        for ordr in Order_txt:
            file.write(str(ordr))
            file.write('\n')

    logger.info('ZMRO_OPS Start')
    session.findById("wnd[0]/tbar[0]/okcd").text = "/NZMRO_OPS"
    session.findById("wnd[0]").sendVKey(0)
    session.findById("wnd[0]/usr/ctxtS_PLANT-LOW").text = "2405"
    session.findById("wnd[0]/usr/txtS_TYPE-LOW").text = "XSUB"
    session.findById("wnd[0]/usr/btn%_S_TYPE_%_APP_%-VALU_PUSH").press()
    session.findById("wnd[1]/usr/tabsTAB_STRIP/tabpSIVA/ssubSCREEN_HEADER:SAPLALDB:3010/tblSAPLALDBSINGLE/txtRSCSEL_255-SLOW_I[1,1]").text = "ZCHD"
    session.findById("wnd[1]/usr/tabsTAB_STRIP/tabpSIVA/ssubSCREEN_HEADER:SAPLALDB:3010/tblSAPLALDBSINGLE/txtRSCSEL_255-SLOW_I[1,2]").text = "ZINT"
    session.findById("wnd[1]/tbar[0]/btn[8]").press()
    session.findById("wnd[0]/usr/btn%_S_ORD_%_APP_%-VALU_PUSH").press()
    session.findById("wnd[1]/tbar[0]/btn[23]").press()
    session.findById("wnd[2]/usr/ctxtDY_PATH").text = download_path
    session.findById("wnd[2]/usr/ctxtDY_FILENAME").text = "Order.txt"
    session.findById("wnd[2]/tbar[0]/btn[0]").press()
    time.sleep(10)
    session.findById("wnd[1]/tbar[0]/btn[8]").press()
    session.findById("wnd[0]/tbar[1]/btn[8]").press()
    session.findById("wnd[0]/tbar[1]/btn[33]").press()
    session.findById("wnd[1]/usr/ssubD0500_SUBSCREEN:SAPLSLVC_DIALOG:0501/cmbG51_USPEC_LBOX").key = "X"
    session.findById("wnd[1]/usr/ssubD0500_SUBSCREEN:SAPLSLVC_DIALOG:0501/cntlG51_CONTAINER/shellcont/shell").setCurrentCell(5, "TEXT")
    session.findById("wnd[1]/usr/ssubD0500_SUBSCREEN:SAPLSLVC_DIALOG:0501/cntlG51_CONTAINER/shellcont/shell").selectedRows = "5"
    session.findById("wnd[1]/usr/ssubD0500_SUBSCREEN:SAPLSLVC_DIALOG:0501/cntlG51_CONTAINER/shellcont/shell").clickCurrentCell()
    time.sleep(10)
    session.findById("wnd[0]/tbar[1]/btn[43]").press()
    session.findById("wnd[1]/usr/ctxtDY_PATH").text = download_path
    session.findById("wnd[1]/usr/ctxtDY_FILENAME").text = "ZMRO_OPS.xlsx"
    session.findById("wnd[1]/usr/ctxtDY_FILENAME").caretPosition = 13
    session.findById("wnd[1]/tbar[0]/btn[0]").press()

    logger.info('ZMRO_OPS export data')
    generic_functions.kill_excel()
    try:
        session.findById("wnd[0]/tbar[0]/btn[15]").press()
        session.findById("wnd[0]/tbar[0]/btn[15]").press()
    except:
        pass
    logger.info('ZMRO_OPS end')


def Download_ZMRO_SALES(session):
    logger.info('ZMROSALES Start')
    session.findById("wnd[0]/tbar[0]/okcd").text = "/NZMROSALES"
    session.findById("wnd[0]").sendVKey(0)
    logger.info('ZMROSALES set filters')
    session.findById("wnd[0]/usr/ctxtS_VKORG-LOW").Text = "4400"
    session.findById("wnd[0]/usr/ctxtS_VTWEG-LOW").Text = "01"
    session.findById("wnd[0]/usr/ctxtS_VTWEG-HIGH").Text = "04"
    session.findById("wnd[0]/usr/ctxtS_SPART-LOW").Text = "25"
    session.findById("wnd[0]/usr/ctxtS_QMART-LOW").Text = "x1"
    session.findById("wnd[0]/usr/ctxtS_QMART-HIGH").Text = "xr"
    session.findById("wnd[0]/usr/ctxtS_WERKS-LOW").Text = "2405"
    session.findById("wnd[0]/usr/ctxtS_ERDAT-HIGH").text = datetime.now().strftime('%m/%d/%Y')
    session.findById("wnd[0]/usr/chkP_OPEN").Selected = True
    session.findById("wnd[0]/usr/chkP_SHIP").Selected = True
    session.findById("wnd[0]/usr/chkP_INVC").Selected = True
    session.findById("wnd[0]/usr/ctxtP_VARI").Text = "Miami"
    session.findById("wnd[0]/tbar[1]/btn[8]").press()

    logger.info('ZMROSALES export data')
    session.findById("wnd[0]/usr/shell/shellcont/shell").pressToolbarContextButton("&MB_EXPORT")
    session.findById("wnd[0]/usr/shell/shellcont/shell").selectContextMenuItem("&XXL")
    session.findById("wnd[1]/usr/ctxtDY_PATH").text = download_path
    session.findById("wnd[1]/usr/ctxtDY_FILENAME").text = "ZMRO_SALES.xlsx"
    session.findById("wnd[1]/tbar[0]/btn[11]").press()
    generic_functions.kill_excel()

    try:
        session.findById("wnd[0]/tbar[0]/btn[15]").press()
        session.findById("wnd[0]/tbar[0]/btn[15]").press()
    except:
        pass
    logger.info('ZMROSALES end')


def Download_IW47(session):
    time.sleep(10)

    logger.info('IW47 Start')
    session.findById("wnd[0]/tbar[0]/okcd").text = "/NIW47"
    session.findById("wnd[0]").sendVKey(0)
    session.findById("wnd[0]/usr/ctxtERSDA_C-LOW").text = (datetime.now() - timedelta(days=7)).strftime('%m/%d/%Y')
    session.findById("wnd[0]/usr/ctxtERSDA_C-HIGH").text = datetime.now().strftime('%m/%d/%Y')
    session.findById("wnd[0]/usr/ctxtERSDA_C-HIGH").setFocus()
    session.findById("wnd[0]/usr/ctxtERSDA_C-HIGH").caretPosition = 10
    session.findById("wnd[0]/tbar[1]/btn[8]").press()

    logger.info('IW47 Export')
    LoopCount = 0
    ExportComplete = False
    while ExportComplete == False:
        try:
            generic_functions.kill_excel()
            session.findById("wnd[0]/mbar/menu[0]/menu[6]").select()
            session.findById("wnd[1]/tbar[0]/btn[0]").press()
            session.findById("wnd[1]/usr/subSUBSCREEN_STEPLOOP:SAPLSPO5:0150/sub:SAPLSPO5:0150/radSPOPLI-SELFLAG[0,0]").select()
            session.findById("wnd[1]/usr/subSUBSCREEN_STEPLOOP:SAPLSPO5:0150/sub:SAPLSPO5:0150/radSPOPLI-SELFLAG[0,0]").setFocus()
            session.findById("wnd[1]/tbar[0]/btn[0]").press()
            session.findById("wnd[1]/tbar[0]/btn[0]").press()
            logger.info('IW47 Save')
            excel = win32com.client.Dispatch("Excel.Application")
            for wb in excel.Workbooks:
                if wb.Name == 'Worksheet in Basis (1)':
                    if "IW47.xlsx" in os.listdir(download_path):
                        os.remove(download_path + '\IW47.xlsx')
                        time.sleep(3)
                    wb.SaveAs(download_path + '\IW47.xlsx')
                    logger.info('export IW47 complete')
                    ExportComplete = True
        except Exception as e:
            logger.info('export IW47 failed')
            logger.info(str(e))
            LoopCount += 1
            if LoopCount > 5:
                break
    time.sleep(10)
    generic_functions.kill_excel()
    try:
        logger.info('IW47 Extract')
        session.findById("wnd[0]/tbar[0]/btn[15]").press()
        session.findById("wnd[0]/tbar[0]/btn[15]").press()
    except:
        pass
    logger.info('IW47 END')


def Download_REWORK_VENDOR(session):
    logger.info('ZMRO_SUBCON_WIP Start')
    session.findById("wnd[0]/tbar[0]/okcd").text = "/NZMRO_SUBCON_WIP"
    session.findById("wnd[0]/tbar[0]/btn[0]").press()
    session.findById("wnd[0]/usr/radRBCLSD").select()

    logger.info('ZMRO_SUBCON_WIP Set FIlters')
    session.findById("wnd[0]/usr/ctxtSP$00005-LOW").text = "2405"
    session.findById("wnd[0]/usr/ctxtSP$00004-LOW").text = "X"
    session.findById("wnd[0]/usr/ctxtSP$00016-LOW").text = datetime.now().replace(day=1, month=1).strftime('%m/%d/%Y')
    session.findById("wnd[0]/usr/ctxtSP$00016-HIGH").text = (datetime.now() - timedelta(days=1)).strftime('%m/%d/%Y')
    logger.info('ZMRO_SUBCON_WIP Extract')
    session.findById("wnd[0]/tbar[1]/btn[8]").press()

    logger.info('ZMRO_SUBCON_WIP Export')
    try:
        session.findById("wnd[0]/usr/cntlCONTAINER/shellcont/shell").pressToolbarContextButton("&MB_EXPORT")
        session.findById("wnd[0]/usr/cntlCONTAINER/shellcont/shell").selectContextMenuItem("&XXL")
        try:
            session.findById("wnd[1]/tbar[0]/btn[0]").press()
        except:
            pass

        logger.info('ZMRO_SUBCON_WIP Download')
        session.findById("wnd[1]/usr/ctxtDY_PATH").text = download_path
        session.findById("wnd[1]/usr/ctxtDY_FILENAME").text = "REWORK_VENDOR.xlsx"
        session.findById("wnd[1]/tbar[0]/btn[11]").press()

        try:
            session.findById("wnd[0]/tbar[0]/btn[15]").press()
            session.findById("wnd[0]/tbar[0]/btn[15]").press()
        except:
            pass
        logger.info('ZMRO_SUBCON_WIP Close WB')
        generic_functions.kill_excel()
    except Exception as e:
        logger.info('REWORK_VENDOR Export Error    <<<=======')
        logger.info(str(e))
    logger.info('ZMRO_SUBCON_WIP END')



