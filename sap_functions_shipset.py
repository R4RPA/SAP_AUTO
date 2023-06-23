import os
from datetime import datetime
import generic_functions
import pandas as pd
from dotenv import load_dotenv
from logger_master import setup_logger

logger = setup_logger('sap_functions_shipset')

# load constants
load_dotenv()
SAP_PATH = os.getenv('SAP_PATH')


def Download_ZMRO_SALES_ShipSet(session):
    logger.info('Shipset ZMROSALES Start')
    basic_start_date = "01/01/2008"  # Trial including the statement
    basic_end_date = datetime.now().strftime("%m/%d/%Y")  # todays date
    session.findById("wnd[0]/tbar[0]/okcd").text = "/nZMROSALES"
    session.findById("wnd[0]").sendVKey(0)
    session.findById("wnd[0]/usr/ctxtS_VKORG-LOW").Text = "4400"
    session.findById("wnd[0]/usr/ctxtS_VTWEG-LOW").Text = "01"
    session.findById("wnd[0]/usr/ctxtS_VTWEG-HIGH").Text = "04"
    session.findById("wnd[0]/usr/ctxtS_SPART-LOW").Text = "25"
    session.findById("wnd[0]/usr/ctxtS_QMART-LOW").Text = "x1"
    session.findById("wnd[0]/usr/ctxtS_QMART-HIGH").Text = "xr"
    session.findById("wnd[0]/usr/ctxtS_WERKS-LOW").Text = "2405"
    session.findById("wnd[0]/usr/ctxtS_ERDAT-LOW").text = basic_start_date  # including for Trial
    session.findById("wnd[0]/usr/ctxtS_ERDAT-HIGH").text = basic_end_date  # including for TRial
    session.findById("wnd[0]/usr/chkP_OPEN").Selected = True
    session.findById("wnd[0]/usr/chkP_SHIP").Selected = True  # Made it True for Trial/before it was False
    session.findById("wnd[0]/usr/chkP_INVC").Selected = True  # Made it True for Trial/before it was False
    session.findById("wnd[0]/usr/ctxtP_VARI").Text = "Miami"
    session.findById("wnd[0]/tbar[1]/btn[8]").press()
    session.findById("wnd[0]/usr/shell/shellcont/shell").pressToolbarContextButton("&MB_EXPORT")
    session.findById("wnd[0]/usr/shell/shellcont/shell").selectContextMenuItem("&XXL")
    session.findById("wnd[1]/usr/ctxtDY_PATH").text = SAP_PATH
    session.findById("wnd[1]/usr/ctxtDY_FILENAME").text = "ZMRO_SALES.xlsx"
    session.findById("wnd[1]/tbar[0]/btn[11]").press()
    logger.info('Shipset ZMROSALES End')
    generic_functions.kill_excel()


def Download_CJI3(session):
    logger.info('CJI3 Start')

    # Extracting the data of Sales Orders that are having ShipSet
    ZMRO_SALES = pd.read_excel(os.path.join(SAP_PATH, "ZMRO_SALES.xlsx"))
    ZMRO_SALES.dropna(subset=['Aircraft Serial Number'], inplace=True)
    WBS_ELEMENT = ZMRO_SALES['WBS'].to_list()

    write_list_to_file(os.path.join(SAP_PATH, "WBS_ELEMENT.txt"), WBS_ELEMENT)

    composite_list = [WBS_ELEMENT[x:x + 50] for x in range(0, len(WBS_ELEMENT), 50)]
    iCount = 0

    download_cji3_part(session, iCount, composite_list)

    logger.info('CJI3 Start merge')
    merge_cji3_files(iCount)
    logger.info('CJI3 End merge')


def write_list_to_file(file_path, items):
    with open(file_path, 'w') as f:
        for item in items:
            f.write("%s\n" % item)


def download_cji3_part(session, iCount, composite_list):
    for sub_list in composite_list:
        iCount += 1
        file_name = "CJI3_{}.xlsx".format(iCount)
        if generic_functions.CheckValidFileTimeStamp(SAP_PATH, file_name):
            file_path = os.path.join(SAP_PATH, "WBS_ELEMENT_{0}.txt".format(iCount))
            write_list_to_file(file_path, sub_list)

            logger.info('CJI3 download Part {} of {}'.format(iCount, len(composite_list)))
            # DOWNLOADING CJI3
            session.findById("wnd[0]/tbar[0]/okcd").text = "/nCJI3"
            session.findById("wnd[0]").sendVKey(0)

            try:
                session.findById("wnd[1]/usr/sub:SAPLSPO4:0300/ctxtSVALD-VALUE[0,21]").text = "7900"
                session.findById("wnd[1]/tbar[0]/btn[0]").press()
                session.findById("wnd[1]/usr/ctxtTCNT-PROF_DB").text = "000000000001"
                session.findById("wnd[1]/tbar[0]/btn[0]").press()
            except:
                pass

            session.findById("wnd[0]/usr/ctxtCN_PSPNR-LOW").showContextMenu()
            session.findById("wnd[0]/usr").selectContextMenuItem("%013")
            session.findById("wnd[1]/tbar[0]/btn[23]").press()
            session.findById("wnd[2]/usr/ctxtDY_PATH").text = SAP_PATH
            session.findById("wnd[2]/usr/ctxtDY_FILENAME").text = "WBS_ELEMENT_{0}.txt".format(iCount)
            session.findById("wnd[2]/tbar[0]/btn[0]").press()
            session.findById("wnd[1]/tbar[0]/btn[8]").press()
            session.findById("wnd[0]/usr/ctxtP_DISVAR").text = "/BUR_GM_BYSO"
            session.findById("wnd[0]/usr/ctxtR_BUDAT-LOW").showContextMenu()
            session.findById("wnd[0]/usr").selectContextMenuItem("DELACTX")
            session.findById("wnd[0]/usr/btnBUT1").press()
            session.findById("wnd[1]/usr/txtKAEP_SETT-MAXSEL").text = "999999"
            session.findById("wnd[1]/tbar[0]/btn[0]").press()
            session.findById("wnd[0]/tbar[1]/btn[8]").press()
            session.findById("wnd[0]/tbar[1]/btn[43]").press()
            session.findById("wnd[1]/tbar[0]/btn[0]").press()
            session.findById("wnd[1]/usr/ctxtDY_PATH").text = SAP_PATH
            session.findById("wnd[1]/usr/ctxtDY_FILENAME").text = file_name
            session.findById("wnd[1]/tbar[0]/btn[11]").press()
            logger.info('CJI3 downloaded Part {} of {}'.format(iCount, len(composite_list)))
            generic_functions.kill_excel()
        else:
            logger.info('CJI3 SKIP Part {} of {}'.format(iCount, len(composite_list)))


def merge_cji3_files(iCount):
    dest_file_path = os.path.join(SAP_PATH, "CJI3.xlsx")
    cji3_df_list = []
    logger.info('CJI3 merge Start')

    for file_num in range(1, iCount + 1):
        sub_file_path = os.path.join(SAP_PATH, "CJI3_{}.xlsx".format(file_num))
        logger.info('CJI3 merged', file_num)
        sub_df = pd.read_excel(sub_file_path)
        cji3_df_list.append(sub_df)

    cji3_df = pd.concat(cji3_df_list)
    cji3_df.to_excel(dest_file_path, index=False)
    logger.info('CJI3 merge End')

