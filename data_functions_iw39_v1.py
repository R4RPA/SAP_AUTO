import os
import pandas as pd
from datetime import datetime
import time
from dotenv import load_dotenv

import generic_functions
from logger_master import setup_logger
import numpy as np

logger = setup_logger('data_functions_iw39')

# load constants
load_dotenv()
folder_path = os.getenv('folder_path')
download_path = os.getenv('download_path')
SITE_INPUTS = os.getenv('SITE_INPUTS')
engine = ''

# Initializing the global variables
dept_dict = {"Assembly": "Assembly", "Bush and Hone": "Bush & Hone", "Bushing": "Bush & Hone", "Dissy": "Disassembly",
             "Engineering Hold": "NA", "Kitting": "Stockroom / Kitting", "Quarantine": "NA", "Grinding": "Machine Shop",
             "Large Lathe": "NA", "Large Mills": "NA", "Medium Lathe": "NA", "Small Lathes": "NA", "Small Mills": "NA",
             "NDT": "Quality", "OSP": "Quality", "Paint": "Paint", "Alodine": "NA", "Bake": "NA",
             "CAD Plating": "Plating", "Chrome Plating": "Plating", "Grit blast": "NA", "Nickel Plating": "Plating",
             "Passivate": "NA", "Shotpeen": "Plating", "Strip": "Plating", "Final Inspection": "Quality",
             "Receiving Inspection": "Quality", "IPI": "Quality", "Plumbing": "Plumming", "S&R": "Survey & Repair"}

suspected_orgin_dict = {"Assembly": "Assembly", "Bush & Hone": "Bush & Hone", "Disassembly": "Disassembly", "Grind": "Machine Shop",
                        "Machine Shop": "Machine Shop", "NDT": "Quality", "Paint": "Paint", "Cad Strip": "Plating", "Chrome Strip": "Plating",
                        "Nickel Strip": "Plating", "Cad Plating": "Plating", "Chrome Plating": "Plating", "Nickle Plating": "Plating",
                        "Plumming": "Plumming", "Stockroom / Kitting": "Stockroom / Kitting", "Survey & Repair": "Survey & Repair",
                        "Wire Shop": "Machine Shop", "IPI": "Quality", "Vendor": "Vendor", "Shot Peen": "Plating", "Unknown": "NA"}



# Main function to pre process IW39 orders and operations
def preprocess_iw39_orders(Process_Orders, Process_Operations):
    logger.info("Processing unconfirmed orders")
    logger.info("Processing IW39_Orders and IW39_Operations files to delete all the DLT type orders")

    mat_master = pd.read_excel(folder_path + '\Material Master Data.xlsx')
    mat_master = mat_master[mat_master['Planning'] == 'Yes']

    if Process_Orders:
        _process_iw39_orders(mat_master)
    else:
        logger.info("IW39_Orders_REWORK_SHIPSET Skip")

    if Process_Operations:
        _process_iw39_operations(mat_master)
    else:
        logger.info("IW39_Operations_REWORK_SHIPSET Skip")

    logger.info("Processing IW39_Orders End")


# Main function to process IW39 orders and operations
def IW39_orders_ops_processing():
    IW39OP_DF, IW39ORD_DF = _load_excel_files()
    IW39OP_DF = _process_operations(IW39OP_DF)
    SCM_FINAL = _factorize_and_complete_status(IW39OP_DF)
    IW39ORD_DF_Copy = IW39ORD_DF.copy()
    IW39ORD_DF = _process_orders(IW39ORD_DF, SCM_FINAL)
    IW39ORD_DF = _process_and_join_sales_data(IW39ORD_DF)
    IW39ORD_DF = _fill_gags_using_orders_data(IW39ORD_DF, IW39ORD_DF_Copy)
    generic_functions.push_data_db(engine, IW39ORD_DF, 'LG_MIAMI_IW39_ORD', download_path, 'replace')
    IW39ORD_DF_New = _process_unique_orders_data(IW39ORD_DF)
    generic_functions.push_data_db(engine, IW39ORD_DF_New, 'LG_MIAMI_IW39_ORD_UNQ', download_path, 'replace')


# Main function to process IW39 FPY Data
def FPY_Data_Processing():
    logger.info("Processing FPY Dashboard Data")
    df, wc_df, routing_df = _read_excel_files()
    data = _clean_data(df, wc_df, routing_df)
    DF2 = _read_sql_data()
    merge_data = _merge_dataframes(data, DF2)
    generic_functions.push_data_db(engine, merge_data, 'LG_MIAMI_FPY', download_path, 'replace')
    logger.info("Processing FPY Dashboard Data Completed")


# Main function to process Rework Data
def process_rework_hours(process_rework_hours_data, process_rework_vendor_hours_data):
    logger.info('process_rework_hours - START')
    ZMRO_SALES, WC_DEF, IW39_ORDERS, IW39_OPERATIONS, IW47_DATA, REWORK_VENDOR = _get_rework_source_data()
    
    if process_rework_hours_data:
        logger.info('process_rework_hours_data - START')
        IW39_OPERATIONS = _process_operations_data(ZMRO_SALES, WC_DEF, IW39_ORDERS, IW39_OPERATIONS, IW47_DATA)
        IW39_DB = _get_rework_data_from_sql()
        IW39_OPERATIONS = _process_operations_data_with_sql(IW39_OPERATIONS, IW39_DB)
        _push_operations_data_to_sql(IW39_OPERATIONS)
        logger.info('process_rework_hours_data - END')
    else:
        logger.info('process_rework_hours_data - SKIP')
    if process_rework_vendor_hours_data:
        logger.info('process_rework_vendor_hours_data - START')
        _push_rework_vendor_hours_data_to_sql(REWORK_VENDOR)
        logger.info('process_rework_vendor_hours_data - END')
    else:
        logger.info('process_rework_vendor_hours_data - SKIP')
    logger.info('process_rework_hours - END')


#############################################
# preprocess_iw39_orders  FUNCTIONS --- START

# function to process IW39 orders
def _process_iw39_orders(mat_master):
    iw39ord_df = pd.read_excel(download_path + '\IW39_Orders.xlsx')
    zmro_ops_df = pd.read_excel(download_path + '\ZMRO_OPS.xlsx')

    iw39ord_df.to_excel(download_path + '\IW39_Orders_REWORK_SHIPSET.xlsx', index=False)

    program_map = zmro_ops_df[['Order', 'Aircraft']].set_index('Order').to_dict()['Aircraft']
    iw39ord_df['Program'] = iw39ord_df['Order'].map(program_map)

    aircraft_map = mat_master.set_index('Incoming Material')['Aircraft'].to_dict()
    iw39ord_df.loc[iw39ord_df['Program'].isna(), 'Program'] = iw39ord_df['Material'].map(aircraft_map)
    iw39ord_df['Program'].replace({'C-17': 'C17', 'AH-64': 'AH-64 APACHE'}, inplace=True)

    iw39ord_df.to_excel(download_path + '\IW39_Orders.xlsx', index=False)


# function to process IW39 operations
def _process_iw39_operations(mat_master):
    iw39op_df = pd.read_excel(download_path + '\IW39_Operations.xlsx')
    iw39op_df.to_excel(download_path + '\IW39_Operations_REWORK_SHIPSET.xlsx', index=False)
    iw39op_df = iw39op_df[~iw39op_df['System Status'].str.contains('DLT')]
    iw39op_df = iw39op_df[~iw39op_df['Control key'].str.contains('ZNPT')]
    iw39op_df['Program'] = iw39op_df['Material'].map(mat_master[['Incoming Material', 'Aircraft']].set_index('Incoming Material').to_dict()['Aircraft'])
    iw39op_df['Program'].replace({'C-17': 'C17', 'AH-64': 'AH-64 APACHE'}, inplace=True)
    iw39op_df.to_excel(download_path + '\IW39_Operations.xlsx', index=False)

# preprocess_iw39_orders  FUNCTIONS --- END
#############################################


###################################################
# IW39_orders_ops_processing  FUNCTIONS --- START

# Function to load the necessary Excel files into DataFrames.
def _load_excel_files():
    IW39OP_DF = pd.read_excel(download_path + '\IW39_Operations.xlsx')
    IW39ORD_DF = pd.read_excel(download_path + '\IW39_Orders.xlsx')
    return IW39OP_DF, IW39ORD_DF


# Function to process the operations DataFrame: create 'Op Status' and factorize 'Order'.
def _process_operations(IW39OP_DF):
    IW39OP_DF['Op Status'] = IW39OP_DF['Act.finish date'].apply(
        lambda x: "COMPLETED" if (not (pd.isnull(x))) else "OPEN")
    return IW39OP_DF


# Factorize 'Order' in the operations DataFrame and update the 'Op Status' accordingly.
def _factorize_and_complete_status(IW39OP_DF):
    logger.info("pd.factorize(IW39OP_DF['Order'])")
    SCM_FINAL = pd.DataFrame()
    for j in list(pd.factorize(IW39OP_DF['Order'])[1]):
        SCM_1 = IW39OP_DF[IW39OP_DF['Order'] == j]
        if True in list(set(SCM_1['Op Status'].str.contains('COMPLETED'))):
            start_index = SCM_1.index.to_list()[0]
            end_index = SCM_1[SCM_1["Op Status"] == "COMPLETED"].index.to_list()[-1]
            if (start_index != end_index):
                SCM_1.loc[start_index:end_index, 'Op Status'] = "COMPLETED"
        SCM_FINAL = SCM_FINAL.append(SCM_1, ignore_index=True)

    SCM_FINAL['Op Status'].replace({'': 'OPEN'}, inplace=True)
    SCM_FINAL.drop(['Sort field'], axis=1, inplace=True)
    return SCM_FINAL


#Function to process the orders DataFrame: apply necessary transformations and join with operations DataFrame.
def _process_orders(IW39ORD_DF, SCM_FINAL):
    IW39ORD_DF = IW39ORD_DF[['Leading order', 'Order', 'Order Type', 'Serial Number', 'Profit Center', 'Actual release', 'Sales Document', 'Sort field', 'PO Number']]
    IW39ORD_DF['XSUB_Count'] = IW39ORD_DF.apply(_get_XSUB_COUNT, args=(IW39ORD_DF,), axis=1)
    IW39ORD_DF = IW39ORD_DF.join(SCM_FINAL.set_index('Order'), on='Order', how='left')
    IW39ORD_DF['Time_Stamp'] = datetime.now().strftime('%m/%d/%Y')

    logger.info("pd.factorize(IW39ORD_DF['Leading order'])")
    XSUB_ORD = pd.DataFrame()
    for k in list(pd.factorize(IW39ORD_DF['Leading order'])[1]):
        temp_df = IW39ORD_DF[(IW39ORD_DF['Leading order'] == k) & (IW39ORD_DF['Order Type'] == 'XSUB') & (IW39ORD_DF['Op Status'] == 'COMPLETED')]
        if (len(temp_df) > 0):
            temp_df = temp_df[(~temp_df['Act.finish date'].isnull())]
            oVal = temp_df[temp_df['Act.finish date'] == max(temp_df['Act.finish date'])]['Order'].values
            if len(oVal) > 0:
                temp_df['Last scan XSUB'] = np.amax(oVal)
            else:
                temp_df['Last scan XSUB'] = ''
            temp_df = temp_df[['Leading order', 'Last scan XSUB']].head(1)
            XSUB_ORD = XSUB_ORD.append(temp_df, ignore_index=True)

    logger.info("clean IW39ORD_DF ")
    IW39ORD_DF['Last scan XSUB'] = IW39ORD_DF['Leading order'].map(XSUB_ORD[['Leading order', 'Last scan XSUB']].set_index('Leading order').to_dict()['Last scan XSUB'])
    IW39ORD_DF.rename(columns={'Sales Document': 'Sales Order'}, inplace=True)
    IW39ORD_DF['Material'] = IW39ORD_DF['Material'].astype(str)
    IW39ORD_DF['Serial Number'] = IW39ORD_DF['Serial Number'].astype(str)
    IW39ORD_DF['Work center'] = IW39ORD_DF['Work center'].astype(str)
    IW39ORD_DF['Opr. short text'] = IW39ORD_DF['Opr. short text'].astype(str)
    return IW39ORD_DF


# function to Process sales data and join with the orders DataFrame.
def _process_and_join_sales_data(IW39ORD_DF):
    logger.info("read SHIPSET_DATA.xlsx ")
    SHISET_DATA = pd.read_excel(os.path.join(SITE_INPUTS, 'SHIPSET_DATA.xlsx'))
    SHISET_DATA.rename(columns={'New': 'Sales Order'}, inplace=True)
    SHISET_DATA['SHIPSET'] = SHISET_DATA['SHIPSET'].astype(str)
    SHISET_DATA = SHISET_DATA[['Sales Order', 'SHIPSET']]

    logger.info("read ZMRO_SALES.xlsx ")
    ZMRO_SALES = pd.read_excel(os.path.join(download_path, "ZMRO_SALES.xlsx"), usecols=['Sales Order', 'Aircraft Serial Number'])
    ZMRO_SALES.dropna(subset=['Aircraft Serial Number'], inplace=True)
    ZMRO_SALES = ZMRO_SALES.join(SHISET_DATA.set_index('Sales Order'), on='Sales Order', how='left')
    ZMRO_SALES['Aircraft Serial Number'] = ZMRO_SALES.apply(lambda x: x['SHIPSET'] if (not (pd.isnull(x['SHIPSET']))) else x['Aircraft Serial Number'], axis=1)
    ZMRO_SALES.drop('SHIPSET', axis=1, inplace=True)
    logger.info("join IW39ORD_DF, SHIPSET_DATA, ZMRO_SALES.xlsx ")
    IW39ORD_DF = IW39ORD_DF.join(ZMRO_SALES.set_index('Sales Order'), on='Sales Order', how='left')
    return IW39ORD_DF


# Function to lookup 'IW39ORD_DF_Copy' to fill the gaps w.r.t 'Order' column
def _fill_gags_using_orders_data(IW39ORD_DF, IW39ORD_DF_Copy):

    # get list of rows where Material, Description, Serial Number is blank, lookup 'IW39ORD_DF_Copy' to fill the gaps w.r.t 'Order' column
    # Define columns to be filled
    fill_columns = ['Material', 'Description', 'Serial Number']

    # Create a copy of the original DataFrame
    IW39ORD_DF_filled = IW39ORD_DF.copy()
    IW39ORD_DF_Copy = IW39ORD_DF_Copy[['Order', 'Material', 'Description', 'Serial Number']]
    # Replace 'nan' and '' with actual NaNs
    IW39ORD_DF_filled[fill_columns] = IW39ORD_DF_filled[fill_columns].replace({'nan': np.nan, '': np.nan})

    # Merge the two DataFrames on 'Order' column, this adds _x and _y suffixes for overlapping column names
    merged = IW39ORD_DF_filled.merge(IW39ORD_DF_Copy, on='Order', how='left', suffixes=('', '_copy'))

    # Loop over each column to be filled
    for col in fill_columns:
        # Fill NaN values in the original column with values from the copy
        merged[col] = merged[col].fillna(merged[col + '_copy'])

    # Drop the unnecessary columns from the copy
    merged.drop(columns=[col + '_copy' for col in fill_columns], inplace=True)

    # Replace original DataFrame with the filled DataFrame
    IW39ORD_DF = merged

    return IW39ORD_DF


def _process_unique_orders_data(IW39ORD_DF):
    logger.info('pushed IW39 orders & operations to database.')
    IW39ORD_DF_New = IW39ORD_DF[['Order', 'Sales Order', 'Material', 'Description', 'Program', 'Op Status']]
    IW39ORD_DF_New = IW39ORD_DF_New.dropna(subset=['Op Status'])
    IW39ORD_DF_New.fillna("", inplace=True)
    IW39ORD_DF_New1 = IW39ORD_DF_New.groupby(
        ['Order', 'Sales Order', 'Material', 'Description', 'Program', 'Op Status']).first().reset_index()
    IW39ORD_DF_New1['OprStatus'] = 0
    IW39ORD_DF_New1.loc[IW39ORD_DF_New1['Op Status'] == 'COMPLETED', 'OprStatus'] = 1
    group_by_columns = ['Order', 'Sales Order', 'Material', 'Description', 'Program']
    reindex_columns =['Order', 'Sales Order', 'Material', 'Description', 'Program', 'OprStatus']
    IW39ORD_DF_New2 = (IW39ORD_DF_New1.groupby(group_by_columns, as_index=False).OprStatus.min().reindex(columns=reindex_columns))
    IW39ORD_DF_New2.loc[IW39ORD_DF_New2['OprStatus'] == 0, 'OprStatus'] = 'OPEN'
    IW39ORD_DF_New2.loc[IW39ORD_DF_New2['OprStatus'] == 1, 'OprStatus'] = 'COMPLETED'
    IW39ORD_DF_New2.rename(columns={'OprStatus': 'Op Status'}, inplace=True)
    return IW39ORD_DF_New2



def _get_XSUB_COUNT(x, IW39ORD_DF):
    if (not (pd.isnull(x['Leading order']))):
        return len(
            IW39ORD_DF[(IW39ORD_DF['Leading order'] == x['Leading order']) & (IW39ORD_DF['Order Type'] == 'XSUB')])
    else:
        return 0

# IW39_orders_ops_processing  FUNCTIONS --- END
###################################################


###################################################
# FPY_Data_Processing  FUNCTIONS --- START

# Function to read excel data
def _read_excel_files():
    logger.info("Read IW39_Operations, WC_Definitions, Routing_Database")
    df = pd.read_excel(download_path + '\IW39_Operations.xlsx')
    wc_df = pd.read_excel(r"WC_Definitions.xlsx")
    routing_df = pd.read_excel(r"Routing_Database.xlsx")
    return df, wc_df, routing_df


# Function to clean data
def _clean_data(df, wc_df, routing_df):
    logger.info("Clean FPY Dashboard Data")
    routing_df['PN_OP'] = routing_df.apply(lambda x: str(x['Part Number']) + "-" + str(x['Operation']).zfill(4), axis=1)
    data = df[df['System Status'].isin(['CNF  REL', 'PCNF REL'])]
    data['PN_OP'] = data.apply(lambda x: str(x['Material']) + "-" + str(x['Activity']).zfill(4), axis=1)
    data['WC_Routing'] = data['PN_OP'].map(routing_df[['PN_OP', 'WC']].set_index('PN_OP').to_dict()['WC'])
    data['dept'] = data['WC_Routing'].map(wc_df[['WC Number', 'Group']].set_index('WC Number').to_dict()['Group'])
    data.dropna(subset=["dept", "Act.finish date"], inplace=True)
    data['dept'] = data.apply(lambda x: dept_dict[x["dept"]] if x["dept"] in dept_dict else x["dept"], axis=1)
    data.rename(columns={'Act.finish date': 'Date', 'Activity': 'OP#'}, inplace=True)
    data["FT"] = "IW39"
    data = data[['Order', 'Material', 'OP#', 'Date', 'Program', 'dept', 'FT']]
    return data


# Function to read SQL data
def _read_sql_data():
    logger.info("Read sql")
    RetryDownloadCnt = 0
    DoSQL = True
    while DoSQL:
        engine.connect()
        cnxn_exec = engine.connect()
        try:
            DF2 = pd.read_sql(
                "SELECT [Date],[Work order #],[Part Number],[Program Type],[Detecting process],[Suspected Origin],"
                "[Defect category],[Defect type],[Defect description],[Serial Number],[Sales Order],[OP#] FROM ["
                "MG_Digital].[dbo].[LG_DEFECT_HISTORY]", cnxn_exec)
            DoSQL = False
        except Exception as e:
            logger.info(datetime.now(), 1, str(e))
            time.sleep(60)
            RetryDownloadCnt += 1
            if RetryDownloadCnt >= 10:
                raise ValueError(str(e))
            time.sleep(10)
    DF2.rename(columns={'Part Number': 'Material', 'Detecting process': 'dept'}, inplace=True)
    DF2["FT"] = "DFTH"
    return DF2

# Function to merge data
def _merge_dataframes(data, DF2):
    logger.info("Merge First Pass Yield Parts")

    # merge data and add TimeStamp
    merge_data = pd.concat([data, DF2], axis=0)
    merge_data["TimeStamp"] = datetime.now().strftime('%m/%d/%Y')

    logger.info("read First Pass Yield Parts")
    FPY_PARTS = pd.read_excel(SITE_INPUTS + '\First Pass Yield Parts.xlsx', usecols=['Material', 'FPY Track'])

    # create a dictionary for mapping
    material_track_map = FPY_PARTS.set_index('Material')['FPY Track'].to_dict()

    # merge and map in one operation
    merge_data["INC/EXC"] = merge_data['Material'].map(material_track_map).fillna('Exclude')

    # use np.where for conditional replacement of 'dept' values
    merge_data['dept'] = np.where(pd.isnull(merge_data['Suspected Origin']), merge_data['dept'],
                                  merge_data['Suspected Origin'].map(suspected_orgin_dict).fillna("NA"))

    # convert 'Order' column into string type
    merge_data['Order'] = merge_data['Order'].astype(str)

    # handle 'OP#' column
    merge_data['OP#'] = merge_data['OP#'].apply(
        lambda x: str(int(float(x))) if pd.notnull(x) and x != '' else "")

    merge_data.to_excel(r"final.xlsx", index=False)

    return merge_data


# FPY_Data_Processing  FUNCTIONS --- END
###################################################


#############################################
# process_rework_hours  FUNCTIONS --- START

# Function to Read all excel files and return dataframes
def _get_rework_source_data():
    ZMRO_SALES = pd.read_excel(os.path.join(download_path, 'ZMRO_SALES.XLSX'), usecols=['Aircraft Type', 'Profit Center', 'Customer Name', 'Sales Order'])
    WC_DEF = pd.read_excel(os.path.join(SITE_INPUTS, 'WC Definition v3.xlsx'), usecols=['WC Number', 'Group'])
    IW39_ORDERS = pd.read_excel(os.path.join(download_path, 'IW39_Orders_REWORK_SHIPSET.xlsx'), usecols=['Order', 'Order Type', 'Sales Document', 'Created on'])
    IW39_OPERATIONS = pd.read_excel(os.path.join(download_path, 'IW39_Operations_REWORK_SHIPSET.xlsx'), usecols=['Order', 'Material', 'Description', 'Activity', 'Work center', 'Opr. short text', 'Actual work', 'Actual start', 'Act.finish date'])
    IW47_DATA = pd.read_excel(os.path.join(download_path, 'IW47.xlsx'), usecols=['Order', 'Activity', 'Actual work'])
    REWORK_VENDOR = pd.read_excel(os.path.join(download_path, 'REWORK_VENDOR.xlsx'))
    return ZMRO_SALES, WC_DEF, IW39_ORDERS, IW39_OPERATIONS, IW47_DATA, REWORK_VENDOR


# Function to Process dataframes and return the modified dataframe
def _process_operations_data(ZMRO_SALES, WC_DEF, IW39_ORDERS, IW39_OPERATIONS, IW47_DATA):
    WC_DEF.dropna(subset=['WC Number'], inplace=True)
    IW39_ORDERS = IW39_ORDERS[IW39_ORDERS['Order Type'] == 'XSUB']
    IW39_OPERATIONS = IW39_OPERATIONS[IW39_OPERATIONS['Order'].isin(list(set(IW39_ORDERS['Order'])))]
    IW39_OPERATIONS['Sales Order'] = IW39_OPERATIONS['Order'].map(IW39_ORDERS[['Order', 'Sales Document']].set_index('Order').to_dict()['Sales Document'])
    IW39_OPERATIONS['Created On'] = IW39_OPERATIONS['Order'].map(IW39_ORDERS[['Order', 'Created on']].set_index('Order').to_dict()['Created on'])
    IW39_OPERATIONS['Program'] = IW39_OPERATIONS['Sales Order'].map(ZMRO_SALES[['Sales Order', 'Aircraft Type']].set_index('Sales Order').to_dict()['Aircraft Type'])
    IW39_OPERATIONS['Profit Center'] = IW39_OPERATIONS['Sales Order'].map(ZMRO_SALES[['Sales Order', 'Profit Center']].set_index('Sales Order').to_dict()['Profit Center'])
    IW39_OPERATIONS['Customer'] = IW39_OPERATIONS['Sales Order'].map(ZMRO_SALES[['Sales Order', 'Customer Name']].set_index('Sales Order').to_dict()['Customer Name'])
    IW39_OPERATIONS['Department'] = IW39_OPERATIONS['Work center'].map(WC_DEF[['WC Number', 'Group']].set_index('WC Number').to_dict()['Group'])
    IW39_OPERATIONS['OrderACTKey'] = IW39_OPERATIONS['Order'].astype(str) + "-" + IW39_OPERATIONS['Activity'].astype(str)
    try:
        IW47_DATA = pd.DataFrame(IW47_DATA.groupby(['Order', 'Activity'])['Actual work'].agg(sum).reset_index())
        IW47_DATA['OrderACTKey'] = IW47_DATA['Order'].astype(str) + "-" + IW47_DATA['Activity'].astype(str)
        IW39_OPERATIONS['Actual Work(IW47)'] = IW39_OPERATIONS['OrderACTKey'].map(IW47_DATA[['OrderACTKey', 'Actual work']].set_index('OrderACTKey').to_dict()['Actual work'])
        IW39_OPERATIONS['Actual Work(IW47)'].fillna(0, inplace=True)
    except:
        IW39_OPERATIONS['Actual Work(IW47)'] = 0

    return IW39_OPERATIONS


# Function to Get data from SQL and return a dataframe
def _get_rework_data_from_sql():
    logger.info("read sql")
    RetryDownloadCnt = 0
    DoSQL = True
    while DoSQL:
        cnxn = pyodbc.connect(driver='{SQL Server}', server='GUSALD2r.utcapp.com', database='MG_Digital',
                              uid='MG_DigitalRW', pwd='Falconine21!')
        try:
            IW39_DB = pd.read_sql('SELECT * FROM [MG_Digital].[dbo].[LG_MIAMI_REWORK_HOURS]', cnxn)
            DoSQL = False
        except Exception as e:
            time.sleep(60)
            logger.info(1, str(e))
            RetryDownloadCnt += 1
            if RetryDownloadCnt >= 10:
                raise ValueError(str(e))
            time.sleep(10)

    return IW39_DB


# Function to Process the dataframe with SQL data and return the modified dataframe
def _process_operations_data_with_sql(IW39_OPERATIONS, IW39_DB):
    logger.info("list operations")
    ORDAct_LIST = IW39_OPERATIONS['OrderACTKey'].to_list()
    IW39_DB = IW39_DB[~IW39_DB['OrderACTKey'].isin(ORDAct_LIST)]
    IW39_OPERATIONS = IW39_OPERATIONS.append(IW39_DB)
    IW39_OPERATIONS['Time_Stamp'] = datetime.now().strftime('%m/%d/%Y')

    Ignored_Description_List = ["Aluminum", "Axle Sleeve", "Bearing Sleeve", "Bush", "Forcemate Bushings",
                                "Gasket", "Inserts", "Lube Fittings", "O/S Gland Nut", "O/S Bushings", "Plug",
                                "Post Special Bushing", "Post Standard Bushing", "R/Sleeve", "Rep.Sleeve",
                                "Repair Bushings", "Repair Plug", "Repair Sleeve", "RS", "Rub Strip", "Shim",
                                "Sleeve", "Spacers", "Stainless – RS", "STD size bushing", "Washer"]

    IW39_OPERATIONS['flagCol'] = np.where(
        IW39_OPERATIONS['Description'].str.lower().str.contains(('|'.join(Ignored_Description_List)).lower()), 1, 0)
    IW39_OPERATIONS = IW39_OPERATIONS[IW39_OPERATIONS['flagCol'] == 0]
    IW39_OPERATIONS.drop('flagCol', axis=1, inplace=True)

    return IW39_OPERATIONS


# Function to Push operations data to SQL
def _push_operations_data_to_sql(IW39_OPERATIONS):
    logger.info("Pushing data to db: ")
    RetryDownloadCnt = 0
    RetryDownload = True
    while RetryDownload:
        try:
            IW39_OPERATIONS.to_sql(name='LG_MIAMI_REWORK_HOURS', schema='dbo', index=False, con=engine,
                                   if_exists='replace', chunksize=110, method='multi')
            IW39_OPERATIONS.to_excel(download_path + '\LG_MIAMI_REWORK_HOURS.xlsx', index=False)
            RetryDownload = False
        except Exception as e:
            logger.info('RetryDownloadCnt: ', RetryDownloadCnt)
            logger.info(str(e))
            RetryDownloadCnt += 1
            if RetryDownloadCnt >= 30:
                raise ValueError(str(e))
            time.sleep(10)
    logger.info("pushed data to db IW39_OPERATIONS: ")


# Task 6: Push rework vendor hours data to SQL
def _push_rework_vendor_hours_data_to_sql(REWORK_VENDOR):
    logger.info("read REWORK_VENDOR")
    RetryDownloadCnt = 0
    RetryDownload = True
    logger.info("push to db REWORK_VENDOR")
    while RetryDownload:
        try:
            REWORK_VENDOR.to_sql(name='LG_MIAMI_REWORK_VENDOR_HOURS', schema='dbo', index=False, con=engine,
                                 if_exists='replace', chunksize=110, method='multi')
            REWORK_VENDOR.to_excel(download_path + '\LG_MIAMI_REWORK_VENDOR_HOURS.xlsx', index=False)
            RetryDownload = False
        except Exception as e:
            logger.info('RetryDownloadCnt: ', RetryDownloadCnt)
            logger.info(str(e))
            RetryDownloadCnt += 1
            if RetryDownloadCnt >= 30:
                raise ValueError(str(e))
            time.sleep(10)
    logger.info("pushed to db REWORK_VENDOR")



# process_rework_hours  FUNCTIONS --- END
#############################################


def read_and_process_files():
    # Reads several Excel files and performs data transformations
    print("Processing shipset data at:", timestamp())
    SHISET_DATA = read_file("SHIPSET_DATA.xlsx", path=SITE_INPUTS2)
    ZMRO_SALES = read_file("ZMRO_SALES.xlsx", path=SAP_PATH,
                           usecols=['Aircraft Type', 'Profit Center', 'Customer Name', 'Sales Order',
                                    'Aircraft Serial Number', 'WBS', 'Order TECO Date'])
    Lookup = read_file("CJI3 Output _Mia_Ryanair_Jan.xlsx", path=RAW_DATA_PUSH, sheet_name="lookup")
    CJI_3 = read_file("CJI3.xlsx", path=SAP_PATH)
    CC = read_file("Cost_Center.xlsx", path=SITE_INPUTS2)
    WC_GATE = read_file("WC Definition.xlsx", path=Shipset_Hours, sheet_name="Cost Center")

    return SHISET_DATA, ZMRO_SALES, Lookup, CJI_3, CC, WC_GATE


def read_file(filename, path=None, **kwargs):
    # Reads an Excel file from the provided path
    return pd.read_excel(os.path.join(path, filename), **kwargs)


def timestamp():
    # Returns a formatted timestamp
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def process_SHISET_DATA(SHISET_DATA):
    SHISET_DATA.rename(columns={'New': 'Sales Order'}, inplace=True)
    SHISET_DATA['SHIPSET'] = SHISET_DATA['SHIPSET'].astype(str)
    SHISET_DATA = SHISET_DATA[['Sales Order', 'SHIPSET']]
    return SHISET_DATA

def process_ZMRO_SALES(ZMRO_SALES, SHISET_DATA):
    ZMRO_SALES.dropna(subset=['Aircraft Serial Number'], inplace=True)
    ZMRO_SALES['Order Status'] = ZMRO_SALES.apply(lambda x: "CLOSED" if not (pd.isnull(x['Order TECO Date'])) else "OPEN", axis=1)
    ZMRO_SALES = ZMRO_SALES.join(SHISET_DATA.set_index('Sales Order'), on='Sales Order', how='left')
    ZMRO_SALES['Aircraft Serial Number'] = ZMRO_SALES.apply(lambda x: x['SHIPSET'] if (not (pd.isnull(x['SHIPSET']))) else x['Aircraft Serial Number'], axis=1)
    ZMRO_SALES.drop('SHIPSET', axis=1, inplace=True)
    return ZMRO_SALES

def process_CJI_3(CJI_3, Lookup):
    CJI_3["Type"] = CJI_3['Cost Element'].map(Lookup[['Cost Element', 'Type']].set_index(['Cost Element']).to_dict()['Type'])
    CJI_3 = CJI_3[CJI_3["Type"] == "Lab"]
    CJI_3.dropna(subset=["WBS element"], inplace=True)
    CJI_3.rename(columns={"WBS element": "WBS"}, inplace=True)
    CJI_3 = CJI_3[["Cost Element", "Cost element name", "WBS", "Total quantity", "Posting Date", "Partner-CCtr", "Object type", "Object", "CO object name", "Personnel Name", "Personnel Number", 'Material', 'Material Description']]
    return CJI_3

def join_ZMRO_SALES_CJI_3(ZMRO_SALES, CJI_3, CC, WC_GATE):
    ZMRO_SALES = ZMRO_SALES.join(CJI_3.set_index('WBS'), on='WBS', how='left')
    ZMRO_SALES['COST CENTER'] = ZMRO_SALES['Partner-CCtr'].map(CC[['COST CENTER', 'COST CENTER NAME']].set_index('COST CENTER').to_dict()['COST CENTER NAME'])
    ZMRO_SALES['Cost Element'] = ZMRO_SALES['Cost Element'].apply(lambda x: str(int(x)) if (not (pd.isnull(x)) and x != '') else "")
    ZMRO_SALES['GATE'] = ZMRO_SALES['Partner-CCtr'].map(WC_GATE[['Cost Cent', 'SUPPORT FUNCTION']].set_index(['Cost Cent']).to_dict()['SUPPORT FUNCTION'])
    ZMRO_SALES = ZMRO_SALES[['Aircraft Type', 'Profit Center', 'Customer Name', 'Sales Order', 'Aircraft Serial Number', 'Total quantity', 'Posting Date', 'COST CENTER', 'Order Status', 'GATE', 'Object type', 'Object', 'CO object name', 'Personnel Name', 'Personnel Number', 'Material', 'Material Description']]
    return ZMRO_SALES

def process_data(SHISET_DATA, ZMRO_SALES, Lookup, CJI_3, CC, WC_GATE):
    SHISET_DATA = process_SHISET_DATA(SHISET_DATA)
    ZMRO_SALES = process_ZMRO_SALES(ZMRO_SALES, SHISET_DATA)
    CJI_3 = process_CJI_3(CJI_3, Lookup)
    ZMRO_SALES = join_ZMRO_SALES_CJI_3(ZMRO_SALES, CJI_3, CC, WC_GATE)
    return SHISET_DATA, ZMRO_SALES, CJI_3


def try_sql_read(cnxn, retry_limit=10, retry_wait=60):
    # Attempts to retrieve data from a SQL database
    ReTryCount = 0
    while True:
        ReTryCount += 1
        try:
            print("Try to get data from SQL at:", timestamp())
            return pd.read_sql('SELECT * FROM [MG_Digital].[dbo].[LG_MIAMI_SHIPSET_HOURS]', cnxn)
        except Exception as e:
            print(str(e))
            if ReTryCount >= retry_limit:
                raise ValueError(str(e))
            time.sleep(retry_wait)


def try_sql_write(ZMRO_SALES, engine, retry_limit=10, retry_wait=60):
    # Attempts to write data to a SQL database
    RetryDownloadCnt = 0
    while Download_LG_MIAMI_SHIPSET_HOURS:
        try:
            engine.connect()
            ChunkLimit = int((2100 / len(ZMRO_SALES.columns)) * 0.9)
            ZMRO_SALES.to_sql(name='LG_MIAMI_SHIPSET_HOURS', schema='dbo', index=False, con=engine, if_exists='replace',
                              chunksize=ChunkLimit, method='multi')
            ZMRO_SALES.to_excel(SAP_PATH + '\LG_MIAMI_SHIPSET_HOURS.xlsx', index=False)
            return
        except Exception as e:
            time.sleep(60)
            print(str(e))
            RetryDownloadCnt += 1
            if RetryDownloadCnt >= retry_limit:
                raise ValueError(str(e))
            time.sleep(10)


if __name__ == "__main__":
    SHISET_DATA, ZMRO_SALES, Lookup, CJI_3, CC, WC_GATE = read_and_process_files()
    process_data(SHISET_DATA, ZMRO_SALES, Lookup, CJI_3, CC, WC_GATE)
    cnxn = pyodbc.connect("<your connection string>")
    ZMRO_SQL = try_sql_read(cnxn, retry_limit, retry_wait)
    # process ZMRO_SQL as in your code
    engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
    try_sql_write(ZMRO_SALES, engine, retry_limit, retry_wait)
    print("Completed shipset script at:", timestamp())