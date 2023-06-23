import os
import time
import pyodbc
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import numpy as np
import generic_functions
from logger_master import setup_logger


class IW39Constants:
    def __init__(self):
        load_dotenv()
        self.folder_path = os.getenv('folder_path')
        self.download_path = os.getenv('download_path')
        self.SITE_INPUTS = os.getenv('SITE_INPUTS')
        self.engine = ''

        self.logger = setup_logger('data_functions_iw39')

        self.dept_dict = {"Assembly": "Assembly", "Bush and Hone": "Bush & Hone", "Bushing": "Bush & Hone",
                          "Dissy": "Disassembly",
                          "Engineering Hold": "NA", "Kitting": "Stockroom / Kitting", "Quarantine": "NA",
                          "Grinding": "Machine Shop",
                          "Large Lathe": "NA", "Large Mills": "NA", "Medium Lathe": "NA", "Small Lathes": "NA",
                          "Small Mills": "NA",
                          "NDT": "Quality", "OSP": "Quality", "Paint": "Paint", "Alodine": "NA", "Bake": "NA",
                          "CAD Plating": "Plating", "Chrome Plating": "Plating", "Grit blast": "NA",
                          "Nickel Plating": "Plating",
                          "Passivate": "NA", "Shotpeen": "Plating", "Strip": "Plating", "Final Inspection": "Quality",
                          "Receiving Inspection": "Quality", "IPI": "Quality", "Plumbing": "Plumming",
                          "S&R": "Survey & Repair"}

        self.suspected_orgin_dict = {"Assembly": "Assembly", "Bush & Hone": "Bush & Hone", "Disassembly": "Disassembly",
                                     "Grind": "Machine Shop",
                                     "Machine Shop": "Machine Shop", "NDT": "Quality", "Paint": "Paint",
                                     "Cad Strip": "Plating", "Chrome Strip": "Plating",
                                     "Nickel Strip": "Plating", "Cad Plating": "Plating", "Chrome Plating": "Plating",
                                     "Nickle Plating": "Plating",
                                     "Plumming": "Plumming", "Stockroom / Kitting": "Stockroom / Kitting",
                                     "Survey & Repair": "Survey & Repair",
                                     "Wire Shop": "Machine Shop", "IPI": "Quality", "Vendor": "Vendor",
                                     "Shot Peen": "Plating", "Unknown": "NA"}


class PreProcessIW39Orders(IW39Constants):
    def __init__(self):
        super().__init__()
        self.mat_master = pd.read_excel(self.folder_path + '\Material Master Data.xlsx')
        self.mat_master = self.mat_master[self.mat_master['Planning'] == 'Yes']

    def main(self, Process_Orders, Process_Operations):
        self.logger.info("Processing unconfirmed orders")
        self.logger.info("Processing IW39_Orders and IW39_Operations files to delete all the DLT type orders")

        if Process_Orders:
            self._process_iw39_orders()
        else:
            self.logger.info("IW39_Orders_REWORK_SHIPSET Skip")

        if Process_Operations:
            self._process_iw39_operations()
        else:
            self.logger.info("IW39_Operations_REWORK_SHIPSET Skip")

        self.logger.info("Processing IW39_Orders End")

    def _process_iw39_orders(self):
        iw39ord_df = pd.read_excel(self.download_path + '\IW39_Orders.xlsx')
        zmro_ops_df = pd.read_excel(self.download_path + '\ZMRO_OPS.xlsx')

        iw39ord_df.to_excel(self.download_path + '\IW39_Orders_REWORK_SHIPSET.xlsx', index=False)

        program_map = zmro_ops_df[['Order', 'Aircraft']].set_index('Order').to_dict()['Aircraft']
        iw39ord_df['Program'] = iw39ord_df['Order'].map(program_map)

        aircraft_map = self.mat_master.set_index('Incoming Material')['Aircraft'].to_dict()
        iw39ord_df.loc[iw39ord_df['Program'].isna(), 'Program'] = iw39ord_df['Material'].map(aircraft_map)
        iw39ord_df['Program'].replace({'C-17': 'C17', 'AH-64': 'AH-64 APACHE'}, inplace=True)

        iw39ord_df.to_excel(self.download_path + '\IW39_Orders.xlsx', index=False)

    def _process_iw39_operations(self):
        iw39op_df = pd.read_excel(self.download_path + '\IW39_Operations.xlsx')
        iw39op_df.to_excel(self.download_path + '\IW39_Operations_REWORK_SHIPSET.xlsx', index=False)
        iw39op_df = iw39op_df[~iw39op_df['System Status'].str.contains('DLT')]
        iw39op_df = iw39op_df[~iw39op_df['Control key'].str.contains('ZNPT')]
        iw39op_df['Program'] = iw39op_df['Material'].map(self.mat_master[['Incoming Material', 'Aircraft']].set_index('Incoming Material').to_dict()['Aircraft'])
        iw39op_df['Program'].replace({'C-17': 'C17', 'AH-64': 'AH-64 APACHE'}, inplace=True)
        iw39op_df.to_excel(self.download_path + '\IW39_Operations.xlsx', index=False)


class IW39DataProcessor(IW39Constants):
    def main(self):
        self.logger.info("Start processing Orders...")

        # load necessary Excel files into DataFrames.
        IW39OP_DF, IW39ORD_DF = self._load_excel_files()

        # process the operations DataFrame: create 'Op Status' and factorize 'Order'.
        IW39OP_DF = self._process_operations(IW39OP_DF)

        # Factorize 'Order' in the operations DataFrame and update the 'Op Status' accordingly.
        SCM_FINAL = self._factorize_and_complete_status(IW39OP_DF)

        # preserve a copy before processing the orders DataFrame
        IW39ORD_DF_Copy = IW39ORD_DF.copy()

        # process the orders DataFrame: apply necessary transformations and join with operations DataFrame.
        IW39ORD_DF = self._process_orders(IW39ORD_DF, SCM_FINAL)

        # process sales data and join with the orders DataFrame.
        IW39ORD_DF = self._process_and_join_sales_data(IW39ORD_DF)

        # lookup 'IW39ORD_DF_Copy' to fill the gaps w.r.t 'Order' column
        IW39ORD_DF = self._fill_gaps_using_orders_data(IW39ORD_DF, IW39ORD_DF_Copy)

        # push data to database
        generic_functions.push_data_db(self.engine, IW39ORD_DF, 'LG_MIAMI_IW39_ORD', self.download_path, 'replace')

        # process unique orders data
        IW39ORD_DF_New = self._process_unique_orders_data(IW39ORD_DF)

        # push unique orders data to database
        generic_functions.push_data_db(self.engine, IW39ORD_DF_New, 'LG_MIAMI_IW39_ORD_UNQ', self.download_path, 'replace')

        self.logger.info("All tasks completed successfully!")

    def _load_excel_files(self):
        IW39OP_DF = pd.read_excel(self.download_path + '\IW39_Operations.xlsx')
        IW39ORD_DF = pd.read_excel(self.download_path + '\IW39_Orders.xlsx')
        return IW39OP_DF, IW39ORD_DF

    def _process_operations(self, IW39OP_DF):
        IW39OP_DF['Op Status'] = IW39OP_DF['Act.finish date'].apply(
            lambda x: "COMPLETED" if (not (pd.isnull(x))) else "OPEN")
        return IW39OP_DF

    def _factorize_and_complete_status(self, IW39OP_DF):
        self.logger.info("pd.factorize(IW39OP_DF['Order'])")
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

    def _process_orders(self, IW39ORD_DF, SCM_FINAL):
        IW39ORD_DF = IW39ORD_DF[
            ['Leading order', 'Order', 'Order Type', 'Serial Number', 'Profit Center', 'Actual release',
             'Sales Document', 'Sort field', 'PO Number']]
        IW39ORD_DF['XSUB_Count'] = IW39ORD_DF.apply(self._get_XSUB_COUNT, args=(IW39ORD_DF,), axis=1)
        IW39ORD_DF = IW39ORD_DF.join(SCM_FINAL.set_index('Order'), on='Order', how='left')
        IW39ORD_DF['Time_Stamp'] = datetime.now().strftime('%m/%d/%Y')

        self.logger.info("pd.factorize(IW39ORD_DF['Leading order'])")
        XSUB_ORD = pd.DataFrame()
        for k in list(pd.factorize(IW39ORD_DF['Leading order'])[1]):
            temp_df = IW39ORD_DF[(IW39ORD_DF['Leading order'] == k) & (IW39ORD_DF['Order Type'] == 'XSUB') & (
                        IW39ORD_DF['Op Status'] == 'COMPLETED')]
            if (len(temp_df) > 0):
                temp_df = temp_df[(~temp_df['Act.finish date'].isnull())]
                oVal = temp_df[temp_df['Act.finish date'] == max(temp_df['Act.finish date'])]['Order'].values
                if len(oVal) > 0:
                    temp_df['Last scan XSUB'] = np.amax(oVal)
                else:
                    temp_df['Last scan XSUB'] = ''
                temp_df = temp_df[['Leading order', 'Last scan XSUB']].head(1)
                XSUB_ORD = XSUB_ORD.append(temp_df, ignore_index=True)

        self.logger.info("clean IW39ORD_DF ")
        IW39ORD_DF['Last scan XSUB'] = IW39ORD_DF['Leading order'].map(
            XSUB_ORD[['Leading order', 'Last scan XSUB']].set_index('Leading order').to_dict()['Last scan XSUB'])
        IW39ORD_DF.rename(columns={'Sales Document': 'Sales Order'}, inplace=True)
        IW39ORD_DF['Material'] = IW39ORD_DF['Material'].astype(str)
        IW39ORD_DF['Serial Number'] = IW39ORD_DF['Serial Number'].astype(str)
        IW39ORD_DF['Work center'] = IW39ORD_DF['Work center'].astype(str)
        IW39ORD_DF['Opr. short text'] = IW39ORD_DF['Opr. short text'].astype(str)
        return IW39ORD_DF

    def _process_and_join_sales_data(self, IW39ORD_DF):
        self.logger.info("read SHIPSET_DATA.xlsx ")
        SHISET_DATA = pd.read_excel(os.path.join(self.SITE_INPUTS, 'SHIPSET_DATA.xlsx'))
        SHISET_DATA.rename(columns={'New': 'Sales Order'}, inplace=True)
        SHISET_DATA['SHIPSET'] = SHISET_DATA['SHIPSET'].astype(str)
        SHISET_DATA = SHISET_DATA[['Sales Order', 'SHIPSET']]

        self.logger.info("read ZMRO_SALES.xlsx ")
        ZMRO_SALES = pd.read_excel(os.path.join(self.download_path, "ZMRO_SALES.xlsx"),
                                   usecols=['Sales Order', 'Aircraft Serial Number'])
        ZMRO_SALES.dropna(subset=['Aircraft Serial Number'], inplace=True)
        ZMRO_SALES = ZMRO_SALES.join(SHISET_DATA.set_index('Sales Order'), on='Sales Order', how='left')
        ZMRO_SALES['Aircraft Serial Number'] = ZMRO_SALES['Aircraft Serial Number'].astype(int).astype(str)
        ZMRO_SALES['SHIPSET'] = ZMRO_SALES['SHIPSET'].fillna(ZMRO_SALES['Aircraft Serial Number'])
        ZMRO_SALES = ZMRO_SALES[['Sales Order', 'SHIPSET']]

        self.logger.info("join ZMRO_SALES with IW39ORD_DF")
        IW39ORD_DF = IW39ORD_DF.join(ZMRO_SALES.set_index('Sales Order'), on='Sales Order', how='left')
        return IW39ORD_DF

    def _fill_gaps_using_orders_data(self, IW39ORD_DF, IW39ORD_DF_Copy):
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

    def _get_XSUB_COUNT(self, row, IW39ORD_DF):
        self.logger.info("Calculating XSUB count...")
        if (not (pd.isnull(row['Leading order']))):
            return len(
                IW39ORD_DF[(IW39ORD_DF['Leading order'] == row['Leading order']) & (IW39ORD_DF['Order Type'] == 'XSUB')])
        else:
            return 0

    def _process_unique_orders_data(self, IW39ORD_DF):
        self.logger.info("Processing unique Orders data...")
        IW39ORD_DF = IW39ORD_DF.drop_duplicates(subset='Order', keep='first')
        return IW39ORD_DF


class FPYDataProcessor(IW39Constants):
    def main(self):
        self.logger.info("Processing FPY Dashboard Data - START")
        df, wc_df, routing_df = self._read_excel_files()
        data = self._clean_data(df, wc_df, routing_df)
        DF2 = self._read_sql_data()
        merge_data = self._merge_dataframes(data, DF2)
        generic_functions.push_data_db(self.engine, merge_data, 'LG_MIAMI_FPY', self.download_path, 'replace')
        self.logger.info("Processing FPY Dashboard Data Completed")

    def _read_excel_files(self):
        self.logger.info("Read IW39_Operations, WC_Definitions, Routing_Database")
        df = pd.read_excel(self.download_path + '\IW39_Operations.xlsx')
        wc_df = pd.read_excel(r"WC_Definitions.xlsx")
        routing_df = pd.read_excel(r"Routing_Database.xlsx")
        return df, wc_df, routing_df

    # Function to clean data
    def _clean_data(self, df, wc_df, routing_df):
        self.logger.info("Clean FPY Dashboard Data")
        routing_df['PN_OP'] = routing_df.apply(lambda x: str(x['Part Number']) + "-" + str(x['Operation']).zfill(4),
                                               axis=1)
        data = df[df['System Status'].isin(['CNF  REL', 'PCNF REL'])]
        data['PN_OP'] = data.apply(lambda x: str(x['Material']) + "-" + str(x['Activity']).zfill(4), axis=1)
        data['WC_Routing'] = data['PN_OP'].map(routing_df[['PN_OP', 'WC']].set_index('PN_OP').to_dict()['WC'])
        data['dept'] = data['WC_Routing'].map(wc_df[['WC Number', 'Group']].set_index('WC Number').to_dict()['Group'])
        data.dropna(subset=["dept", "Act.finish date"], inplace=True)
        data['dept'] = data.apply(lambda x: self.dept_dict[x["dept"]] if x["dept"] in self.dept_dict else x["dept"],
                                  axis=1)
        data.rename(columns={'Act.finish date': 'Date', 'Activity': 'OP#'}, inplace=True)
        data["FT"] = "IW39"
        data = data[['Order', 'Material', 'OP#', 'Date', 'Program', 'dept', 'FT']]
        return data

    # Function to read SQL data
    def _read_sql_data(self):
        self.logger.info("Read sql")
        RetryDownloadCnt = 0
        DoSQL = True
        while DoSQL:
            cnxn_exec = self.engine.connect()
            try:
                DF2 = pd.read_sql(
                    "SELECT [Date],[Work order #],[Part Number],[Program Type],[Detecting process],[Suspected Origin],"
                    "[Defect category],[Defect type],[Defect description],[Serial Number],[Sales Order],[OP#] FROM ["
                    "MG_Digital].[dbo].[LG_DEFECT_HISTORY]", cnxn_exec)
                DoSQL = False
            except Exception as e:
                self.logger.info(datetime.now(), 1, str(e))
                time.sleep(60)
                RetryDownloadCnt += 1
                if RetryDownloadCnt >= 10:
                    raise ValueError(str(e))
                time.sleep(10)
        DF2.rename(columns={'Part Number': 'Material', 'Detecting process': 'dept'}, inplace=True)
        DF2["FT"] = "DFTH"
        return DF2

    # Function to merge data
    def _merge_dataframes(self, data, DF2):
        self.logger.info("Merge First Pass Yield Parts")

        # merge data and add TimeStamp
        merge_data = pd.concat([data, DF2], axis=0)
        merge_data["TimeStamp"] = datetime.now().strftime('%m/%d/%Y')

        self.logger.info("read First Pass Yield Parts")
        FPY_PARTS = pd.read_excel(self.SITE_INPUTS + '\First Pass Yield Parts.xlsx', usecols=['Material', 'FPY Track'])

        # create a dictionary for mapping
        material_track_map = FPY_PARTS.set_index('Material')['FPY Track'].to_dict()

        # merge and map in one operation
        merge_data["INC/EXC"] = merge_data['Material'].map(material_track_map).fillna('Exclude')

        # use np.where for conditional replacement of 'dept' values
        merge_data['dept'] = np.where(pd.isnull(merge_data['Suspected Origin']), merge_data['dept'],
                                      merge_data['Suspected Origin'].map(self.suspected_orgin_dict).fillna("NA"))

        # convert 'Order' column into string type
        merge_data['Order'] = merge_data['Order'].astype(str)

        # handle 'OP#' column
        merge_data['OP#'] = merge_data['OP#'].apply(
            lambda x: str(int(float(x))) if pd.notnull(x) and x != '' else "")

        merge_data.to_excel(r"final.xlsx", index=False)

        return merge_data


class ReworkDataProcessor(IW39Constants):
    def main(self, process_rework_hours_data, process_rework_vendor_hours_data):
        self.logger.info('process_rework_hours - START')
        ZMRO_SALES, WC_DEF, IW39_ORDERS, IW39_OPERATIONS, IW47_DATA, REWORK_VENDOR = self._get_rework_source_data()

        if process_rework_hours_data:
            self.logger.info('process_rework_hours_data - START')
            IW39_OPERATIONS = self._process_operations_data(ZMRO_SALES, WC_DEF, IW39_ORDERS, IW39_OPERATIONS, IW47_DATA)
            IW39_DB = self._get_rework_data_from_sql()
            IW39_OPERATIONS = self._process_operations_data_with_sql(IW39_OPERATIONS, IW39_DB)
            self._push_operations_data_to_sql(IW39_OPERATIONS)
            self.logger.info('process_rework_hours_data - END')
        else:
            self.logger.info('process_rework_hours_data - SKIP')

        if process_rework_vendor_hours_data:
            self.logger.info('process_rework_vendor_hours_data - START')
            self._push_rework_vendor_hours_data_to_sql(REWORK_VENDOR)
            self.logger.info('process_rework_vendor_hours_data - END')
        else:
            self.logger.info('process_rework_vendor_hours_data - SKIP')

        self.logger.info('process_rework_hours - END')

    def _get_rework_source_data(self):
        ZMRO_SALES = pd.read_excel(os.path.join(self.download_path, 'ZMRO_SALES.XLSX'),
                                   usecols=['Aircraft Type', 'Profit Center', 'Customer Name', 'Sales Order'])
        WC_DEF = pd.read_excel(os.path.join(self.SITE_INPUTS, 'WC Definition v3.xlsx'), usecols=['WC Number', 'Group'])
        IW39_ORDERS = pd.read_excel(os.path.join(self.download_path, 'IW39_Orders_REWORK_SHIPSET.xlsx'),
                                    usecols=['Order', 'Order Type', 'Sales Document', 'Created on'])
        IW39_OPERATIONS = pd.read_excel(os.path.join(self.download_path, 'IW39_Operations_REWORK_SHIPSET.xlsx'),
                                        usecols=['Order', 'Material', 'Description', 'Activity', 'Work center',
                                                 'Opr. short text', 'Actual work', 'Actual start', 'Act.finish date'])
        IW47_DATA = pd.read_excel(os.path.join(self.download_path, 'IW47.xlsx'),
                                  usecols=['Order', 'Activity', 'Actual work'])
        REWORK_VENDOR = pd.read_excel(os.path.join(self.download_path, 'REWORK_VENDOR.xlsx'))
        return ZMRO_SALES, WC_DEF, IW39_ORDERS, IW39_OPERATIONS, IW47_DATA, REWORK_VENDOR

    def _process_operations_data(self, ZMRO_SALES, WC_DEF, IW39_ORDERS, IW39_OPERATIONS, IW47_DATA):
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

    def _get_rework_data_from_sql(self):
        self.logger.info("read sql")
        RetryDownloadCnt = 0
        DoSQL = True
        while DoSQL:
            cnxn = pyodbc.connect(driver='{SQL Server}', server='GUSALD2r.utcapp.com', database='MG_Digital', uid='MG_DigitalRW', pwd='Falconine21!')
            try:
                IW39_DB = pd.read_sql('SELECT * FROM [MG_Digital].[dbo].[LG_MIAMI_REWORK_HOURS]', cnxn)
                DoSQL = False
            except Exception as e:
                time.sleep(60)
                self.logger.info(1, str(e))
                RetryDownloadCnt += 1
                if RetryDownloadCnt >= 10:
                    raise ValueError(str(e))
                time.sleep(10)

        return IW39_DB

    def _process_operations_data_with_sql(self, IW39_OPERATIONS, IW39_DB):
        self.logger.info("list operations")
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

    def _push_operations_data_to_sql(self, IW39_OPERATIONS):
        self.logger.info("Pushing data to db: ")
        RetryDownloadCnt = 0
        RetryDownload = True
        while RetryDownload:
            try:
                IW39_OPERATIONS.to_sql(name='LG_MIAMI_REWORK_HOURS', schema='dbo', index=False, con=self.engine,
                                       if_exists='replace', chunksize=110, method='multi')
                IW39_OPERATIONS.to_excel(self.download_path + '\LG_MIAMI_REWORK_HOURS.xlsx', index=False)
                RetryDownload = False
            except Exception as e:
                self.logger.info('RetryDownloadCnt: ', RetryDownloadCnt)
                self.logger.info(str(e))
                RetryDownloadCnt += 1
                if RetryDownloadCnt >= 30:
                    raise ValueError(str(e))
                time.sleep(10)
        self.logger.info("pushed data to db IW39_OPERATIONS: ")

    def _push_rework_vendor_hours_data_to_sql(self, REWORK_VENDOR):
        self.logger.info("read REWORK_VENDOR")
        RetryDownloadCnt = 0
        RetryDownload = True
        self.logger.info("push to db REWORK_VENDOR")
        while RetryDownload:
            try:
                REWORK_VENDOR.to_sql(name='LG_MIAMI_REWORK_VENDOR_HOURS', schema='dbo', index=False, con=self.engine,
                                     if_exists='replace', chunksize=110, method='multi')
                REWORK_VENDOR.to_excel(self.download_path + '\LG_MIAMI_REWORK_VENDOR_HOURS.xlsx', index=False)
                RetryDownload = False
            except Exception as e:
                self.logger.info('RetryDownloadCnt: ', RetryDownloadCnt)
                self.logger.info(str(e))
                RetryDownloadCnt += 1
                if RetryDownloadCnt >= 30:
                    raise ValueError(str(e))
                time.sleep(10)
        self.logger.info("pushed to db REWORK_VENDOR")


