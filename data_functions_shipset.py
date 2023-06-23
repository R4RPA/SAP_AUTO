import os
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import generic_functions
from logger_master import setup_logger


class ShipsetConstants:
    def __init__(self):
        load_dotenv()

        self.SAP_PATH2 = os.getenv('SAP_PATH2')
        self.SITE_INPUTS2 = os.getenv('SITE_INPUTS2')
        self.RAW_DATA_PUSH = os.getenv('RAW_DATA_PUSH')
        self.Shipset_Hours = os.getenv('Shipset_Hours')
        self.engine = ''
        self.zmro_readcols = ['Aircraft Type', 'Profit Center', 'Customer Name', 'Sales Order', 'Aircraft Serial Number', 'WBS', 'Order TECO Date']
        self.cji3_columns = ["Cost Element", "Cost element name", "WBS", "Total quantity", "Posting Date", "Partner-CCtr", "Object type", "Object", "CO object name", "Personnel Name", "Personnel Number", 'Material', 'Material Description']
        self.zmro_columns = ['Aircraft Type', 'Profit Center', 'Customer Name', 'Sales Order', 'Aircraft Serial Number', 'Total quantity', 'Posting Date', 'COST CENTER', 'Order Status', 'GATE', 'Object type', 'Object', 'CO object name', 'Personnel Name', 'Personnel Number', 'Material', 'Material Description']
        self.logger = setup_logger('data_functions_iw39')


class PreProcessIW39Orders(ShipsetConstants):
    def main(self):
        self.logger.info("Processing unconfirmed orders")
        self._read_excel_data()
        self._process_and_join_sales_data()
        self._process_cji3_data()
        self._join_cji3_process_zmro_sales()
        self._get_shispet_data_from_sql()
        self._fill_missing_order_in_zmro_sales()
        self._push_shipset_data_to_sql()
        self.logger.info("Processing IW39_Orders End")

    def _read_excel_data(self):
        self.SHISET_DATA = pd.read_excel(os.path.join(self.SITE_INPUTS2, "SHIPSET_DATA.xlsx"))
        self.ZMRO_SALES = pd.read_excel(os.path.join(self.SAP_PATH, "ZMRO_SALES.xlsx"), usecols=self.zmro_readcols)
        self.Lookup = pd.read_excel(os.path.join(self.RAW_DATA_PUSH, "CJI3 Output _Mia_Ryanair_Jan.xlsx"), sheet_name="lookup")
        self.CJI_3 = pd.read_excel(os.path.join(self.SAP_PATH, "CJI3.xlsx"))
        self.CC = pd.read_excel(os.path.join(self.SITE_INPUTS2, "Cost_Center.xlsx"))
        self.WC_GATE = pd.read_excel(os.path.join(self.Shipset_Hours, "WC Definition.xlsx"), sheet_name="Cost Center")

    def _process_and_join_sales_data(self):
        self.logger.info("process SHIPSET_DATA")
        self.SHISET_DATA.rename(columns={'New': 'Sales Order'}, inplace=True)
        self.SHISET_DATA['SHIPSET'] = self.SHISET_DATA['SHIPSET'].astype(str)
        self.SHISET_DATA = self.SHISET_DATA[['Sales Order', 'SHIPSET']]

        self.logger.info("porcess and join ZMRO_SALES and SHISET_DATA")
        self.ZMRO_SALES.dropna(subset=['Aircraft Serial Number'], inplace=True)
        self.ZMRO_SALES['Order Status'] = self.ZMRO_SALES.apply(lambda x: "CLOSED" if not (pd.isnull(x['Order TECO Date'])) else "OPEN", axis=1)
        self.ZMRO_SALES = self.ZMRO_SALES.join(self.SHISET_DATA.set_index('Sales Order'), on='Sales Order', how='left')
        self.ZMRO_SALES['Aircraft Serial Number'] = self.ZMRO_SALES.apply(lambda x: x['SHIPSET'] if (not (pd.isnull(x['SHIPSET']))) else x['Aircraft Serial Number'], axis=1)
        self.ZMRO_SALES.drop('SHIPSET', axis=1, inplace=True)

    def _process_cji3_data(self):
        self.logger.info("process CJI_3 - map Cost Element usign Lookup sheet")
        self.CJI_3["Type"] = self.CJI_3['Cost Element'].map(self.Lookup[['Cost Element', 'Type']].set_index(['Cost Element']).to_dict()['Type'])
        self.CJI_3 = self.CJI_3[self.CJI_3["Type"] == "Lab"]
        self.CJI_3.dropna(subset=["WBS element"], inplace=True)
        self.CJI_3.rename(columns={"WBS element": "WBS"}, inplace=True)
        self.CJI_3 = self.CJI_3[self.cji3_columns]

    def _join_cji3_process_zmro_sales(self):
        self.logger.info("join ZMRO_SALES, CJI3 and and map Cost Center data using CC data")
        self.ZMRO_SALES = self.ZMRO_SALES.join(self.CJI_3.set_index('WBS'), on='WBS', how='left')
        self.ZMRO_SALES['COST CENTER'] = self.ZMRO_SALES['Partner-CCtr'].map(self.CC[['COST CENTER', 'COST CENTER NAME']].set_index('COST CENTER').to_dict()['COST CENTER NAME'])
        self.ZMRO_SALES['Cost Element'] = self.ZMRO_SALES['Cost Element'].apply(lambda x: str(int(x)) if (not (pd.isnull(x)) and x != '') else "")
        self.ZMRO_SALES['GATE'] = self.ZMRO_SALES['Partner-CCtr'].map(self.WC_GATE[['Cost Cent', 'SUPPORT FUNCTION']].set_index(['Cost Cent']).to_dict()['SUPPORT FUNCTION'])
        self.ZMRO_SALES = self.ZMRO_SALES[self.zmro_columns]

    def _get_shispet_data_from_sql(self):
        # This is to maintain historical data
        self.logger.info("get LG_MIAMI_SHIPSET_HOURS data from sql")
        cnxn = self.engine.connect()
        query = 'SELECT * FROM [MG_Digital].[dbo].[LG_MIAMI_SHIPSET_HOURS]'
        self.ZMRO_SQL = generic_functions.get_dataframe_using_sql(query, cnxn)

    def _fill_missing_order_in_zmro_sales(self):
        self.logger.info("fill missing sales orders in ZMRO SALES data using existing LG_MIAMI_SHIPSET_HOURS")
        ZMRO_SALES_LIST = list(set(self.ZMRO_SALES['Sales Order']))
        OLD_SO = self.ZMRO_SQL[~self.ZMRO_SQL['Sales Order'].isin(ZMRO_SALES_LIST)]
        self.ZMRO_SALES = self.ZMRO_SALES.append(OLD_SO)
        self.ZMRO_SALES['Time_Stamp'] = datetime.now().strftime('%m/%d/%Y')
        self.ZMRO_SALES.to_excel(os.path.join(self.SAP_PATH, "ZMRO_SALES_" + datetime.now().strftime("%m_%d_%Y") + "_.xlsx"))

    def _push_shipset_data_to_sql(self):
        self.logger.info("push LG_MIAMI_SHIPSET_HOURS to sql")
        download_path = os.path.join(self.SITE_INPUTS2, "G_MIAMI_SHIPSET_HOURS.xlsx")
        generic_functions.push_data_db(self.engine, self.ZMRO_SALES, 'LG_MIAMI_SHIPSET_HOURS', download_path, 'replace')

