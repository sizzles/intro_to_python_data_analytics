import pandas as pd
import sqlite3
import datetime as dt
import os
import platform
import numpy as np
import shutil
import logging
from tqdm import tqdm


os.getlogin()
platform.node()
DB_NAME = "films.db"

def configure_logging():
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S', filename='bfi_loader.log', encoding='utf-8', level=logging.DEBUG)

def get_raw_dir():
    return os.path.join("bfi_data", "raw")

def get_processed_dir():
    return os.path.join("bfi_data", "processed")

def get_error_dir():
    return os.path.join("bfi_data", "error")

def report_name_to_date(report_name):
    #bfi-weekend-box-office-report-2022-10-21-23.xls
    report_name = report_name.replace(".xls", "")
    report_name = report_name.replace("bfi-weekend-box-office-report-", "")
    #bfi-weekend-box-office-report-2022-10-21-23
    year = report_name[0:4]
    month = report_name[5:7]
    day = report_name[8:10]

    start_date = dt.datetime(year=int(year), month=int(month), day=int(day))
    report_date = start_date + dt.timedelta(days=2) #Covers the weekend so easiest to aqdd 2 days

    return report_date.date()

def load_report(report_name):
    #Read the file
    report_date = report_name_to_date(report_name)
    xls_path = os.path.join(get_raw_dir(),report_name)
    now = dt.datetime.utcnow()
    modified_by = f'{os.getlogin()} / {platform.node()}'

    df = pd.read_excel(xls_path)
    df.columns = df.iloc[0]
    df = df.drop(0, axis = 0)
    df = df.dropna(subset=['Film', 'Rank'])
    df = df.set_index('Film')
    df = df.dropna(how='all', axis=1)
    df = df.drop(columns=[np.nan])
    df.rename(columns = {"Film": "Film", "Rank": "Rank", "Country of Origin": "CountryOfOrigin", "Weekend Gross": "WeekendGross", "Distributor": "Distributor", "% change on last week" : "PercentWeekChange", "Weeks on release": "WeeksReleased", "Number of cinemas": "NoCinemas", "Site average": "SiteAvg", "Total Gross to date": "TotalGross" }, inplace=True)
    
    df['ReportName'] = report_name
    df['ReportDate'] = report_date
    df['ModifiedTime'] = str(now)
    df['ModifiedBy'] = modified_by

    #Example of some validation for moving to a silver tier
    df_validated = df
    df_validated['PercentWeekChange'] = df_validated['PercentWeekChange'].replace(to_replace='-', value=0)
    df_validated = df_validated.drop(labels=["CountryOfOrigin"],axis=1)
    df_validated = df_validated.drop(labels=["ReportName"],axis=1)

    with sqlite3.connect(DB_NAME) as connection:
        r = df.to_sql("BFI_Raw", connection, if_exists='append', chunksize=100)

        #Clear out as an upsert would not remove potentially stale entries
        cur = connection.cursor()
        cur.execute(f"DELETE FROM BFI_Validated WHERE ReportDate =?", (report_date,))
        
        connection.commit()

        r = df_validated.to_sql("BFI_Validated", connection, if_exists='append', chunksize=100)
    return r

def run():
    logging.info("-"*20)
    logging.info('Start BFI Loader job')
    i = 0

    for f in tqdm(os.listdir(get_raw_dir())):
        
        try:
            report_name = os.fsdecode(f)

            if not report_name.endswith(".xls"):
                continue

            logging.info(f'Loading Report: {report_name}')

            load_result = load_report(report_name)
            logging.info(f'Loaded {load_result} rows')
            shutil.move(os.path.join(get_raw_dir(), report_name), os.path.join(get_processed_dir(), report_name))
        
        except Exception as e:
            logging.error(f'Could not load report: {report_name}')
            logging.error(e)
            #try and move to the error folder
            try:
                shutil.move(os.path.join(get_raw_dir(), report_name), os.path.join(get_error_dir(), report_name))
                logging.error(f'Moved report {report_name} to error directory')
            except Exception as e:
                logging.error(f'Could not move report to error directory: {report_name}')
                logging.error(e)
        i += 1
    logging.info(f'Processed: {i} reports')
    logging.info('Finished BFI Loader job')
    logging.info("-"*20)
    
if __name__ == '__main__':
    configure_logging()
    run()