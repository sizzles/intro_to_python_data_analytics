import streamlit as st
import pandas as pd
import altair as alt
from st_aggrid import AgGrid
import pandas as pd
import sqlite3
import datetime as dt
import numpy as np

st.set_page_config(layout="wide", page_title="BFI Dashboard")
DB_NAME = 'films.db'

def load_data():
    with sqlite3.connect(DB_NAME) as connection:
        #Get away with comparing dates as strings because of the format we have used
        df = pd.read_sql("select * from BFI_Validated where ReportDate > 2022-01-01 ORDER BY ReportDate ASC", con=connection)

    df['ReportDate'] = pd.to_datetime(df['ReportDate'])

    return df

def get_latest_data_refresh():
    with sqlite3.connect(DB_NAME) as connection: 
        df = pd.read_sql("select ModifiedTime from BFI_Validated ORDER BY ModifiedTime DESC LIMIT 1",  con=connection)
    
    return df

def load_summary_data():
    with sqlite3.connect(DB_NAME) as connection:
        #Get away with comparing dates as strings because of the format we have used
        df = pd.read_sql("select Rank, Film, WeekendGross, Distributor, WeeksReleased, NoCinemas, SiteAvg, TotalGross, ReportDate from BFI_Validated where ReportDate = (select max(ReportDate) from BFI_Validated) ORDER BY Rank ASC LIMIT 10", con=connection)

    df['ReportDate'] = pd.to_datetime(df['ReportDate'])

    return df

def build_dashboard():
    #Summary of latest report
    df = get_latest_data_refresh()
    latest_data_load = pd.to_datetime(df['ModifiedTime'].iloc[0])
    #print(latest_data_load)
    st.title("BFI Weekend Takings Dashboard")
    st.subheader(f"Latest Data Load @ {latest_data_load.strftime('%Y-%m-%d %H:%M:%S')}")
    st.subheader("Latest Report Summary")
    df = load_summary_data()
    AgGrid(df)

    col1, col2 = st.columns(2)
    #Weekend Gross Average
    with col1:
        df = load_data()
        dfg = df.groupby(['ReportDate']).mean()
        st.subheader(f"Average Weekend Gross: {(min(dfg.index)).strftime('%Y-%m-%d')} to {(max(dfg.index)).strftime('%Y-%m-%d')} ")
        st.line_chart(data=dfg['WeekendGross'] )
    
    with col2:
        #Weekend Gross By Film
        st.subheader('Weekend Gross Drill Down')
        unique_films = sorted(list(df['Film'].unique()))
        selected_film = st.selectbox("Select a film to see breakdown", unique_films, index=0, key=None, help=None, on_change=None, args=None, kwargs=None, disabled=False, label_visibility="visible")
        st.write('You selected: ', selected_film)
        df = df.set_index(df['ReportDate'])
        df = df[df['Film'] == selected_film]
        st.bar_chart(data=df['WeekendGross'] )
    
    st.write('Environment: UAT - Version 1.0.0')

if __name__ == '__main__':
    build_dashboard()