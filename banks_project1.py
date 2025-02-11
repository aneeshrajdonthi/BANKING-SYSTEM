import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime




url="https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs=["Name","MC_USD_Billion"]
csv_path="./Largest_banks_data.csv"
Database_name="Banks.db"
Table_name="Largest_banks"
csvfile_path="E:\python new\graded project\exchange_rate.csv"

# df=pd.DataFrame(columns=table_attribs)



def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''

        # text=requests.get(url)
        # data=BeautifulSoup(text.content,"html.parser")
        # tables=data.find_all('tbody')
        # rows=tables[0].find_all('tr')
        # for row in rows:
        #     col=row.find_all('td')
        #     if len(col)!=0:
        #         if col[1].find('a') is not None :
        #             name = col[1].a.contents[0] if col[1].a.contents else None
        #             mc_usd_billion = col[2].contents[0] if col[2].contents else None
        #             data_dict = {"Name": name,
        #                          "MC_USD_Billion": mc_usd_billion}
        #             df1=pd.DataFrame(data_dict,index=[0])
        #             df=pd.concat([df,df1],ignore_index=True)
      # Initialize an empty DataFrame
    df = pd.DataFrame(columns=table_attribs)
    text = requests.get(url)
    data = BeautifulSoup(text.content, "html.parser")
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col) >= 3:
            name = col[1].get_text().strip() if col[1].find('a') else None
            mc_usd_billion = col[2].get_text().strip()
            if name is not None and mc_usd_billion:
                data_dict = {"Name": name, "MC_USD_Billion": mc_usd_billion}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)
    # print(df)
    return df

def transform(df, csvfile_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    df3=pd.read_csv(csvfile_path)
    exchange_rate= df3.set_index('Currency').to_dict()['Rate']
    # print(exchange_rate)
    
    df['MC_USD_Billion'] = df['MC_USD_Billion'].astype(float)
    # for x in df['MC_USD_Billion']:
    #     print(type(x))
    #     print(x)
    #     print(exchange_rate['GBP'])
    #     print(x * exchange_rate['GBP'])
    #     print("---")
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    print(df)
    
    return df
def load_to_csv(df, csv_path):
    ''' This function saves the final data frame as a CSV file in
        the provided path. Function returns nothing.'''
    df.to_csv(csv_path,index=False)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name,con=sql_connection,if_exists='replace',index=False)

def run_query(query_statement,sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    ''' Here, you define the required entities and call the relevant
    functions in the correct order to complete the project. Note that this
    portion is not inside any function.'''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

def log_progress(message):
    now=datetime.now()
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    timestamp = now.strftime(timestamp_format)
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')





log_progress("Preliminaries complete. Initiating ETL process")

df=extract(url, table_attribs)
log_progress("Data extraction complete. Initiating Transformation process")

df=transform(df, csvfile_path)
log_progress("Data transformation complete. Initiating Loading process")

load_to_csv(df, csv_path)
log_progress("Data saved to CSV file")

sql_connection = sqlite3.connect('Banks.db')
log_progress("SQL Connection initiated")

load_to_db(df, sql_connection, Table_name)
log_progress("Data loaded to Database as a table, Executing queries")

query_statement="SELECT * FROM Largest_banks"
run_query(query_statement, sql_connection)
query_statement="SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(query_statement, sql_connection)
query_statement3="SELECT Name from Largest_banks LIMIT 5"
run_query(query_statement, sql_connection)

log_progress("Process Complete")

sql_connection.commit()
sql_connection.close()
log_progress("Server Connection closed")