#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 23 19:42:57 2019

@author: jasoncasey
"""
import keyring
import psycopg2
from io import BytesIO, StringIO
from zipfile import ZipFile
from urllib.request import urlopen
import pandas as pd


def fix_cols(dat):
    dat.columns = [colname.lower() for colname in list(dat.columns.values)]
    return(dat)

def fix_number(col):
    try:
        answer = pd.to_numeric(col.str.replace("[^0-9\.\-]", ""), errors = "coerce").fillna(0.0)
    except Exception as e:
        # print(str(e))
        answer = 0.0
   
    return(answer)

def make_proportion(col):
    answer = col / 100
    return(answer)

def get_filename(file_list):
    match = [s for s in file_list if "_rv" in s]
    answer = file_list[0]
    if len(match) > 0:
        answer = match[0]
    return(answer)

def net_load_info(url):
    with urlopen(url) as resp:
        zipfile = ZipFile(BytesIO(resp.read()))
        files = zipfile.namelist()
    return(files)

def net_load_data(url, types = "object"):
    with urlopen(url) as resp:
        zipfile = ZipFile(BytesIO(resp.read()))
        file_name = get_filename(zipfile.namelist())
        data_file = zipfile.open(file_name)
        answer = pd.read_csv(data_file,
                             low_memory = False,
                             dtype = "object",
                             encoding = "iso-8859-1")
    
    return(answer)

def insert_to_db(df, db, table):
    username = input("User ID: ")
    
    sio = StringIO()
    sio.write(df.to_csv(index=None, header=None))  # Write the Pandas DataFrame as a csv to the buffer
    sio.seek(0)  # Be sure to reset the position to the start of the stream
    
    try:
        con = psycopg2.connect(user = username,
                         password = keyring.get_password(db, username),
                         host = "127.0.0.1",
                         port = "5432",
                         database = db)
        # Copy the string buffer to the database, as if it were an actual file
        with con.cursor() as c:
            c.copy_from(sio, table, columns = df.columns, sep=',')
        con.commit()
    except (Exception, psycopg2.Error) as error :
        print ("Error while connecting to PostgreSQL", error)
    finally:
        #closing database connection.
        if(con):
            # cursor.close()
            con.close()



def read_from_db(db, query):
    username = input("User ID: ")
    # dat = sqlio.read_sql_query(sql, conn)
    
    try:
        con = psycopg2.connect(user = username,
                         password = keyring.get_password(db, username),
                         host = "127.0.0.1",
                         port = "5432",
                         database = db)
        # cursor = con.cursor()
        # cursor.execute(query)
        df = pd.read_sql(query, con)
    except (Exception, psycopg2.Error) as error :
        print ("Error while connecting to PostgreSQL", error)
    finally:
        #closing database connection.
        if(con):
            # cursor.close()
            con.close()
    
    return(df)
