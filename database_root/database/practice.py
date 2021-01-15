import pymysql
from datetime import datetime
from pandas_datareader import wb
import pandas_datareader.data as web
import datetime
import requests
import json
import pandas as pd




db_conf = {
    "host": "127.0.0.1",
    "user": 'root',
    "password": "!wjddbsrms3@",
    "database": "stock_db"
}


def create_table(db_conf):
    con = pymysql.connect(**db_conf)
    cur = con.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS MARKET_CAP(
    number INT(20) AUTO_INCREMENT
    date VARCHAR(10),
    code VARCHAR(6),
    name VARCHAR(30),
    open float,
    high float,
    low float,
    close float,
    volume float,
    amount float,
    changes float,
    changes_ratio float,
    marcap_million float,
    stocks float,
    marcap_ratio float,
    foreign_shares float,
    foreign_ratio float,
    collected_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    primary key (number))
    """)

    con.commit()
    con.close()

    return 0

#

if __name__ == "__main__":
    create_table(db_conf)