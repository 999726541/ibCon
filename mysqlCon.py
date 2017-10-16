#!/bin/usr/env python3
# -*- coding:utf-8 -*-
__author__ = 'Leo Tao'
# ===============================================================================
# LIBRARIES
# ===============================================================================
import pandas as pd
from time import sleep
import mysql.connector
import datetime
# ===============================================================================
# ===============================================================================

"""
Connect with Our mysql database @ 192.168.1.177

"""

DB_HOST = '99.230.202.105'
USERNAME = 'leo'
PASSWORD = 'batman12'
PORT = 2222

class mysql_con():

    def __init__(self,DB_DB,DB_HOST=DB_HOST):
        # Establish Connection

        print('Connecting to DB...')
        self.cnx = mysql.connector.connect(user=USERNAME,
                                           password=PASSWORD,
                                           host=DB_HOST,
                                           database=DB_DB,
                                           port=PORT
                                           )
        self.cursor = self.cnx.cursor(buffered=True)
        self.curA = self.cnx.cursor(buffered=True)
        self.curB = self.cnx.cursor(buffered=True)

        print('Connecting successed')
        sleep(2)

    # ==================================================================================================================
    def csv2mysql(self, path_, tableName):
        """
        add CBOE option csv file into database
        path_: path of file
        """
        data = pd.read_csv(path_)
        print(datetime.datetime.now())
        data_ = data[:10000]
        iteration_num = 1
        print(path_)
        while True:
            data_['underlying_symbol'] = data_['underlying_symbol'].apply(lambda x: x.replace('^', ''))
            data_.rename(columns={'underlying_symbol': 'SYMBOL', 'active_underlying_price': 'UND_PRICE'}, inplace=True)
            data_.columns = map(str.upper, data_.columns)

            data_.loc[:, 'UPDATED_DATETIME'] = datetime.datetime.now().strftime('%Y-%m-%d %T')
            data_ = data_[['SYMBOL', 'QUOTE_DATETIME', 'BID', 'ASK', 'BID_SIZE', 'ASK_SIZE', 'EXPIRATION', 'ROOT',
                           'STRIKE', 'OPTION_TYPE', 'TRADE_VOLUME', 'DELTA', 'IMPLIED_VOLATILITY', 'GAMMA', 'THETA',
                           'VEGA', 'RHO', 'UND_PRICE', 'UPDATED_DATETIME']]

            dic = data_.T.to_dict().values()
            print(datetime.datetime.now())

            add_ = ("INSERT INTO `" + tableName + "`" +
                    " (QUOTE_DATETIME,SYMBOL, BID, BID_SIZE, ASK, ASK_SIZE, EXPIRATION, ROOT, STRIKE, OPTION_TYPE,TRADE_VOLUME, "
                    "DELTA, IMPLIED_VOLATILITY, GAMMA, THETA, VEGA, RHO, UND_PRICE, UPDATED_DATETIME) "
                    "VALUES (%(QUOTE_DATETIME)s, %(SYMBOL)s, %(BID)s, %(BID_SIZE)s, %(ASK)s, %(ASK_SIZE)s,"
                    " %(EXPIRATION)s, %(ROOT)s, %(STRIKE)s, %(OPTION_TYPE)s, %(TRADE_VOLUME)s, %(DELTA)s, %(IMPLIED_VOLATILITY)s,"
                    " %(GAMMA)s, %(THETA)s, %(VEGA)s, %(RHO)s, %(UND_PRICE)s,  %(UPDATED_DATETIME)s)"
                    " on DUPLICATE key update"
                    " QUOTE_DATETIME=VALUES(QUOTE_DATETIME), BID=VALUES (BID), BID_SIZE=VALUES (BID_SIZE), ASK=VALUES (ASK),ASK_SIZE=VALUES (ASK_SIZE), "
                    "EXPIRATION=VALUES (EXPIRATION), ROOT=VALUES (ROOT), STRIKE=VALUES (STRIKE), OPTION_TYPE=VALUES (OPTION_TYPE), TRADE_VOLUME=VALUES (TRADE_VOLUME),"
                    " DELTA = VALUES (DELTA), IMPLIED_VOLATILITY = VALUES (IMPLIED_VOLATILITY), GAMMA = VALUES(GAMMA), THETA = VALUES(THETA), VEGA = VALUES(VEGA), "
                    " RHO=VALUES (RHO), UND_PRICE = VALUES(UND_PRICE), UPDATED_DATETIME = VALUES(UPDATED_DATETIME)"
                    )

            self.curA.executemany(add_, dic)
            # sleep(10)
            self.cnx.commit()

            if len(data_) < 10000: break
            data_ = data[iteration_num * 10000:(iteration_num + 1) * 10000]
            iteration_num += 1
            print(iteration_num * 10000)

    # ==================================================================================================================
    def if_exist(self,name):
        ask = ("SHOW TABLES LIKE '" + name + "'")
        self.cursor.execute(ask)
        # print(str(self.cursor.fetchone()))
        if name in str(self.cursor.fetchone()):
            return 1
        else:
            return 0

    # ==================================================================================================================
    def query(self,_query):
        self.cursor.execute(_query)
        return self.cursor

    # ==================================================================================================================
    def get_data_by_pandas(self,_query):
        self.cnx.reconnect()
        return pd.read_sql(_query,self.cnx)

    # ==================================================================================================================
    def save_ib_option_dic_to_mysql(self,dic,tableName):
        qury_ = ("INSERT INTO `" + tableName + "`" +
                    " (QUOTE_DATETIME,SYMBOL, BID, BID_SIZE, ASK, ASK_SIZE, EXPIRATION, ROOT, STRIKE, OPTION_TYPE,TRADE_VOLUME, "
                    "DELTA, IMPLIED_VOLATILITY, GAMMA, THETA, VEGA, RHO, UND_PRICE, PV_DIVIDEND, UPDATED_DATETIME) "
                    "VALUES (%(QUOTE_DATETIME)s, %(SYMBOL)s, %(BID)s, %(BID_SIZE)s, %(ASK)s, %(ASK_SIZE)s,"
                    " %(EXPIRATION)s, %(ROOT)s, %(STRIKE)s, %(OPTION_TYPE)s, %(TRADE_VOLUME)s, %(DELTA)s, %(IMPLIED_VOLATILITY)s,"
                    " %(GAMMA)s, %(THETA)s, %(VEGA)s, %(RHO)s, %(UND_PRICE)s, %(PV_DIVIDEND)s, %(UPDATED_DATETIME)s)"
                    " on DUPLICATE key update"
                    " QUOTE_DATETIME=VALUES(QUOTE_DATETIME), BID=VALUES (BID), BID_SIZE=VALUES (BID_SIZE), ASK=VALUES (ASK),ASK_SIZE=VALUES (ASK_SIZE), "
                    "EXPIRATION=VALUES (EXPIRATION), ROOT=VALUES (ROOT), STRIKE=VALUES (STRIKE), OPTION_TYPE=VALUES (OPTION_TYPE), TRADE_VOLUME=VALUES (TRADE_VOLUME),"
                    " DELTA = VALUES (DELTA), IMPLIED_VOLATILITY = VALUES (IMPLIED_VOLATILITY), GAMMA = VALUES(GAMMA), THETA = VALUES(THETA), VEGA = VALUES(VEGA), "
                    " RHO=VALUES (RHO), UND_PRICE = VALUES(UND_PRICE), PV_DIVIDEND = VALUES(PV_DIVIDEND), UPDATED_DATETIME = VALUES(UPDATED_DATETIME);"
                    )
        self.curB.execute(qury_,dic)
        self.cnx.commit()

if __name__=='__main__':
    pass