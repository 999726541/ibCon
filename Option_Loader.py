
import queue
import time

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract,ContractDetails

from ibapi.common import *
from ibapi.ticktype import *

from threading import Thread
from mysqlCon import mysql_con

def overridesWrapper(fn):
    return fn

class IBApp(EWrapper, EClient):
    def __init__(self):
        print('ppp')
        EWrapper.__init__(self)  # Instantiate a wrapper object
        EClient.__init__(self, wrapper=self)  # Instantiate a client object
        self.dbcon = mysql_con('OPTION_DATA')
        # CONNECTION OPTIONS

        self.socket_port = 4002  # Gateway: 4002; Workstation: 7497
        self.ip_address = "127.0.0.1"
        self.client_id = 1
        self.ContractReqQueue = queue.Queue()
        # APP VARIABLES

        self.reqID = 22  # Stores the next valid request id
        self.account = None  # Stores the account number

        # CONNECT TO SERVER

        self.connect(self.ip_address, self.socket_port, clientId=self.client_id)  # Connect to server
        # Check connection
        print("serverVersion:{} connectionTime:{}\n".format(self.serverVersion(),self.twsConnectionTime()))

        # Live market data is streaming data relayed back in real time.
        # Market data subscriptions are required to receive live market data.
        # Different market data subscriptions give you access to different information
        # https://interactivebrokers.github.io/tws-api/market_data_type.html#gsc.tab=0
        self.reqMarketDataType(1)
        print('Market Data type: 1 ')

        # save all existing contract need to be record, refresh every day
        self.optContractsDic =[]
        # save received msg and store into mysql
        self.tempContractDic = {}
        # save all instruments conId
        self.futureContractsDic = {}

        # when loading each of following stuff make sure other Thread are waiting
        # reqId 1 for loadingFut Locking
        # reqId 2 for loadingOpt Locking
        self.loadingFut = False
        self.loadingOpt = False

    # WRAPPER OVERRIDEN METHODS
    @overridesWrapper
    def nextValidId(self, orderId: int):
        """ Receives next valid order id. This function is called by the server upon connection
        and the information is received upon calling the run function."""
        print('Next ID %s'%orderId)
        self.reqID = orderId  # Assign the received id to the app variable

    @overridesWrapper
    def contractDetails(self, reqId:int, contractDetails:ContractDetails):
        """Receives a comma-separated string with the managed account ids. This function is called
        by the server upon connection and the information is received upon calling the run funtion."""
        if self.loadingFut is True:
            # add future contract into futureStockContractsDic and match this with reqID
            self.futureContractsDic[self.reqID] = contractDetails.summary
            # add for next future
            self.increment_id()

        # print('contractDetails ',vars())

    @overridesWrapper
    def contractDetailsEnd(self, reqId: int):
        # print('contractDetailsEnd',vars())
        # waiting for future loading
        time.sleep(5)
        if self.loadingFut is True:
            self.loadingFut = False

    @overridesWrapper
    def error(self, reqId: TickerId, errorCode: int, errorString: str):
        print('error',vars())

    @overridesWrapper
    def tickPrice(self, reqId:TickerId , tickType:TickType, price:float,
                  attrib:TickAttrib):
        if tickType == 1:
            self.tempContractDic[reqId]['BID'] = price
        if tickType == 2:
            self.tempContractDic[reqId]['ASK'] = price

        # print('tickPrice',vars())

    @overridesWrapper
    def tickSize(self, reqId:TickerId, tickType:TickType, size:int):
        if tickType == 0:
            self.tempContractDic[reqId]['BID_SIZE'] = size
        if tickType == 3:
            self.tempContractDic[reqId]['ASK_SIZE'] = size
        # print('tickSize',vars())

    @overridesWrapper
    def tickGeneric(self, reqId:TickerId, tickType:TickType, value:float):
        # print('tickGeneric',vars())
        pass

    @overridesWrapper
    def tickString(self, reqId:TickerId, tickType:TickType, value:str):
        # print('tickString',vars())
        pass

    @overridesWrapper
    def tickSnapshotEnd(self, reqId:int):
        # print('snapShot End => ',reqId)
        # store this snapshot into mydql database
        try:
            dic = self.tempContractDic.pop(reqId)
        except:
            return ''
        self.dbcon.save_ib_option_dic_to_mysql(dic,'TEST_RAW_OPTION_DATA')
        # print(dic)

    @overridesWrapper
    def tickOptionComputation(self, reqId:TickerId, tickType:TickType ,
            impliedVol:float, delta:float, optPrice:float, pvDividend:float,
            gamma:float, vega:float, theta:float, undPrice:float):
        if impliedVol != None:
            self.tempContractDic[reqId]['IMPLIED_VOLATILITY'] = round(impliedVol,5)
        if delta != None:
            self.tempContractDic[reqId]['DELTA'] = round(delta,5)
        if gamma != None:
            self.tempContractDic[reqId]['GAMMA'] = round(gamma)
        if pvDividend != None:
            self.tempContractDic[reqId]['PV_DIVIDEND'] = round(pvDividend)
        if vega != None:
            self.tempContractDic[reqId]['VEGA'] = round(vega)
        if theta != None:
            self.tempContractDic[reqId]['THETA'] = round(theta)
        if undPrice != None:
            self.tempContractDic[reqId]['UND_PRICE'] = round(undPrice)

        # print('tickOptionComputation',vars())

    @overridesWrapper
    def securityDefinitionOptionParameter(self, reqId:int, exchange:str,
    underlyingConId:int, tradingClass:str, multiplier:str,
    expirations:SetOfString, strikes:SetOfFloat):
        # print('securityDefinitionOptionParameter',vars())
        for expiration in expirations:
            for strike in strikes:
                self.save_opt_contracts_to_dict(
                    # Created a future option by future symbol and other info
                    self.create_fut_opt_contract(
                        self.futureContractsDic[reqId].symbol,exchange,strike,expiration,tradingClass,multiplier,
                        'C'
                    )
                )
                self.save_opt_contracts_to_dict(
                    # Created a future option by future symbol and other info
                    self.create_fut_opt_contract(
                        self.futureContractsDic[reqId].symbol, exchange, strike, expiration, tradingClass, multiplier,
                        'P'
                    )
                )

    @overridesWrapper
    def securityDefinitionOptionParameterEnd(self, reqId:int):
        print('loading Option data finished',vars())
        # waiting for option loading to list
        time.sleep(5)
        if self.loadingOpt is True:
            self.loadingOpt = False



    # OTHER METHODS
    def loading_all_future_options(self,future_list):
        """
        Adding all options corresponding to all futures for different maturity
        """
        while self.conn.isConnected():
            # @TODO change to socket
            ticker = input('Enter symbol you want, enter "Close" to kill thread')

            if self.loadingFut is False and self.loadingFut is False and ticker != 'Close':

                # self.loadingOpt = True
                # Loading Futures that contains options First
                for futureTicker in future_list:
                    contract = self.create_fut_contract(futureTicker)
                    self.reqContractDetails(self.reqID, contract)
                    self.increment_id()
                    self.loadingFut = True
                    # Waite until all futures conid has been loaded
                    while self.loadingFut is True:
                        continue
                print(self.futureContractsDic)
                # starting record options
                for reqID in self.futureContractsDic.keys():
                    self.reqSecDefOptParams(reqID,
                                            self.futureContractsDic[reqID].symbol,
                                            self.futureContractsDic[reqID].exchange,
                                            self.futureContractsDic[reqID].secType,
                                            self.futureContractsDic[reqID].conId
                                            )
                    self.loadingOpt = True
                    while self.loadingOpt is True:
                        continue
                self.increment_id()
                print('Loading option details Finished')
                print('total Number of option:',len(self.optContractsDic))

            else:
                print('Ticker Closed')
                return

    def OptEventsObserver(self):
        """
        Adding contracts to queue once the length of queue less than 10
        Add Thread B
        """
        while True:
            # check if all contract loading to optContractsDic
            # make sure no option details loading
            if self.ContractReqQueue.empty() and self.loadingOpt is False and self.loadingFut is False:
                # print('Loading Data')
                for contract in self.optContractsDic:
                    self.ContractReqQueue.put(contract)


    def loop(self):
        """Loop containing the app logic."""

        # This makes sure we receive the account number and next valid request id
        # both of which are not used in this script, but might be useful for more advanced things
        # like passing orders etc.
        # self.reqIds(2) # get validated reqID
        print('start')
        Thread(target=self.run).start() # start a thread recording msg
        print('thread 1 started')
        Thread(target=self.OptEventsObserver).start() # Thread for add events to queue once there is no event
        print('thread 2 started')
        Thread(target=self.loading_all_future_options,args=(['ES'],)).start() # start a thread for loading option contract accordingly
        print('thread 3 started')
        Thread(target=self.get_snap_shot_parallel, args=(self.ContractReqQueue,), kwargs={'startId':1000000}).start()
        Thread(target=self.get_snap_shot_parallel, args=(self.ContractReqQueue,), kwargs={'startId':2000000}).start()
        Thread(target=self.get_snap_shot_parallel, args=(self.ContractReqQueue,), kwargs={'startId':3000000}).start()

    def save_opt_contracts_to_dict(self,contract:Contract):
        self.optContractsDic.append(contract)

    def increment_id(self):
        """ Increments the request id"""
        self.reqID += 1

    def create_fut_opt_contract(self, symbol,exchange,strike,expiration,tradingClass,multiplier,right_):
        """ Creates an IB contract."""

        contract = Contract()
        contract.symbol = symbol
        contract.exchange = exchange
        contract.secType = 'FOP'
        contract.currency = 'USD'
        contract.right = right_
        contract.strike = strike
        contract.lastTradeDateOrContractMonth = expiration
        contract.tradingClass = tradingClass
        contract.multiplier = multiplier
        return contract

    def create_fut_contract(self,symbol):
        """ Creates an IB contract."""

        contract = Contract()
        contract.symbol = symbol
        contract.exchange = 'GLOBEX'
        contract.secType = 'FUT'
        contract.currency = 'USD'
        # contract.lastTradeDateOrContractMonth = '20171020'
        return contract

    def get_snap_shot_parallel(self,eventsQueue,startId):
        print('Start Loading')
        while self.conn.isConnected():
            if eventsQueue.empty() is False:
                contract = self.ContractReqQueue.get()
                self.tempContractDic[startId] = {
                    'QUOTE_DATETIME':time.strftime('%Y-%m-%d %H:%M:%S'),
                    'SYMBOL':contract.symbol,
                    'BID':-1.000, # tickPrice bid
                    'BID_SIZE':-1.000, # tickSize bid_size
                    'ASK':-1.000,   # tickPrice ask
                    'ASK_SIZE':-1.000,  # tickSize ask_size
                    'EXPIRATION':contract.lastTradeDateOrContractMonth,
                    'ROOT':contract.tradingClass,
                    'STRIKE':contract.strike,
                    'OPTION_TYPE':contract.right,
                    'TRADE_VOLUME':0,
                    'DELTA':None,
                    'IMPLIED_VOLATILITY':None,
                    'GAMMA':None,
                    'THETA':None,
                    'VEGA':None,
                    'RHO':None,
                    'UND_PRICE':None,
                    'PV_DIVIDEND':0,
                    'UPDATED_DATETIME':None
                }
                self.reqMktData(startId,contract,"",True,False,[])
                startId += 1 # add reqID
                time.sleep(.1)

    def check_all_snap_finished(self):
        pass


zz = IBApp()
zz.loop()