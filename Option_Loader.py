
import queue
import time

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract,ContractDetails

from ibapi.common import *
from ibapi.ticktype import *

from threading import Thread

def overridesWrapper(fn):
    return fn


class IBApp(EWrapper, EClient):
    def __init__(self):
        EWrapper.__init__(self)  # Instantiate a wrapper object
        EClient.__init__(self, wrapper=self)  # Instantiate a client object

        # CONNECTION OPTIONS

        self.socket_port = 4001  # Gateway: 4002; Workstation: 7497
        self.ip_address = "127.0.0.1"
        self.client_id = 0
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
        self.optContractsDic ={}
        # save received msg and store into mysql
        self.tempContractDic = {}
        self.loadingMsg = True

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

        self.save_opt_contracts_to_dict(contractDetails)

    @overridesWrapper
    def contractDetailsEnd(self, reqId: int):
        print('End')
        self.loadingMsg = False

    @overridesWrapper
    def error(self, reqId: TickerId, errorCode: int, errorString: str):
        print(reqId,errorCode,errorString)

    @overridesWrapper
    def tickPrice(self, reqId:TickerId , tickType:TickType, price:float,
                  attrib:TickAttrib):
        print(reqId,tickType,price,attrib)

    @overridesWrapper
    def tickSize(self, reqId:TickerId, tickType:TickType, size:int):
        print(reqId,tickType,size)

    @overridesWrapper
    def tickGeneric(self, reqId:TickerId, tickType:TickType, value:float):
        print(reqId,tickType,value)

    @overridesWrapper
    def tickString(self, reqId:TickerId, tickType:TickType, value:str):
        print(reqId,tickType,value)

    @overridesWrapper
    def tickSnapshotEnd(self, reqId:int):
        print('snapShot End => ',reqId)


    # OTHER METHODS
    def loading_all_option_details(self):
        """
        blocking optContractsDic until contractDetails triggred
        """
        contract = self.create_contract('SPX')

        print('starting ur request')
        self.reqContractDetails(self.reqID, contract)
        while self.loadingMsg == True:
            continue

        print('Loading option details Finished')

    def OptEventsObserver(self):
        """
        Adding contracts to queue once the length of queue less than 10
        """
        while True:
            # check if all contract loading to optContractsDic
            # make sure no option details loading
            if self.ContractReqQueue.empty() and self.loadingMsg == False:
                for contract in self.optContractsDic.values():
                    self.ContractReqQueue.put(contract)


    def loop(self):
        """Loop containing the app logic."""

        # This makes sure we receive the account number and next valid request id
        # both of which are not used in this script, but might be useful for more advanced things
        # like passing orders etc.
        self.reqIds(2) # get validated reqID

        Thread(target=self.run).start() # start a thread recording msg
        self.loading_all_option_details()
        self.optContractsDic

    def save_opt_contracts_to_dict(self,contractDetails:ContractDetails):
        self.optContractsDic[contractDetails.summary.localSymbol] = contractDetails.summary

    def increment_id(self):
        """ Increments the request id"""
        self.reqID += 1

    def create_contract(self, symbol):
        """ Creates an IB contract."""

        contract = Contract()
        contract.symbol = symbol
        contract.exchange = 'SMART'
        contract.secType = 'OPT'
        contract.currency = 'USD'
        # contract.lastTradeDateOrContractMonth = '20171020'
        return contract

    def get_snap_shot_parallel(self):
        while self.ContractReqQueue.not_empty:
            contract = self.ContractReqQueue.get()
            self.tempContractDic[self.reqID] = {
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
                'TRADING_VOLUME':0,
                'DELTA':0,
                'IMPLIED_VOLATILITY':0,
                'GAMMA':0,
                'THETA':0,
                'VEGA':0,
                'RHO':0,
                'UNDER_PRICE':0,
                'UPDATED_DATETIME':time.strftime('%Y-%m-%d %H:%M:%S')
            }

            self.increment_id() # add reqID

    def check_all_snap_finished(self):
        pass


zz = IBApp()
zz.loop()