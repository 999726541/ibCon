from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.common import *
from ibapi.ticktype import *
from ibapi.contract import ContractDetails


def overrideswrapper(fn):
    return fn



class IBDATAWrapper(EWrapper):
    """

    """
    def __init__(self):
        EWrapper.__init__(self)

# WRAPPER OVERRIDEN METHODS
    @overrideswrapper
    def nextValidId(self, orderId: int):
        """ Receives next valid order id. This function is called by the server upon connection
        and the information is received upon calling the run function."""
        print('Next ID %s'%orderId)
        self.reqID = orderId  # Assign the received id to the app variable

    @overrideswrapper
    def contractDetails(self, reqId:int, contractDetails:ContractDetails):
        """Receives a comma-separated string with the managed account ids. This function is called
        by the server upon connection and the information is received upon calling the run funtion."""

        self.save_opt_contracts_to_dict(contractDetails)
        print('get contract ',contractDetails.summary.symbol,contractDetails.summary.lastTradeDateOrContractMonth)

    @overrideswrapper
    def contractDetailsEnd(self, reqId: int):
        print('End')
        self.loadingOpt = False

    @overrideswrapper
    def error(self, reqId: TickerId, errorCode: int, errorString: str):
        print(reqId,errorCode,errorString)

    @overrideswrapper
    def tickPrice(self, reqId:TickerId , tickType:TickType, price:float,
                  attrib:TickAttrib):
        if tickType == 1:
            self.tempContractDic[reqId]['BID'] = price
        if tickType == 2:
            self.tempContractDic[reqId]['ASK'] = price

        print('tickPrice',reqId,tickType,price,attrib)

    @overrideswrapper
    def tickSize(self, reqId:TickerId, tickType:TickType, size:int):
        if tickType == 0:
            self.tempContractDic[reqId]['BID_SIZE'] = size
        if tickType == 3:
            self.tempContractDic[reqId]['ASK_SIZE'] = size
        print('tickSize',reqId,tickType,size)

    @overrideswrapper
    def tickGeneric(self, reqId:TickerId, tickType:TickType, value:float):
        print('tickGeneric',reqId,tickType,value)

    @overrideswrapper
    def tickString(self, reqId:TickerId, tickType:TickType, value:str):
        print('tickString',reqId,tickType,value)

    @overrideswrapper
    def tickSnapshotEnd(self, reqId:int):
        print('snapShot End => ',reqId)
        # store this snapshot into mydql database
        try:
            dic = self.tempContractDic.pop(reqId)
        except:
            return ''
        self.dbcon.save_ib_option_dic_to_mysql(dic,'TEST_RAW_OPTION_DATA')
        print(dic)

    @overrideswrapper
    def tickOptionComputation(self, reqId:TickerId, tickType:TickType ,
            impliedVol:float, delta:float, optPrice:float, pvDividend:float,
            gamma:float, vega:float, theta:float, undPrice:float):
        if impliedVol != None:
            self.tempContractDic[reqId]['IMPLIED_VOLATILITY'] = impliedVol
        if delta != None:
            self.tempContractDic[reqId]['DELTA'] = delta
        if gamma != None:
            self.tempContractDic[reqId]['GAMMA'] = gamma
        if pvDividend != None:
            self.tempContractDic[reqId]['PV_DIVIDEND'] = pvDividend
        if vega != None:
            self.tempContractDic[reqId]['VEGA'] = vega
        if theta != None:
            self.tempContractDic[reqId]['THETA'] = theta
        if undPrice != None:
            self.tempContractDic[reqId]['UND_PRICE'] = undPrice