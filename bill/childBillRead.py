# -*- coding: utf-8 -*-
import os
# import re
import datetime
import traceback
from decimal import *

# import chardet
# from prettytable import *
# import dbf
import pandas as pd
import logging
import logging.handlers
import logging.config
from bill.billRead import Bill, BillList

#----------------------------------------------------------------------
logging.config.fileConfig(r'..\bill\logging.conf')

child_logger = logging.getLogger('childbill')
#logger = logging.getLogger('billRead')
child_logger.info('test child bill logger')


# 手数 |    开仓价   |    结算价   |   持仓盈亏  |    保证金
#  手数 |    开仓价   |    结算价   |   持仓盈亏  |    保证金
# 手数 |       开仓均价  |        结算价   |     持仓盯市盈亏|    保证金占用
# 成交价|  手数 |  开平 |      手续费   |      成交编号
# 开仓价|       平仓价|  手数 |  开平 |   平仓盈亏  | 投/保|     成交编号
GChildDec = ["手数", "开仓价", "结算价", "持仓盈亏", "保证金", "开仓均价", "持仓盯市盈亏", "保证金占用",
             "成交价", "手续费", "平仓价", "平仓盈亏"]
# keyWords = ['结算单', '资金状况', '持仓明细', '持仓汇总', '成交明细', '平仓明细', '出入金明细', '中信期货']
#KN_Keys = ['结算单', '资金状况', '持仓明细', '持仓汇总', '成交明细', '平仓明细', '出入金明细', '中信期货']
    # operator = {'结算单':setSettlementTxt, '资金状况':setAccountTxt, '持仓明细':setPositionsDetailTxt,
    #             '持仓汇总':setPositionsTxt, '成交明细':setTransactionTxt, '平仓明细':setRealizeTxt, '交割明细':setDeliveryTxt,
    #             '中信期货':setCompTxt, '出入金明细':setDepositNWithdraw}
KN_Keys = ['结算单', '资金状况', '持仓明细', '持仓汇总', '成交明细', '平仓明细', '出入金明细', '中信期货', '交割明细']
KN_Dicts = {"SettlementStatement" : "结算单", "AccountSummary" : "资金状况", "TransactionRecord" : "成交明细",
            "PositionClosed" : "平仓明细", "PositionsDetail" : "持仓明细", "Positions" : "持仓汇总",
            "Delivery" : "交割明细", "Company" : '中信期货', "DepositNWithdraw" : '出入金明细'}
GSsettlement ={'clientID': '账户', 'Date':'日期'}


class childBill(Bill):

    def setSettlementTxt(self, txt = []):
        for line in txt:
            self.settlementTxt.append(line)
        return
    def setAccountTxt(self, txt = []):
        self.accountTxt.extend(txt)
        return
    def setPositionsDetailTxt(self, txt = []):
        self.positionsDetailTxt.extend(txt)
        return
    def setPositionsTxt(self, txt = []):
        self.positionsTxt.extend(txt)
        return
    def setTransactionTxt(self, txt = []):
        self.transactionTxt.extend(txt)
        return

    def setRealizeTxt(self, txt = []):
        self.realizeTxt.extend(txt)
        return
    def setDeliveryTxt(self, txt = []):
        self.deliveryTxt.extend(txt)
        return
    def setCompTxt(self, txt = []):
        return
    def setDepositNWithdraw(self, txt = []):
        self.depositTxt.extend(txt)
        return
    def isGarbageLine(self, txt):
        ans = False
        phanzi=re.compile(u'[\u4e00-\u9fa5]+');
        if len(txt) > 0:

            #res = phanzi.findall(line)
            if '---' in txt:
                res = phanzi.findall(txt)
                if res:
                    #txt = res[0]
                    ans = False

                else:
                    ans = True
            else:
                ans = False
        else:
            ans = True

        return ans
    def setAccNdate(self, txt = []):
        #GSsettlement ={'clientID': '账户', 'Date':'日期'}
        global GSsettlement
        NameToValue = {}
        for line in txt[1:]:

            str =line.split()
            phanzi=re.compile(u'[\u4e00-\u9fa5]+');

            res = phanzi.findall(line)
            nums = re.findall(r'([a-zA-Z]*\d+)', line)



            resLen = res.__len__()
            numsLen = nums.__len__()
            len = resLen if resLen < numsLen else numsLen

            for index in range(len):
                NameToValue[res[index]] = nums[index]
        self.settlementDict = NameToValue
        if GSsettlement['clientID'] in NameToValue:
            self.__accNum = NameToValue[GSsettlement['clientID']]
        elif  '客户号' in NameToValue:
            self.__accNum = NameToValue[GSsettlement['clientID']]
        else:
            print('账户没有正确解析')
            self.__accNum = 'NULL'
        self.__date = NameToValue[GSsettlement['Date']]
        #结算会员:13887    结算会员名称:中信期货(0018)   结算日期:20161213
        #self.__strHeader = '结算会员: ' + self.__accNum[0:-2] + '       结算会员名称:中信期货(0018)' + '      结算日期:' + self.__date
        self.__strHeader = '结算会员: ' + self.__accNum + '       结算会员名称:中信期货(0018)' + '      结算日期:' + self.__date
        return
    def splitToData(self, row):
        res = []
        vals = row.split('|')
        vals = vals[1:-1]
        cleanVals = []
        for eachVal in vals:
            cleanVals.append(eachVal.strip())
        # vals = cleanVals
        if '共' in vals[0]:
            return res
        res = cleanVals
        return res
    def washPosition(self, txtlist = []):
        #print(txtlist)
        index = 0
        #filter the header and nonsense lines
        while("|" not in txtlist[index]):
            index += 1
        schema_txt = txtlist[index]
        phanzi = re.compile(u'[\u4e00-\u9fa5]+\/?[\u4e00-\u9fa5]+');
        keys = phanzi.findall(schema_txt)
        keySize = len(keys)

        # there is no need for fiter the english key
        list = []
        index += 1
        while(index < len(txtlist)):

            if "|" not in txtlist[index]:
                break;

            singleRow = txtlist[index]
            vals = self.splitToData(singleRow)
            if not vals:
                break


            if len(vals) != keySize:
                child_logger.error("in washPosition of ParentBill valSize not equal to keySize")
                child_logger.error(vals)
            else:

                list.append(vals)
            index += 1


        df = pd.DataFrame(list, columns = keys)
        # print(df)
        return df




    def washAccount(self, txt = []):

        NameToValue = {}
        for line in txt[1:]:

            str = ' '.join(line.split())
            phanzi=re.compile(u'[\u4e00-\u9fa5]+[\s]?[\u4e00-\u9fa5]+[\s]?[\u4e00-\u9fa5]+');

            res = phanzi.findall(str)
            nums = re.findall(r'([-+]?\d+\.\d+)', str)

            resLen = res.__len__()
            numsLen = nums.__len__()
            len = resLen if resLen < numsLen else numsLen

            for index in range(len):
                NameToValue[res[index]] = nums[index]
        return NameToValue
    def toNum(self, pdata):
        global GChildDec
        # print(pdata.dtypes)
        pCols = pdata.columns
        for eachCol in pCols:
            if eachCol in GChildDec:
                try:
                    pdata[eachCol] = pdata[eachCol].astype(float)
                except Exception as e:
                    print(pdata[eachCol])

                # pdata[eachCol] = pdata[eachCol].astype(Decimal)
                #print(pdata[eachCol])
        #print(pdata.dtypes)

        return
    def computeTransFee(self):
        global GStransactionRecord

        tTransList = self.transList
        setDet = {}
        for index, row in tTransList.iterrows():
            if row[GStransactionRecord['Instrument']] not in setDet:
                tempSDNode = {}
                tempSDNode['卖'] = 0
                tempSDNode['买'] = 0
                tempSDNode['卖成交价'] = 0
                tempSDNode['买成交价'] = 0
                if '买' in row[GStransactionRecord['B/S']]:
                    tempSDNode['买'] = int(row[GStransactionRecord['Lots']])
                    tempSDNode['买成交价'] = Decimal(row[GStransactionRecord['Price']])
                else:
                    tempSDNode['卖'] = int(row[GStransactionRecord['Lots']])
                    tempSDNode['卖成交价'] = Decimal(row[GStransactionRecord['Price']])
                tempSDNode['手续费'] = Decimal(row[GStransactionRecord['Fee']])
                tempSDNode['合约'] = row[GStransactionRecord['Instrument']]
                tempSDNode['投/保'] = row[GStransactionRecord['S/H']]
                setDet[row[GStransactionRecord['Instrument']]] = tempSDNode
            else:
                tempSDNode = setDet[row[GStransactionRecord['Instrument']]]
                #print(tempSDNode)
                if '买' in row[GStransactionRecord['B/S']]:
                    oldBuyAvg = tempSDNode['买成交价']
                    oldBuySum = tempSDNode['买']
                    nodeBuyAvg = Decimal(row[GStransactionRecord['Price']])
                    nodeBuySum = int(row[GStransactionRecord['Lots']])
                    newBuyAvg = oldBuyAvg * oldBuySum + nodeBuyAvg * nodeBuySum
                    newBuyAvg = newBuyAvg / (oldBuySum + nodeBuySum)
                    tempSDNode['买成交价'] = newBuyAvg
                    tempSDNode['买'] += int(row[GStransactionRecord['Lots']])
                else:
                    oldSellAvg = tempSDNode['卖成交价']
                    oldSellSum = tempSDNode['卖']
                    nodeSellAvg = Decimal(row[GStransactionRecord['Price']])
                    nodeSellSum = int(row[GStransactionRecord['Lots']])
                    newSellAvg = oldSellAvg * oldSellSum + nodeSellAvg * nodeSellSum
                    newSellAvg = newSellAvg / (oldSellSum + nodeSellSum)

                    tempSDNode['卖成交价'] = newSellAvg
                    tempSDNode['卖'] += int(row[GStransactionRecord['Lots']])
                tempSDNode['手续费'] += Decimal(row[GStransactionRecord['Fee']])


        self.__transfee = setDet
        return setDet
    def readPosConfig(self):
        self.IDToMultiplier = {}
        self.__customer_Clientid = {}
        self.__customer_Userid = {}
        self.__broker_Partid = {}
        self.__FutToExchange = {}
        self.__FutToPartid = {}
        self.__FutToClientid = {}
        config = open('./futuresConfig.txt', 'r')
        textlist = config.readlines()
        futureID = re.compile(u'[a-zA-Z]+');
        multiplier = re.compile(u'\d+');
        for item in textlist:
            id = futureID.findall(item)
            mul = multiplier.findall(item)
            self.IDToMultiplier[id[0].upper()] = int(mul[0])

        config.close()

        accConfig = open('./acountConfig.txt', 'r')
        textlist = accConfig.readlines()
        ID = re.compile(u'[a-zA-Z_a-zA-Z]+');
        multiplier = re.compile(u'\d+');
        # customer_Clientid = {}
        # customer_Userid = {}
        # broker_Partid = {}
        operater = {"customer_Clientid": self.__customer_Clientid, "customer_Userid": self.__customer_Userid, "broker_Partid": self.__broker_Partid}
        #accdata = {}
        temp = {}
        key = ''
        for item in textlist:

            if "#" in item:
                id = ID.findall(item)
                temp = operater[id[0]]
            else:

                id = ID.findall(item)
                mul = multiplier.findall(item)
                temp[id[0]] = mul[0]
        accConfig.close()

        futConfig = open('./config.ini', 'r')
        textlist = futConfig.readlines()
        getWord = re.compile(u'[a-zA-Z]+');
        multiplier = re.compile(u'\d+');

        tExchange = ''
        tFuture = ''
        for item in textlist:

            if "#" in item:
                tWords = getWord.findall(item)
                tExchange = tWords[0]
            else:
                tWords = getWord.findall(item)
                tFuture = tWords[0]
                self.__FutToExchange[tFuture] = tExchange
                self.__FutToClientid[tFuture] = self.__customer_Clientid[tExchange]
                self.__FutToPartid[tFuture] = self.__broker_Partid[tExchange]



        return
    def genTable(self):
        templist = []
        processedRec = {} #to record processed settlement
        global GSpositions

        if hasattr(self, 'positionList'):
            tposList = self.positionList
            for index, row in tposList.iterrows():
                instrument = row[GSpositions['Instrument']]
                buyHolding = 0
                sellHolding = 0
                if instrument not in processedRec:
                    if '买' in row[GSpositions['B/S']]:
                        buyHolding = int(row[GSpositions['Lots']])
                    else:
                        sellHolding = int(row[GSpositions['Lots']])
                    getWord = re.compile(u'[a-zA-Z]+');
                    futureHead = getWord.findall(row[GSpositions['Instrument']])
                    temp_partid = self.__FutToPartid[futureHead[0].upper()]
                    temp_clientid = self.__FutToClientid[futureHead[0].upper()]
                    temp_Margin = 0
                    temp_Margin = float(row[GSpositions['MarginOccupied']])
                    oneTabRow = [temp_partid, temp_clientid, row[GSpositions['Instrument']], float(row[GSpositions['SttlToday']]), 0, 0, 0, 0, 0, 0, 0.00, 0.00,buyHolding, sellHolding, temp_Margin, float(row[GSpositions['MTMP/L']]), 0.00 ]
                    processedRec[instrument] = oneTabRow
                else:
                    if '买' in row[GSpositions['B/S']]:
                        buyHolding = int(row[GSpositions['Lots']])
                    else:
                        sellHolding = int(row[GSpositions['Lots']])
                    existRow = processedRec[instrument]
                    existRow[12] += buyHolding
                    existRow[13] += sellHolding
                    #判断是否是单边最大方向，如果是，则更新保证金占用字段，如果不是则不更新

                    temp_Margin = float(row[GSpositions['MarginOccupied']])
                    existRow[14] += temp_Margin
                    # if float(onePos['保证金占用']) > existRow[15]:
                    #     existRow[14] = float(onePos['保证金占用'])
                    existRow[15] += float(row[GSpositions['MTMP/L']])
                    processedRec[instrument] = existRow

        if self.transactionTxt:
            for instrument in self.__transfee:
                tRec = self.__transfee[instrument]
                if tRec['合约'] in processedRec:
                    tFee = round(tRec['手续费'],2)
                    processedRec[tRec['合约']][16] = tFee
                else:
                    buyHolding = tRec['买']
                    sellHolding = tRec['卖']
                    #float(onePos['结算价'])
                    tclearPrice = round(tRec['卖成交价'],2)
                    #float(onePos['保证金占用'])
                    tmargin = 0.0
                    #float(onePos['持仓盯市盈亏'])
                    tactual = 0.0
                    getWord = re.compile(u'[a-zA-Z]+');
                    futureHead = getWord.findall(tRec['合约'])
                    temp_partid = self.__FutToPartid[futureHead[0].upper()]
                    temp_clientid = self.__FutToClientid[futureHead[0].upper()]
                    tFee = round(tRec['手续费'],2)
                    tRow = [temp_partid, temp_clientid, tRec['合约'], tclearPrice, 0, 0, 0, 0, 0, 0, 0.00, 0.00,buyHolding, sellHolding, tmargin, tactual, tFee]
                    processedRec[tRec['合约']] = tRow

        return processedRec
    def writePosDbf(self, path):
        temphead = {}
        temphead['结算会员号'] = str(self.__accNum)
        table = dbf.Table(path)
        table.open()
        try:
            copyTable = table.new('./output/'+ self.__accNum + '_' + self.__date + '_settlementdetail.dbf')
        except PermissionError as e:
            print(e)
            print("dbf file already has been opened, please close it and restart the app")
        copyTable.open()

        rowVals = []
        # if self.transactionTxt:
        #     self.computeTransFee()
        # self.readPosConfig()
        processedRec = self.genTable()
        # self.__genTable = processedRec.values()
        rowVals = processedRec.values()
        for oneRow in rowVals:
            tempRecord = []
            for data in oneRow:
                tempRecord.append(str(data))
            # if Gdebug:
            #     print(tuple(tempRecord))
            #table.append(tuple(tempRecord))
            copyTable.append(tuple(tempRecord))
            #tempRecord.clear()

        copyTable.close()
        table.close()


        return

    def assignAcc(self):
        self.__Balance_bf = 0.00
        self.__Deposit = 0.00
        self.__Realized = 0.00
        self.__MTM = 0.00
        self.__Commission = 0.00
        self.__Delivery_Fee = 0.00
        self.__Balance_cf = 0.00
        self.__Margin_Occupied = 0.00
        self.__Fund_Avail = 0.00
        self.__Risk_Degree = 0.00
        self.__Currency = 'CNY'
        self.__account = 0
        self.__mydate = ''
        self.__ChgInFund = 0.0
        self.__Payment = 0.0
        try:
            NameToValue = self.accountDict
            self.__Balance_bf = float(NameToValue[GSaccShema['PreBalance']])
            self.__Delivery_Fee = float(NameToValue[GSaccShema['DeliveryFee']])
                #self.__Deposit = float(NameToValue['出 入 金'])
            self.__Deposit = float(NameToValue[GSaccShema['DepositWithdrawal']])
            self.__Balance_cf = float(NameToValue[GSaccShema['Balancecf']])
            self.__Realized = float(NameToValue[GSaccShema['RealizedPL']])
            self.__Margin_Occupied = float(NameToValue[GSaccShema['MarginOccupied']])
            self.__MTM = float(NameToValue[GSaccShema['MTMPL']])
            self.__Fund_Avail = float(NameToValue[GSaccShema['FundAvail']])
            self.__Commission = float(NameToValue[GSaccShema['Fee']])
            self.__Risk_Degree = float(NameToValue[GSaccShema['RiskDegree']])
            self.__Currency = 'CNY'
            self.__ChgInFund = 0.00 + self.__MTM - self.__Commission -self.__Payment
        except KeyError as e:
            print("金牛导出结算单字段名发生改变，请联系开发人员")
            print(e)
            traceback.print_exc(file=sys.stdout)
        except Exception as e:
            print(e)
            traceback.print_exc(file=sys.stdout)
        return
    def writeAccDbf(self, path):
        global  GSaccShema

        self.assignAcc()
        temphead = {}
        temphead['结算会员号'] = str(self.__accNum)
        table = dbf.Table(path)
        table.open()
        copyTable = table.new('./output/'+ self.__accNum + '_' + self.__date + '_capital.dbf')
        copyTable.open()


        rows = []
        rows.append((temphead['结算会员号'], '上一交易日实有货币资金余额', str(self.__Balance_bf)))
        rows.append((temphead['结算会员号'],'加：当日收入资金', str(self.__Deposit)))
        rows.append((temphead['结算会员号'], '当日盈亏', str(self.__MTM)))
        rows.append((temphead['结算会员号'],'减：当日付出资金', str(self.__Payment)))
        rows.append((temphead['结算会员号'], '手续费', str(self.__Commission)))
        rows.append((temphead['结算会员号'], '其中：交易手续费', str(self.__Commission)))
        rows.append((temphead['结算会员号'], '结算手续费', '0.00'))
        rows.append((temphead['结算会员号'],'交割手续费', '0.00'))
        rows.append((temphead['结算会员号'], '移仓手续费', '0.00'))
        rows.append((temphead['结算会员号'], '当日实有货币资金余额', str(self.__Balance_cf)))
        rows.append((temphead['结算会员号'],'其中：交易保证金', str(self.__Margin_Occupied)))
        rows.append((temphead['结算会员号'],  '结算准备金', str(self.__Fund_Avail)))
        rows.append((temphead['结算会员号'],  '减：交易保证金', str(self.__Margin_Occupied)))
        rows.append((temphead['结算会员号'],  '当日结算准备金余额', str(self.__Fund_Avail)))
        rows.append((temphead['结算会员号'],  '加：申报划入金额', '0.00'))
        rows.append((temphead['结算会员号'],  '减：申报划出金额', '0.00'))
        rows.append((temphead['结算会员号'],  '下一交易日开仓准备金', str(self.__Fund_Avail)))
        rows.append((temphead['结算会员号'], '其它', '-' ))
        rows.append((temphead['结算会员号'], '应收手续费', str(self.__Commission )))
        rows.append( (temphead['结算会员号'], '实有货币资金变动', str(float(self.__MTM) + float(self.__Realized )) ))
        rows.append((temphead['结算会员号'], '其中：交易保证金变动', '0.00' ))
        rows.append((temphead['结算会员号'], '结算准备金变动', '0.00'))

        s =  r'上一交易日实有货币资金余额'
        #s.decode('UTF-8')
        #table.append( ('13887', '上一交易日实有货币资金余额', '10000.00') )
        for datum in rows:
            #table.append(datum)
            copyTable.append(datum)

        copyTable.close()
        table.close()
        return

    def writeTransDbf(self, path):

        tTransaction = self.transList
        table = dbf.Table(path)
        table.open()
        copyTable = table.new('./output/'+ self.__accNum + '_' + self.__date + '_Trade.dbf')
        copyTable.open()
        tempTime = "9:30:00"
        timedelta = datetime.timedelta(minutes=1)
        pivotTime = datetime.datetime.strptime(tempTime,'%H:%M:%S')
        pivotOrderid = 400000
        #transaction
        global GStransactionRecord

        for index, row in tTransaction.iterrows():
            futureID = re.compile(u'[a-zA-Z]+');
            try:
                tFuture = futureID.findall(row[GStransactionRecord['Instrument']].upper())
            except KeyError as e:
                print(e)
                traceback.print_exc(file=sys.stdout)
            except Exception as e:
                print(e)
                traceback.print_exc(file=sys.stdout)
            try:
                TradeAmount = int(row[GStransactionRecord['Lots']]) * float(row[GStransactionRecord['Price']]) * self.IDToMultiplier[tFuture[0]]
            except KeyError as e:
                print(e)
                print("合约不存在，请补全配置文件futuresConfig.txt")
                print("print exc")
                traceback.print_exc(file=sys.stdout)

            getWord = re.compile(u'[a-zA-Z]+');
            futureHead = getWord.findall(row[GStransactionRecord['Instrument']])
            temp_partid = self.__FutToPartid[futureHead[0].upper()]
            temp_clientid = self.__FutToClientid[futureHead[0].upper()]
            temp_Userid = self.__customer_Userid["Userid"]
            strTime = pivotTime.strftime('%X')
            pivotTime = pivotTime + timedelta
            strOrderid = str(pivotOrderid)
            pivotOrderid = pivotOrderid + 1


            if len(row[GStransactionRecord['Trans.No.']]) > 12:
                oneTabTrade = (temp_partid, temp_clientid, row[GStransactionRecord['Instrument']], row[GStransactionRecord['Trans.No.']][-12:],
                               str(row[GStransactionRecord['Lots']]), str(row[GStransactionRecord['Price']]), str(TradeAmount), strTime, row[GStransactionRecord['B/S']],
                               row[GStransactionRecord['O/C']], strOrderid, temp_Userid)
            else:

                oneTabTrade = (temp_partid, temp_clientid, row[GStransactionRecord['Instrument']],
                               row[GStransactionRecord['Trans.No.']], str(row[GStransactionRecord['Lots']]),
                               str(row[GStransactionRecord['Price']]), str(TradeAmount), strTime,
                               row[GStransactionRecord['B/S']], row[GStransactionRecord['O/C']],
                               strOrderid, temp_Userid)
            #table.append(oneTabTrade)
            #print(oneTabTrade)
            #print('====================')


            copyTable.append(oneTabTrade)

        copyTable.close();
        table.close()

        return
    def writeDbf(self, path =  './template'):
        # self.__myAcc.writeDbf(self.__myDBFPath + '/capital.dbf')
        accDbfName = '/capital.dbf'
        posDbfName = '/settlementdetail.dbf'
        transDbfName = '/Trade.dbf'

        self.readPosConfig()
        if self.transactionTxt:
            self.computeTransFee()

        if self.accountTxt:
            self.writeAccDbf(path + accDbfName)

        if self.positionsTxt:
            self.writePosDbf(path + posDbfName)
        elif self.transactionTxt:
            self.writePosDbf(path + posDbfName)
            print()
        else:
            print()
        if self.transactionTxt:
            self.writeTransDbf(path + transDbfName)

        return
    def cleanRawTxt(self,txt):
        self.settlementTxt = []
        self.accountTxt = []
        self.depositTxt = []
        self.transactionTxt = []
        self.realizeTxt = []
        self.deliveryTxt = []
        self.positionsDetailTxt = []
        self.positionsTxt = []
        global KN_Keys
        global KN_Dicts
        self.keyWords = KN_Keys

        self.operator = {KN_Dicts['SettlementStatement']: self.setSettlementTxt, KN_Dicts['AccountSummary']: self.setAccountTxt, KN_Dicts['PositionsDetail']: self.setPositionsDetailTxt,
                        KN_Dicts['Positions']: self.setPositionsTxt, KN_Dicts['TransactionRecord']: self.setTransactionTxt, KN_Dicts['PositionClosed']: self.setRealizeTxt,
                        KN_Dicts['Delivery']: self.setDeliveryTxt, KN_Dicts['Company']: self.setCompTxt, KN_Dicts['DepositNWithdraw']: self.setDepositNWithdraw}



        cleanTxt = []
        txtcup = []
        tempkey = ''
        newBlock = False
        #self.cleanDBFTables(self.__myDBFPath)
        GenTxt = False
        for line in txt:
            if line:
                for key in self.keyWords:
                    if key in line:
                        if txtcup:
                            if tempkey in self.operator:
                                self.operator.get(tempkey)(txtcup)
                            txtcup.clear()
                        tempkey = key
                        newBlock = True
                        break
                if len(line) > 0:
                    txtcup.append(line)


        if txtcup:
            self.operator.get(tempkey)(txtcup)
            txtcup.clear()

        if self.settlementTxt:
            self.setAccNdate(self.settlementTxt)
        if self.accountTxt:
            self.accountDict = self.washAccount(self.accountTxt)
        if self.positionsDetailTxt:
            self.positionsDetailList = self.washPosition(self.positionsDetailTxt)
            self.toNum(self.positionsDetailList)
        if self.deliveryTxt:
            self.deliveryList = self.washPosition(self.deliveryTxt)
            self.toNum(self.deliveryList)
        if self.depositTxt:
            self.depositList = self.washPosition(self.depositTxt)
            self.toNum(self.depositList)
        if self.positionsTxt:
            self.positionList = self.washPosition(self.positionsTxt)
            self.toNum(self.positionList)
        if self.transactionTxt:
            self.transList = self.washPosition(self.transactionTxt)
            self.toNum(self.transList)

        return

    # 将账户信息转化为字典类型
    def get_settle(self):
        mod_settle = self.settlementDict
        return mod_settle

    # 将账户信息，及详细信息字典类型的key转换为数据库表字段，并且转换数据类型，以便插入数据库
    def get_accsum(self):
        # {'日期 Date': '20170407', '客户号 Client ID': '100109999', '客户名称 Client Name': '中证资本管理（深圳）有限公司'}
        key_header = {'多头期权市值': 'MARKET_VALUE_LONG', '币种': 'CURRENCY_CODE', '货币质出': 'FX_REDEMPTION',
                      '质押变化金额': 'CHG_IN_PLEDGE_AMT',
                      '客户权益': 'CLIENT_EQUITY', '期末结存': 'BALANCE_C_F', '行权手续费': 'EXERCISE_FEE',
                      '基础保证金': 'INITIAL_MARGIN',
                      '货币质入': 'NEW_FX_PLEDGE', '质 押 金': 'PLEDGE_AMOUNT', '货币质押保证金占用': 'FX_PLEDGE_OCC',
                      '空头期权市值': 'MARKET_VALUE_SHORT', '交割保证金': 'DELIVERY_MARGIN', '账户': 'CLIENT_ID',
                      '上次结算资金': 'BALANCE_B_F',
                      '制表时间': 'CREATION_DATE', '日期': 'DATE_VALUE', '权利金收入': 'PREMIUM_RECEIVED', '客户名称': 'CLIENT_NAME',
                      '风险度': 'RISK_DEGREE', '期权执行盈亏': 'EXERCISE_P_L', '应追加资金': 'MARGIN_CALL',
                      '市值权益': 'MARKET_VALUE_EQUITY',
                      '可用资金': 'FUND_AVAIL', '权利金支出': 'PREMIUM_PAID', '平仓盈亏': 'REALIZED_P_L', '持仓盈亏': 'MTM_P_L',
                      '保证金占用': 'MARGIN_OCCUPIED', '出入金': 'DEPOSIT_WITHDRAWAL', '手续费': 'COMMISSION',
                      '交割手续费': 'DELIVERY_FEE'}
        str_type = ['client_name', 'client_id']
        date_type = ['date_value']

        mod_acc_dict = {key_header[key].lower(): self.accountDict[key] for key in self.accountDict}
        mod_settle = self.get_settle()
        mod_settle_1 = {key_header[key].lower(): mod_settle[key] for key in mod_settle}
        mod_acc_dict.update(mod_settle_1)
        for key in mod_acc_dict:
            # print(key, " ", mod_acc_dict[key])
            if key in str_type:
                continue
            elif key in date_type:
                mod_acc_dict[key] = datetime.datetime.strptime(mod_acc_dict[key], "%Y%m%d")
            else:
                mod_acc_dict[key] = float(mod_acc_dict[key])
        return mod_acc_dict


# 将一个文件下下所有金牛账单抽象为一个list
class ChildBillList(BillList):

    def getBillKeys(self):
        keys = []
        keys = self.__BillDict.keys()
        return keys
    def getBill(self, key):

        cBill = self.__BillDict[key]
        return cBill

    def getAllBills(self):
        return self.__BillDict.values()

    def getBills(self):
        self.__BillDict = {}
        for acc in self.textList:
            temp_txt = self.textList[acc]
            cBill = childBill(temp_txt)
            self.__BillDict[acc] = cBill
        return


    def getAcc(self, file_name):
        ans = ""
        ans = os.path.splitext(os.path.split(file_name)[-1])[0]
        # ans = keys[-1]
        return ans



def readSingleBill(file):


    if not os.path.isfile(file):
        return
    encoding = chardet.detect(open(file, 'rb').readline())['encoding']
    with open(file, 'rb') as srcFile:
        content = srcFile.read().decode(encoding)

        textlist = content.split('\n')

    return textlist
def main():

    path = './kingnew'
    subAccPath = './kingnew/zh0232017-03-10.txt';

    cbills = ChildBillList(path)

    bills = list(cbills.getAllBills())
    data_list = []
    for a_bill in bills:
        print(a_bill.settlementDict)
        print(a_bill.accountDict)
        print(a_bill.get_accsum())
        data_list.append(a_bill.get_accsum())
    print(bills)

    # txt = readSingleBill(subAccPath)
    # cbill = childBill(txt)
    # parentAccPath = "./parentAcc"
    # presentCTPBill = './presentCTPBill'





    print("\n程序运行正常结束！")


    os.system("pause")
    return

if __name__=="__main__":
    main()