# -*- coding: utf-8 -*-
# 读取中信期货有限公司的对账单
import os
import re

# import chardet
import datetime
import pandas as pd

import logging
import logging.handlers
import logging.config



Gdebug = False

       # 品种       |      合约      |    买持     |    买均价   |     卖持     |    卖均价    |  昨结算  |  今结算  |持仓盯市盈亏|  保证金占用   |  投/保     |   多头期权市值   |   空头期权市值    |
       # |成交日期| 交易所 |       品种       |      合约      |买/卖|   投/保    |  成交价  | 手数 |   成交额   |       开平       |  手续费  |  平仓盈亏  |     权利金收支      |  成交序号  |
GPaDec = ["买持", "买均价", "卖持", "卖均价", "昨结算", "今结算", "持仓盯市盈亏", "保证金占用", "多头期权市值", "空头期权市值",
          "成交价", "手数", "成交额", "手续费", "平仓盈亏", "权利金收支"
          ]
#keyWords = ['结算单', '资金状况', '持仓明细', '持仓汇总', '成交明细', '平仓明细', '出入金明细', '中信期货']
# pBillKeys = {'交易结算单(盯市)', '资金状况', '成交记录', '平仓明细', '持仓明细', '持仓汇总'}
# added on 2017-09-08
pBillKeys = {'交易结算单(盯市)', '资金状况', '成交记录', '平仓明细', '持仓明细', '持仓汇总', '出入金明细'}
pBillDicts = {"SettlementStatement" : "交易结算单(盯市)", "AccountSummary" : "资金状况", "TransactionRecord" : "成交记录", "PositionClosed" : "平仓明细", "PositionsDetail" : "持仓明细", "Positions" : "持仓汇总"}

GParentSettlement ={'clientID': '客户号 Client ID', 'Date':'客户名称 Client Name'}
#----------------------------------------------------------------------
logging.config.fileConfig(r'..\bill\logging.conf')
root_logger = logging.getLogger('root')
root_logger.info('root logger')
logger = logging.getLogger('main')
#logger = logging.getLogger('billRead')
logger.info('test main logger')
#logging.ERROR



class Bill(object):

    def __init__(self, textList = []):
        self.settlementTxt = []
        self.accountTxt = []
        self.depositTxt = []
        self.transactionTxt = []
        self.realizeTxt = []
        self.deliveryTxt = []
        self.positionsDetailTxt = []
        self.positionsTxt = []
        name = ''
        block = []
        for line in textList:
            #remove all invisiable tag like \r \n space and so on
            if (not line) or (not line.strip('\n')) or (self.isGarbageLine(line)) or (not line.strip('\r')):
                    continue
            else:
                    if len(line) > 0 and line:
                        block.append(line.strip())

        self.cleanRawTxt(block)

        return
    def isGarbageLine(self, txt):
        ans = False
        phanzi=re.compile(u'[\u4e00-\u9fa5]+');

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

        return ans

    def setSettlementTxt(self, txt = []):
        # for line in txt:
        #     self.settlementTxt.append(line)
        return
    def setAccountTxt(self, txt = []):
        #self.accountTxt.extend(txt)
        return
    def setPositionsDetailTxt(self, txt = []):
        #self.positionsDetailTxt.extend(txt)
        return
    def setPositionsTxt(self, txt = []):
        self.positionsTxt.extend(txt)
        return
    def setTransactionTxt(self, txt = []):
        #self.transactionTxt.extend(txt)
        return

    def setRealizeTxt(self, txt = []):
        #self.realizeTxt.extend(txt)
        return
    def setDeliveryTxt(self, txt = []):
        #self.deliveryTxt.extend(txt)
        return
    def setCompTxt(self, txt = []):
        return
    def setDepositNWithdraw(self, txt = []):
        #self.depositTxt.extend(txt)
        return
    # operator = {'结算单':setSettlementTxt, '资金状况':setAccountTxt, '持仓明细':setPositionsDetailTxt,
    #             '持仓汇总':setPositionsTxt, '成交明细':setTransactionTxt, '平仓明细':setRealizeTxt, '交割明细':setDeliveryTxt,
    #             '中信期货':setCompTxt, '出入金明细':setDepositNWithdraw}
    def writeDeliveryDbf(self, path):

        return
    def writeDepositDbf(self, path):
        return
    def writePosDbf(self, path):
        return
    def writeTransDbf(self, path):

        return
    def writeAccDbf(self, path):


        return

    def writeDbf(self):


        return
    def cleanRawTxt(self,txt):
        global pBillDicts

        self.keyWords = pBillDicts
        #pBillDicts = {"SettlementStatement" : "交易结算单(盯市)", "AccountSummary" : "资金状况", "TransactionRecord" : "成交记录", "PositionClosed" : "平仓明细", "PositionsDetail" : "持仓明细", "Positions" : "持仓汇总"}
        self.operator = {pBillDicts['SettlementStatement']: self.setSettlementTxt, pBillDicts['AccountSummary']: self.setAccountTxt, pBillDicts['PositionsDetail']: self.setPositionsDetailTxt,
                pBillDicts['Positions']: self.setPositionsTxt, pBillDicts['TransactionRecord']: self.setTransactionTxt, pBillDicts['PositionClosed']: self.setRealizeTxt}


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
            self.operator.get(tempkey)(self, txtcup)
            txtcup.clear()
        # if self.settlementTxt:
        #     self.setAccNdate(self.settlementTxt)
        # if self.accountTxt:
        #     self.__myAcc.set(self.accountTxt)
            #self.__myAcc.setAccNdate(self.__accNum, self.__date)
        positionList = {}
        if self.positionsTxt:
            positionList = self.__myPositions.set(self.positionsTxt)


        return positionList
    def read(self):

        return
    def readAll(self, path):
        files = os.listdir(path)
        textContainer = []
        for file in files:
            file = path + '/' + file
            if not os.path.isfile(file):
                continue
            if file.endswith('txt'):
                with open(file, 'rb') as srcFile:
                    #content = fp.read().decode('utf-8')
                    content = srcFile.read().decode('gbk')
                    # for oneline in content:
                    #     oneline.decode('utf-8')
                    #content = content.strip('\r')
                    text = content.split('\n')


                    #text = srcFile.readlines()
                textContainer.append(text)
                srcFile.close()

        return textContainer

    def get_accsum(self):
        return self.accountDict





class ParentBill(Bill):

    def setSettlementTxt(self, txt = []):
        for line in txt:
            self.settlementTxt.append(line)
        return
    def setAccountTxt(self, txt = []):
        self.accountTxt.extend(txt)
        return
    def setPositionsDetailTxt(self, txt = []):
        #self.positionsDetailTxt.extend(txt)
        return
    def setPositionsTxt(self, txt = []):
        self.positionsTxt.extend(txt)
        return
    def setTransactionTxt(self, txt = []):
        self.transactionTxt.extend(txt)
        return

    def setRealizeTxt(self, txt = []):
        #self.realizeTxt.extend(txt)
        return
    def setDeliveryTxt(self, txt = []):
        #self.deliveryTxt.extend(txt)
        return
    def setCompTxt(self, txt = []):
        return
    def setDepositNWithdraw(self, txt = []):
        #self.depositTxt.extend(txt)
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
        #filter the english key
        index += 2
        #filter the nonsense line
        list = []
        while(index < len(txtlist)):

            if "|" not in txtlist[index]:
                break;



            # testVal = re.compile(u'[^\|]*');
            # vals = testVal.findall(txtlist[index])

            singleRow = txtlist[index]
            vals = self.splitToData(singleRow)
            if not vals:
                break
            # vals = singleRow.split('|')
            # vals = vals[1:-1]
            # cleanVals = []
            # for eachVal in vals:
            #     cleanVals.append(eachVal.strip())
            # vals = cleanVals
                # print(eachVal.strip())
            # collectVal = re.compile(u'\d*[\u4e00-\u9fa5]+\d+[\u4e00-\u9fa5]*|[A-Za-z]{1,2}\d{3,4}|[-+]?\d+\.?\d+|[\u4e00-\u9fa5]+|\d');
            # vals = collectVal.findall(txtlist[index])
            # vals = cleanVals
            if '共' in vals[0]:
                break
            if len(vals) != keySize:
                logger.error("in washPosition of ParentBill valSize not equal to keySize")
                logger.error(vals)
            else:
                # temp = {}
                # for index in range(len(keys)):
                #     temp[keys[index]] = vals[index]

                list.append(vals)
            index += 1

            #vals = txtlist[index].split('|')
        #(list)
        df = pd.DataFrame(list, columns = keys)
        # print(df)
        return df
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
    def washTrantransaction(self,txtlist = []):
        #print(txtlist)
        index = 0
        #filter the header and nonsense lines
        while("|" not in txtlist[index]):
            index += 1
        schema_txt = txtlist[index]
        phanzi = re.compile(u'[\u4e00-\u9fa5]+\/?[\u4e00-\u9fa5]+');
        keys = phanzi.findall(schema_txt)
        keySize = len(keys)
        #filter the english key
        index += 2
        #filter the nonsense line
        list = []
        while(index < len(txtlist)):

            if "|" not in txtlist[index]:
                break;

            # testVal = re.compile(u'[^\|]*');
            # vals = testVal.findall(txtlist[index])
            singleRow = txtlist[index]
            vals = self.splitToData(singleRow)
            if not vals:
                break
            # singleRow = txtlist[index]
            # vals = singleRow.split('|')
            # vals = vals[1:-1]
            # cleanVals = []
            # for eachVal in vals:
            #     cleanVals.append(eachVal.strip())
            # vals = cleanVals
            # collectVal = re.compile(u'[\u4e00-\u9fa5]+\d+[\u4e00-\u9fa5]*|[A-Za-z]{1,2}\d{3,4}|[-+]?\d+\.?\d+|[\u4e00-\u9fa5]+|\d');
            # vals = collectVal.findall(txtlist[index])
            if '共' in vals[0]:
                break
            if len(vals) != keySize:
                logger.error("in washPosition of ParentBill valSize not equal to keySize")
                logger.error(vals)

            else:
                # temp = {}
                # for index in range(len(keys)):
                #     temp[keys[index]] = vals[index]

                list.append(vals)
            index += 1

            #vals = txtlist[index].split('|')
        #print(list)
        df = pd.DataFrame(list, columns = keys)
        #print(df)
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

    def parse_str(self, str=None, gap='：'):
        keys = []
        vals = []
        if not str:
            return None
        str_list = str.split(gap)
        keys.append(str_list[0])
        for item in str_list[1:-1]:
            sp_items = item.split(maxsplit=1)
            tval = sp_items[0]
            tkey = sp_items[1]
            keys.append(tkey)
            vals.append(tval)
        vals.append(str_list[-1])

        ans = {key: val for key, val in zip(keys, vals)}
        return ans


    def setAccNdate(self, txt = []):
        # GParentSettlement ={'clientID': '客户号', 'Date':'日期'}
        global GParentSettlement
        # global GParentSettlement
        # t_test = self.washAccount(txt)
        NameToValue = {}
        for line in txt[1:]:
            t_dict = self.parse_str(line)
            NameToValue.update(t_dict)

        self.settlementDict = NameToValue
        if GParentSettlement['clientID'] in NameToValue:
            self.__accNum = NameToValue[GParentSettlement['clientID']]
        elif  '客户号' in NameToValue:
            self.__accNum = NameToValue[GParentSettlement['clientID']]
        else:
            print('账户没有正确解析')
            self.__accNum = 'NULL'
        self.__date = NameToValue[GParentSettlement['Date']]
        self.__strHeader = '结算会员: ' + self.__accNum + '       结算会员名称:中信期货(0018)' + '      结算日期:' + self.__date
        return


    def toNum(self, pdata):
        global GPaDec
        # print(pdata.dtypes)
        pCols = pdata.columns
        for eachCol in pCols:
            if eachCol in GPaDec:
                try:
                    pdata[eachCol] = pdata[eachCol].astype(float)
                except Exception as e:
                    print(pdata[eachCol])
                    print(pdata[eachCol].dtype)
                # pdata[eachCol] = pdata[eachCol].astype(Decimal)
                #print(pdata[eachCol])
        #print(pdata.dtypes)

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
        global pBillDicts
        global pBillKeys
        self.keyWords = pBillKeys
        # 资金状况  币种：人民币  Account Summary  Currency：CNY
        #pBillDicts = {"SettlementStatement" : "交易结算单(盯市)", "AccountSummary" : "资金状况", "TransactionRecord" : "成交记录", "PositionClosed" : "平仓明细", "PositionsDetail" : "持仓明细", "Positions" : "持仓汇总"}
        self.operator = {pBillDicts['SettlementStatement']: self.setSettlementTxt, pBillDicts['AccountSummary']: self.setAccountTxt, pBillDicts['PositionsDetail']: self.setPositionsDetailTxt,
                pBillDicts['Positions']: self.setPositionsTxt, pBillDicts['TransactionRecord']: self.setTransactionTxt, pBillDicts['PositionClosed']: self.setRealizeTxt}


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

        if self.accountTxt:
            self.accountDict = self.washAccount(self.accountTxt)

        if self.settlementTxt:
            self.setAccNdate(self.settlementTxt)

        if self.positionsTxt:
            self.positionList = self.washPosition(self.positionsTxt)
            self.toNum(self.positionList)
        # if self.transactionTxt:
        #     self.transList = self.washTrantransaction(self.transactionTxt)
        #     self.toNum(self.transList)
        return

    # 将账户信息转化为字典类型
    def get_settle(self):
        mod_settle = {key.split()[0]: self.settlementDict[key]  for key in self.settlementDict}
        return mod_settle

    # 将账户信息，及详细信息字典类型的key转换为数据库表字段，并且转换数据类型，以便插入数据库
    def get_accsum(self):
        # {'日期 Date': '20170407', '客户号 Client ID': '100109999', '客户名称 Client Name': '中证资本管理（深圳）有限公司'}
        key_header = {'多头期权市值': 'MARKET_VALUE_LONG', '币种': 'CURRENCY_CODE', '货币质出': 'FX_REDEMPTION', '质押变化金额': 'CHG_IN_PLEDGE_AMT',
             '客户权益': 'CLIENT_EQUITY', '期末结存': 'BALANCE_C_F', '行权手续费': 'EXERCISE_FEE', '基础保证金': 'INITIAL_MARGIN',
             '货币质入': 'NEW_FX_PLEDGE', '质 押 金': 'PLEDGE_AMOUNT', '货币质押保证金占用': 'FX_PLEDGE_OCC',
             '空头期权市值': 'MARKET_VALUE_SHORT', '交割保证金': 'DELIVERY_MARGIN', '客户号': 'CLIENT_ID', '期初结存': 'BALANCE_B_F',
             '制表时间': 'CREATION_DATE', '日期': 'DATE_VALUE', '权利金收入': 'PREMIUM_RECEIVED', '客户名称': 'CLIENT_NAME',
             '风 险 度': 'RISK_DEGREE', '期权执行盈亏': 'EXERCISE_P_L', '应追加资金': 'MARGIN_CALL', '市值权益': 'MARKET_VALUE_EQUITY',
             '可用资金': 'FUND_AVAIL', '权利金支出': 'PREMIUM_PAID', '平仓盈亏': 'REALIZED_P_L', '持仓盯市盈亏': 'MTM_P_L',
             '保证金占用': 'MARGIN_OCCUPIED', '出 入 金': 'DEPOSIT_WITHDRAWAL', '手 续 费': 'COMMISSION', '交割手续费': 'DELIVERY_FEE'}
        str_type = ['client_name', 'client_id']
        date_type = ['date_value']

        mod_acc_dict = {key_header[key].lower(): self.accountDict[key] for key in self.accountDict}
        mod_settle = self.get_settle()
        mod_settle_1 = {key_header[key].lower(): mod_settle[key] for key in mod_settle}
        mod_acc_dict.update(mod_settle_1)
        for key in mod_acc_dict:
            # print(key, " ",  mod_acc_dict[key])
            if key in str_type:
                continue
            elif key in date_type:
                mod_acc_dict[key] = datetime.datetime.strptime(mod_acc_dict[key], "%Y%m%d")
            else:
                mod_acc_dict[key] = float(mod_acc_dict[key])
        return mod_acc_dict


class BillList(object):
    def __init__(self, path):
        self.textList = self.readAll(path)
        self.getBills()
        #self.txtGroup =
        return


    def getBills(self):

        return
    def getAcc(self, file_name):


        return

    def readAll(self, path):
        files = os.listdir(path)
        textContainer = []
        txtDict = {}
        for file in files:
            # txt_key = file
            file = path + '/' + file
            if not os.path.isfile(file):
                continue
            txt_key = self.getAcc(file)
            if file.endswith('txt'):
                encoding = chardet.detect(open(file, 'rb').readline())['encoding']
                with open(file, 'rb') as srcFile:

                    content = srcFile.read().decode(encoding)
                    text = content.split('\n')

            txtDict[txt_key] = text
            text = []

        return txtDict
class ParentBillList(BillList):

    def getPBillKeys(self):
        keys = []
        keys = self.__BillDict.keys()
        return keys
    def getPBill(self, key):

        pBill = self.__BillDict[key]
        return pBill

    def getAllPBills(self):
        return self.__BillDict.values()

    def getBills(self):
        self.__BillDict = {}
        for acc in self.textList:
            temp_txt = self.textList[acc]
            pBill = ParentBill(temp_txt)
            self.__BillDict[acc] = pBill
        return


    def getAcc(self, file_name):
        ans = ""
        phanzi = re.compile(u'[0-9]+');
        keys = phanzi.findall(file_name)
        ans = keys[-1]
        return ans




def main():

    path = './txt'
    subAccPath = './subacc';
    parentAccPath = "./bill"
    presentCTPBill = './presentCTPBill'
    #subAccPath = presentCTPBill
    data_list = []
    #textContainer = Bill.readBill_All(parentAccPath)
    pbills = ParentBillList(parentAccPath)
    bills = list(pbills.getAllPBills())
    for a_bill in bills:
        print(a_bill.settlementDict)
        print(a_bill.get_accsum())
        print(a_bill.settlementDict)
        data_list.append(a_bill.get_accsum())

    print(bills)


    print("\n程序运行正常结束！")


    os.system("pause")
    return

if __name__=="__main__":
    main()
