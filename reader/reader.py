#-*- code: utf-8 -*-

import pandas as pd
import os

class FileReader(object):
    @staticmethod
    def readall(path):
        files = os.listdir(path)
        excel_files = []
        for file in files:
            file = path + '/' + file
            if not os.path.isfile(file):
                continue
            if file.endswith('xlsx') or file.endswith('xls') or file.endswith('csv'):
                excel_files.append(file)

        return excel_files

    @staticmethod
    def readfirst(path):
        files = FileReader.readall(path)
        ans = files[0]
        return ans

    @classmethod
    def test2(cls):
       print('class method')

    @staticmethod
    def read(path='../excel/ziguanjihua.xlsx', header=None):
        config_path = path
        df_header = header

        df = pd.read_excel(config_path, header=None, skiprows=[0])
        df.columns = df.columns.map(lambda x: df_header[int(x)].strip())
        # print(df)
        # last_modified_date
        df = df.drop(['last_modified_date'], axis=1)
        # df = df[1:]
        # print(df)

        return df



    @staticmethod
    def read_vertical(path='../excel/vertical.xlsx', header=None):
        config_path = path
        df_header = header

        df = pd.read_excel(config_path, header=None)
        # print(df)
        df1 = df.iloc[:, 1:]
        # print(df1)
        df = df1.T
        # print(df)
        df.columns = df.columns.map(lambda x: df_header[int(x)].strip())

        df = df.drop(['last_modified_date'], axis=1)

        # print(df)
        df.to_csv('./testcsv.csv')
        return df




class SecFileReader(FileReader):
    G_MANAGEMENT_WARRANT = {'风险事件': 'RISK_EVENTS', '汇率(兑人民币)': 'EXCHANGE_RATE', '标的最新价格': 'LATEST_PRICE',
                            '投资机构代码': 'MANAGE_COMPANY_CODE', '币种': 'CURRENCY_CODE', '仓储费元/吨*每天': 'WAREHOUSING_COSTS',
                            '当日是否追保': 'IF_DAY_AFTER', '增值税税率': 'VAT', '资金年化收益率': 'MONEY_ANNUALIZED_YIELD',
                            '当日卖出方式': 'DAY_SELLING_WAY', '单一品种剩余额度': 'SINGLE_SPECIES_REMAINING',
                            '衍生品账户编号': 'DERIVATIVES_ACCOUNT_NUMBER', '场内外类型': 'ON_EXCHANGE_FLAG',
                            '主力合约历史1年数据(不需存，算VAR用）': 'MAIN_CONTRACT_1_YEAR',
                            '主力合约历史1年数据(不需存，算VaR用）': 'MAIN_CONTRACT_1_YEAR',
                            '主力合约历史1年数据': 'MAIN_CONTRACT_1_YEAR',
                            '合约名义价值': 'NOTIONAL_VALUE',
                            '交易结束日期': 'TRANSACTION_END_DATE', '单一品种是否超限': 'SINGLE_SPECIES_OVERRUN',
                            '权利金（若为买入期权）': 'PREMIUM_PRICE', '单一交易对手限额': 'SINGLE_COUNTERPARTY_LIMITS',
                            '单一交易对手剩余额度': 'SINGLE_COUNTERPARTY_REMAINING', '当日卖出数量': 'DAY_SELL_QUANTITY',
                            '总收益': 'TOTAL_REVENUE', '最晚回购日': 'LATEST_REPO_DATE', '交易市场': 'EXCHANGE_TYPE',
                            '其他成本': 'OTHER_COSTS', '累计回购金额': 'CML_REPURCHASE_PRICE', '当日回购金额': 'REPURCHASE_PRICE',
                            '业务资金占用率': 'CAPITAL_OCCUPANCY_RATE', '主力合约最新价': 'MAIN_CONTRACT_LPRICE',
                            '交易对手': 'COUNTER_PARTY',
                            '增值税调整项': 'ADJUST_VAT', '合同编号': 'CONTRACT_NO', '当日卖出价格': 'SELLING_PRICE',
                            '初始基准价格': 'INITIAL_BENCHMARK_PRICE', '业务出入金对象及事项': 'BUSINESS_DISCREPANCY',
                            '账面价值（若为买入CDS）': 'BOOK_VALUE', '业务申请金额': 'BUSINESS_APPLICATION',
                            '交易成立日期': 'TRANSACTION_DATE',
                            '平仓价': 'UNWIND_PRICE', '合约乘数': 'CONTRACT_MULTIPLIER', 'Delta金额（若为卖出股票期权）': 'DELTA_VALUE',
                            'DELTA金额（若为卖出股票期权）': 'DELTA_VALUE',
                            '买入货款总值': 'TOTAL_COST_BUY', '标的资产': 'HOLD_ASSET', '持仓日期(数据日期)': 'REPORT_DATE',
                            '交易对手证件类别': 'CP_CRED_TYPE', '单一品种限额': 'SINGLE_STRAIN_LIMIT', '交易对手内部评级': 'CP_INTER_RATING',
                            '主力合约': 'THE_MAIN_CONTRACT', '卖出货款总值': 'SELL_TOTAL', '仓储天数': 'STORAGE_DAYS',
                            '压力测试损失金额（若为卖出场外期权）': 'PRESS_LOSS', '当日回购数量': 'NUMBER_REPURCHASE',
                            '标的手数': 'TARGET_NUMBER_HAND',
                            '当日追保金额': 'DAY_INSURED_AMOUNT', '是否为集团内部交易': 'GROUP_INTER_TRANS_FLAG',
                            '交易对手资金余额': 'COUNTERPARTY_BALANCES', '处置价': 'DIS_PRICE', '交易对手证件号码': 'CP_CRED_ID',
                            '衍生品业务类型': 'DERIVATIVES_TYPE', '资金成本': 'THE_COST_OF_CAPITAL', '最新基准价格': 'BENCHMARK_LPRICE',
                            '集团内部交易对手（如有）': 'GROUP_COUNTER_PARTY', '集团内部交易金额（如有）': 'GROUP_COUNTER_VALUE',
                            '资金成本(年化)': 'ANNUAL_COST_CAPITAL', '数据更新时间': 'LAST_MODIFIED_DATE',
                            '累计回购数量': 'CML_NUMBER_REPURCHASE', '预警线': 'LINE_OF', '标的数量': 'MARK_THE_NUMBER',
                            '质押率': 'LOAN_RATIO', '融出资金余额': 'RONGCHU_BALANCES', '仓单市值': 'VALUE_RECEIPT',
                            '业务类型': 'BUSINESS_TYPES', '是否超过单一交易对手限额': 'SIN_COUNTER_LIMITS_EXCEED',
                            '实际融出金额': 'ACTUAL_AMOUNT_RONGCHU', '累计追保金额': 'CUMULATIVE_AMOUNT',
                            '交割结算价': 'DELIVERY_SETTLED',
                            '升贴水': 'PREMIUMS', '处置线': 'DISPOSAL_LINE', '业务出入金金额': 'AMOUNT_IN_OUT',
                            '追保线（预警价）': 'CONFIRMED_LINE',
                            '业务使用资金': 'BUSINESS_USE_MONEY', '剩余未回购数量': 'REMAINING_NOT_REPURCHASE',
                            '最早回购日': 'EARLIEST_REPO',
                            # 增加字段是否约定购回 2017-10-12
                            '是否约定购回': 'REDEEM',
                            }
    G_OTC_DERIVATIVES = {'场外衍生品了结方式': 'OTC_DERIVATIVES_SETTLEMENT_WAY', 'MID-VOL': 'MID_VOL', 'Delta': 'DELTA',
                         '交易确认书编号（平仓）': 'TRANSACTION_CONFIRM_POSITIONS', '交易结束日期': 'TRANSACTION_END_DATE',
                         '合同编号': 'CONTRACT_NO',
                         '总收益': 'TOTAL_REVENUE', '期权最后观察日': 'OPTIONS_LAST_OBSERVATION', 'Theta': 'THETA',
                         '交易对手': 'COUNTER_PARTY',
                         '期权类型': 'THE_OPTION_TYPE', '交易对手证件号码': 'CP_CRED_ID', '追保线': 'CONFIRMED_LINE',
                         '标的手数': 'TARGET_NUMBER_HAND',
                         '利率': 'THE_INTEREST_RATE', '期权交易方向': 'OPTIONS_TRADING_DIRECTION',
                         '集团内部交易对手（如有）': 'GROUP_COUNTER_PARTY',
                         'Gamma': 'GAMMA', 'Delta金额（若为卖出股票期权）': 'DELTA_VALUE', '累计追保金额': 'CUMULATIVE_AMOUNT',
                         '币种': 'CURRENCY_CODE',
                         '账面价值（若为买入CDS）': 'BOOK_VALUE', '场外衍生品项目类别': 'OTC_DERIVATIVES_CATEGORIES',
                         '压力测试损失金额（若为卖出场外期权）': 'PRESS_LOSS',
                         '衍生品账户编号': 'DERIVATIVES_ACCOUNT_NUMBER', '当日是否追保': 'IF_DAY_AFTER', '交易市场': 'EXCHANGE_TYPE',
                         '业务类型': 'BUSINESS_TYPES', '业务出入金金额': 'AMOUN_IN_OUT', '业务出入金对象及事项': 'BUSINESS_DISCREPANCY',
                         '行权价': 'PRICE',
                         '业务使用资金': 'BUSINESS_USE_MONEY', '交易确认书编号（开仓）': 'TRANSACTION_CONFIRM_OPEN',
                         '交易对手资金余额': 'COUNTERPARTY_BALANCES',
                         '持仓日期(数据日期)': 'REPORT_DATE', '交易对手内部评级': 'CP_INTER_RATING',
                         '集团内部交易金额（如有）': 'GROUP_COUNTER_VALUE',
                         '是否为集团内部交易': 'GROUP_INTER_TRANS_FLAG', '定价波动率(TRADE-VOL)': 'PRICE_VOLATILITY_TRADE_VOL',
                         '交易对手授信金额': 'COUNTERPARTY_CREDIT_AMOUNT', '业务申请金额': 'BUSINESS_APPLICATION',
                         '权利金（若为买入期权）': 'PREMIUM_PRICE',
                         '当日追保金额': 'DAY_INSURED_AMOUNT', '业务资金占用率': 'CAPITAL_OCCUPANCY_RATE', 'DIVIDENT': 'DIVIDENT',
                         '场内外类型': 'ON_EXCHANGE_FLAG', 'CALL/PUT': 'CALL_PUT', 'Call/Put': 'CALL_PUT', 'Rho': 'RHO', '风险事件': 'RISK_EVENTS',
                         '场外衍生品平仓/行权金额': 'OTC_DERIV_POSITIONS_EXERCISE', '标的最新价格': 'LATEST_PRICE', '标的资产': 'HOLD_ASSET',
                         '数据更新时间': 'LAST_MODIFIED_DATE', '标的数量': 'MARK_THE_NUMBER',
                         '计算VAR的基准合约': 'BENCHMARK_CALCULATE_VAR',
                         '汇率(兑人民币)': 'EXCHANGE_RATE', '投资机构代码': 'MANAGE_COMPANY_CODE',
                         '权利金收入（若为卖出期权）': 'ROYALTY_INCOME',
                         '当日波动率(CURRENT-VOL)': 'THE_VOLATILITY',
                         '场外基准合约历史531个收盘价(不需存，算VAR用）': 'OTC_BENCHMARK_HISTORY_531_CP',
                         '交易对手证件类别': 'CP_CRED_TYPE', '合约名义价值': 'NOTIONAL_VALUE', '证券账户编号': 'SECURITIES_ACCOUNT_NUMBER',
                         'Vega': 'VEGA',
                         'CARRYCOST': 'CARRYCOST', '交易成立日期': 'TRANSACTION_DATE', '衍生品业务类型': 'DERIVATIVES_TYPE'}
    G_BASIS_PRICING = {'质押率': 'LOAN_RATIO', '资金成本(年化)': 'ANNUAL_COST_CAPITAL', '标的数量': 'MARK_THE_NUMBER',
                       '其他成本': 'OTHER_COSTS',
                       '压力测试损失金额（若为卖出场外期权）': 'PRESS_LOSS', '仓储天数': 'STORAGE_DAYS',
                       '集团内部交易金额（如有）': 'GROUP_COUNTER_VALUE',
                       '衍生品账户编号': 'DERIVATIVES_ACCOUNT_NUMBER', '增值税调整项': 'ADJUST_VAT', '最晚回购日': 'LATEST_REPO_DATE',
                       '币种': 'CURRENCY_CODE', '标的资产': 'HOLD_ASSET', '交易对手证件类别': 'CP_CRED_TYPE',
                       '衍生品业务类型': 'DERIVATIVES_TYPE',
                       '资金年化收益率': 'MONEY_ANNUALIZED_YIELD', '汇率(兑人民币)': 'EXCHANGE_RATE', '合同编号': 'CONTRACT_NO',
                       '平仓价': 'UNWIND_PRICE',
                       '当日回购金额': 'REPURCHASE_PRICE', '当日回购数量': 'NUMBER_REPURCHASE', '交割结算价': 'DELIVERY_SETTLED',
                       '标的手数': 'TARGET_NUMBER_HAND',

                       'Delta金额（若为卖出股票期权）': 'DELTA_VALUE',

                       '当日卖出价格': 'SELLING_PRICE',
                       '交易结束日期': 'TRANSACTION_END_DATE', '开仓价': 'MAKE_THE_WAREHOUSE_PRICE', '当日卖出方式': 'DAY_SELLING_WAY',
                       '合约名义价值': 'NOTIONAL_VALUE', '持仓日期(数据日期)': 'REPORT_DATE', '账面价值（若为买入CDS）': 'BOOK_VALUE',
                       '业务资金占用率': 'CAPITAL_OCCUPANCY_RATE', '最早回购日': 'EARLIEST_REPO', '升贴水': 'PREMIUMS',
                       '实际借出金额': 'THE_ACTUAL_AMOUNT_BORROWED', '当日买入价格': 'THE_BUYING_PRICE', '卖出货款总值': 'SELL_TOTAL',
                       '场内外类型': 'ON_EXCHANGE_FLAG', '业务申请金额': 'BUSINESS_APPLICATION', '业务使用资金': 'BUSINESS_USE_MONEY',
                       '业务出入金对象及事项': 'BUSINESS_DISCREPANCY', '剩余未回购数量': 'REMAINING_NOT_REPURCHASE',
                       '交易对手内部评级': 'CP_INTER_RATING',
                       '仓储费元/吨*每天': 'WAREHOUSING_COSTS', '交易对手': 'COUNTER_PARTY', '交易市场': 'EXCHANGE_TYPE',
                       '交易对手证件号码': 'CP_CRED_ID',
                       '集团内部交易对手（如有）': 'GROUP_COUNTER_PARTY', '标的最新价格': 'LATEST_PRICE', '风险事件': 'RISK_EVENTS',
                       '增值税税率': 'VAT',
                       '权利金（若为买入期权）': 'PREMIUM_PRICE', '投资机构代码': 'MANAGE_COMPANY_CODE', '业务类型': 'BUSINESS_TYPES',
                       '数据更新时间': 'LAST_MODIFIED_DATE', '交易对手资金余额': 'COUNTERPARTY_BALANCES',
                       '当日卖出数量': 'DAY_SELL_QUANTITY',
                       '当日买入方式': 'ON_THE_WAY_OF_BUYING', '交易成立日期': 'TRANSACTION_DATE', '总收益': 'TOTAL_REVENUE',
                       '是否为集团内部交易': 'GROUP_INTER_TRANS_FLAG', '买入货款总值': 'TOTAL_COST_BUY', '业务出入金金额': 'AMOUN_IN_OUT',
                       '资金成本': 'THE_COST_OF_CAPITAL'}
    G_BASIS_OPTION = {'标的资产': 'HOLD_ASSET', '交易市场': 'EXCHANGE_TYPE', '交易对手证件类别': 'CP_CRED_TYPE',
                      '数据更新时间': 'LAST_MODIFIED_DATE', '交易对手内部评级': 'CP_INTER_RATING',
                      '是否为集团内部交易': 'GROUP_INTER_TRANS_FLAG', '合约名义价值': 'NOTIONAL_VALUE', '权利金（若为买入期权）': 'PREMIUM_PRICE',
                      '集团内部交易对手（如有）': 'GROUP_COUNTER_PARTY', 'Theta': 'THETA', 'Gamma': 'GAMMA',
                      # 'DELTA金额（若为卖出股票期权）': 'DELTA_VALUE',
                      'Delta金额（若为卖出股票期权）': 'DELTA_VALUE',
                      '风险事件': 'RISK_EVENTS', 'Vega': 'VEGA',
                      '50ETF历史282天开盘和收盘价数据(不需存，算VaR用）': 'ETF_50_HISTORY_282_DAY_OP_CP',
                      '50ETF历史282天开盘和收盘价数据(不需存，算VAR用）': 'ETF_50_HISTORY_282_DAY_OP_CP',
                      '业务出入金对象及事项': 'BUSINESS_DISCREPANCY', '交易对手证件号码': 'CP_CRED_ID',
                      '持仓日期(数据日期)': 'REPORT_DATE',
                      '币种': 'CURRENCY_CODE',
                      '压力测试损失金额（若为卖出场外期权）': 'PRESS_LOSS', '衍生品账户编号': 'DERIVATIVES_ACCOUNT_NUMBER',
                      '账面价值（若为买入CDS）': 'BOOK_VALUE',
                      '业务使用资金': 'BUSINESS_USE_MONEY', '业务资金占用率': 'CAPITAL_OCCUPANCY_RATE',
                      '场内外类型': 'ON_EXCHANGE_FLAG',
                      '业务类型': 'BUSINESS_TYPES',
                      'Rho': 'RHO',
                      '交易对手': 'COUNTER_PARTY', 'Delta': 'DELTA',
                      '汇率(兑人民币)': 'EXCHANGE_RATE',
                      '业务出入金金额': 'AMOUN_IN_OUT', '业务申请金额': 'BUSINESS_APPLICATION', '衍生品业务类型': 'DERIVATIVES_TYPE',
                      '投资机构代码': 'MANAGE_COMPANY_CODE', '集团内部交易金额（如有）': 'GROUP_COUNTER_VALUE'}

    G_OPTION_DEALER = {'GAMMA': 'GAMMA', '有效连续报价比率': 'EFFCT_CONTINUOUS_PRICE_RATIO', 'Rho': 'RHO',
                       '交易对手证件号码': 'CP_CRED_ID',
                       '交易对手': 'COUNTER_PARTY', 'VEGA': 'VEGA', '主力合约最新价': 'MAIN_CONTRACT_LPRICE',
                       'Delta金额（若为卖出股票期权）': 'DELTA_VALUE',
                       'DELTA': 'DELTA', '合约名义价值': 'NOTIONAL_VALUE', '投资机构代码': 'MANAGE_COMPANY_CODE',
                       '汇率(兑人民币)': 'EXCHANGE_RATE',
                       '数据更新时间': 'LAST_MODIFIED_DATE', '是否为集团内部交易': 'GROUP_INTER_TRANS_FLAG', '币种': 'CURRENCY_CODE',
                       '标的最新价格': 'LATEST_PRICE', '压力测试损失金额（若为卖出场外期权）': 'PRESS_LOSS', '账面价值（若为买入CDS）': 'BOOK_VALUE',
                       'THETA': 'THETA',
                       '业务申请金额': 'BUSINESS_APPLICATION', '业务出入金金额': 'AMOUN_IN_OUT', '业务类型': 'BUSINESS_TYPES',
                       '风险事件': 'RISK_EVENTS',
                       '主力合约': 'THE_MAIN_CONTRACT', '有效回应询价比率': 'EFFECTIVE_RESPONSE_INQUIRY',
                       '业务资金占用率': 'CAPITAL_OCCUPANCY_RATE',
                       '持仓日期(数据日期)': 'REPORT_DATE', '集团内部交易金额（如有）': 'GROUP_COUNTER_VALUE',
                       '衍生品业务类型': 'DERIVATIVES_TYPE',
                       '主力合约历史282天数据(不需存，算做市VAR）': 'MAIN_CONTRACT_HISTORY_282_DAY',
                       '主力合约历史21天数据(不需存，算20日波动率）': 'MAIN_CONTRACT_HISTORY_21_DAYS', '权利金（若为买入期权）': 'PREMIUM_PRICE',
                       '集团内部交易对手（如有）': 'GROUP_COUNTER_PARTY', '交易对手内部评级': 'CP_INTER_RATING',
                       '衍生品账户编号': 'DERIVATIVES_ACCOUNT_NUMBER',
                       '业务使用资金': 'BUSINESS_USE_MONEY', '业务出入金对象及事项': 'BUSINESS_DISCREPANCY', '交易市场': 'EXCHANGE_TYPE',
                       '当前期货/期权合约收盘价、剩余日期、隐含波动率': 'FUT_DEVBY_OPT_CP_RD_IV', '交易对手证件类别': 'CP_CRED_TYPE',
                       '场内外类型': 'ON_EXCHANGE_FLAG',
                       '隐含波动率': 'IMPLIED_VOLATILITY'}

    # 持仓excel中文和表字段映射关系, 根据9月9=8日需求，增加五个字段
    POSITION = {'投资者名称': 'CLIENT_NAME', '交易所编码': 'EXCHANGE_ID', '卖出成交量': 'SELL_VOLUME',
                # 停止使用该字段，保持与衍生品表字段相同
                # '标的价格': 'TARGET_PRICE',
                '标的价格': 'BOOK_VALUE',
                '投保': 'S_H',
                '平仓盯市盈亏': 'REALIZED_MTM_P_L', '投资者代码': 'CLIENT_ID', '卖变化': 'SELL_CHANGE', '交易所': 'EXCHANGE',
                '平仓浮动盈亏': 'REALIZED_ACCUM_P_L', '保证金': 'MARGIN', '买持量': 'LONG_POS', '空头期权市值': 'MARKET_VALUE_SHORT',
                '卖持量': 'SHORT_POS', '产品': 'PRODUCT', '交易所保证金': 'EXCHANGE_MARGIN', '交易日': 'TRADING_DAY',
                '上交手续费': 'PAY_FEE',
                '卖均价': 'AVG_SELL_PRICE', '多头期权市值': 'MARKET_VALUE_LONG', '交割期': 'DELIVERY_DAY', '买入成交量': 'BUY_VOLUME',
                '可用资金': 'FUND_AVAIL', '买变化': 'BUY_CHANGE', '币种代码': 'CURRENCY',
                # 停止使用该字段，保持与衍生品表字段相同
                # '合约': 'INSTRUMENT',
                '合约': 'HOLD_ASSET',
                '买均价': 'AVG_BUY_PRICE',
                '结算价': 'SETTLEMENT_PRICE', '净持仓量': 'NET_POSITION', '盯市盈亏': 'MTM_P_L', '手续费': 'FEE', '浮动盈亏': 'ACCUM_P_L',
                '当日盈亏': 'DAY_P_L',
                '合约名义价值': 'NOTIONAL_VALUE',

                '压力测试损失金额（若为卖出场外期权）': 'PRESS_LOSS', '账面价值（若为买入CDS）': 'BOOK_VALUE',
                '权利金（若为买入期权）': 'PREMIUM_PRICE',
                'Delta金额（若为卖出股票期权）': 'DELTA_VALUE',
                'DELTA金额': 'DELTA_VALUE',
                '压力测试损失金额（若为卖出场外期权）': 'PRESS_LOSS',
                '账面价值（若为买入CDS）': 'BOOK_VALUE',
                '权利金（若为买入期权）': 'PREMIUM_PRICE',
                '合约名义价值': 'NOTIONAL_VALUE',
                # add on 2017-09-29, 增加两个字段，持仓手数，方向
                '买卖方向': 'DIRECTION',
                '持仓手数': 'LOTS',
                }

    # 添加etf持仓表中文和表字段映射关系， 2017-10-12
    ETFPOSITION = {'压力测试损失金额（若为卖出场外期权）': 'PRESS_LOSS', '交易所多头保证金': 'EXCHANGE_LONG_DEPOSIT', '投资者保证金': 'INVESTORS_MARGIN',
     '投资者多头保证金': 'INVESTORS_LONG_BOND', '昨结算价': 'SETTLEMENT_PRICE_YESTERDAY', '账面价值（若为买入CDS）': 'BOOK_VALUE',
     '交易所保证金': 'EXCHANGE_MARGIN', '合约编码': 'CONTRACT_CODE', '投资者名称': 'CLIENT_NAME',
     '期权多头持仓市值': 'OPTIONS_LONG_OPEN_MARKET_VALUE', '权利金（若为买入期权）': 'PREMIUM_PRICE', '期权子账户': 'CHILD_ACCOUNT_OPTIONS',
     '资金账号': 'CAPITAL_ACCOUNT', '组合合约多头持仓量': 'COMBINATION_LONG_HOLDINGS', '卖出成本': 'SELLING_COSTS',
     '合约数量乘数': 'CONTRACT_NUMBER_MULTIPLIER', '合约代码': 'HOLD_ASSET', '交易所空头保证金': 'SHORT_EXCHANGE_DEPOSIT',
     '证券账户': 'SECURITIES_ACCOUNT', '投资者空头保证金': 'INVESTORS_SHORT_MARGIN', '投机套保标志': 'SPECULATIVE_HEDGE_MARKS',
     '持仓手数': 'LOTS', '买卖方向': 'DIRECTION', '交易所': 'EXCHANGE', '义务仓持仓量': 'COMPULSORY_HOLDINGS',
     '结算价': 'SETTLEMENT_PRICE', '买入成本': 'PURCHASE_COST', '权利仓持仓量': 'POWER_HOLDINGS', '合约名义价值': 'NOTIONAL_VALUE',
     '投资者代码': 'CLIENT_ID', '持仓类型': 'POSITION_TYPE', '非组合空头持仓量': 'COMBINATION_SHORT_POSITIONS',
     '昨多头持仓量': 'LONG_HOLDINGS_YESTERDAY', '组合合约空头持仓量': 'PORTFOLIO_SHORT_POSITIONS',
     '期权空头持仓市值': 'OPTION_SHORT_POSITIONS', '非组合多头持仓量': 'COMBINATION_LONG_POSITIONS', '备兑标志': 'COVERED_LOGO',
     '标的证券编码': 'UNDERLYING_SECURITIES_CODE', 'Delta金额（若为卖出股票期权）': 'DELTA_VALUE',
     'DELTA金额（若为卖出股票期权）': 'DELTA_VALUE',
     'DELTA金额': 'DELTA_VALUE',
     '交易日': 'TRADING_DAY',
     '昨空头持仓量': 'YESTERDAY_SHORT_POSITIONS'}

    HEADER = {'G_MANAGEMENT_WARRANT': G_MANAGEMENT_WARRANT, 'G_OTC_DERIVATIVES': G_OTC_DERIVATIVES,
              'G_BASIS_PRICING': G_BASIS_PRICING, 'G_BASIS_OPTION': G_BASIS_OPTION,
              'G_OPTION_DEALER': G_OPTION_DEALER, 'POSITION': POSITION,
              'ETFPOSITION': ETFPOSITION,
              }

    TYPE_G_MANAGE_ASSET_INFO = {'LIQUIDATION_AMOUNT': 'float64', 'MANAGE_MODE': 'str', 'MANAGE_ASSET_VALUE': 'float64',
                                'PRODUCT_CODE': 'str',
                                'BUSINESS_TYPE': 'str', 'PRODUCT_TYPE': 'str', 'CF_PRODUCT_TYPE': 'str',
                                'MANAGE_COMPANY_CODE': 'str',
                                'REPORT_DATE': 'int', 'PRODUCT_NAME': 'str', 'CURRENT_NAV': 'float64',
                                'CF_PRODUCT_SUB_TYPE1': 'str',
                                'WINDING_UP_WARNING': 'float64', 'CF_PRODUCT_SUB_TYPE3': 'str',
                                'CREDIT_WARNING': 'float64',
                                'CURRENT_NAV_O32': 'float64', 'LAST_MODIFIED_DATE': 'object',
                                'CF_PRODUCT_SUB_TYPE2': 'str',
                                'MANAGE_ASSET_VALUE_O32': 'float64', 'WINDING_UP': 'str'}

    TYPE_G_CITIC_INVEST_INFO = {'EXCHANGE_RATE': 'float64', 'SECURITY_SUB_TYPE': 'str', 'REDEM_LIMIT_FLAG': 'str',
                                'GROUP_COUNTER_VALUE': 'float64', 'WINDING_UP_WARNING': 'float64',
                                'AMC_SECURITY_TYPE': 'str',
                                'DAILY_PURCHASE_ENTITY': 'str', 'CURRENT_NAV': 'float64',
                                'DAILY_REDEEM_AMOUNT': 'float64', 'BIUNIQUE_FLAG': 'str',
                                'DAILY_REDEEM_ENTITY': 'str', 'AMC_SECURITY_SUB_TYPE2': 'str',
                                'SECURITY_RISK_RANK': 'str',
                                'HOLD_MARKET_VALUE': 'float64', 'CURRENCY_CODE': 'str', 'LAST_MODIFIED_DATE': 'object',
                                'HOLD_COST_VALUE': 'float64', 'FREEZE_PLEDGE_VALUE': 'float64', 'SECURITY_NAME': 'str',
                                'ISSUER_ENTITY': 'str',
                                'DAILY_PURCHASE_AMOUNT': 'float64', 'SERVICE_TYPE': 'str', 'HOLD_AMOUNT': 'float64',
                                'INVEST_COMPANY_CODE': 'str',
                                'GRADED_FUND_FLAG': 'str', 'CREDIT_WARNING': 'float64', 'FREEZE_PLEDGE_FLAG': 'str',
                                'SECURITY_CODE': 'str',
                                'GROUP_COUNTER_PARTY': 'str', 'SECURITY_TYPE': 'str', 'REDEM_TERM': 'str',
                                'AMC_SECURITY_SUB_TYPE3': 'str',
                                'GROUP_INTER_TRANS_FLAG': 'str', 'AGREE_LOSS_FLAG': 'str',
                                'DAILY_PURCHASE_QUANTITY': 'int',
                                'HOLD_MARKET_VALUE_O32': 'float64', 'AMC_SECURITY_SUB_TYPE1': 'str',
                                'DAILY_REDEEM_QUANTITY': 'int',
                                'OPENING_FREQUENCY': 'str', 'CURRENT_NAV_O32': 'float64', 'GRADED_FUND_PRI': 'str',
                                'REPORT_DATE': 'int'}

    TYPE_G_OWN_INVEST_NEEQ = {'REDEM_TERM': 'str', 'DAILY_REDEEM_AMOUNT': 'float64', 'WINDING_UP_WARNING': 'float64',
                              'FREEZE_PLEDGE_FLAG': 'str', 'CREDIT_WARNING': 'float64', 'OPENING_FREQUENCY': 'str',
                              'EXCHANGE_RATE': 'float64',
                              'REDEM_LIMIT_FLAG': 'str', 'GRADED_FUND_FLAG': 'str', 'DAILY_PURCHASE_QUANTITY': 'int',
                              'SECURITY_RISK_RANK': 'str', 'DAILY_REDEEM_ENTITY': 'str', 'HOLD_AMOUNT': 'float64',
                              'SERVICE_TYPE': 'str',
                              'CURRENT_NAV': 'float64', 'SECURITY_CODE': 'str', 'GROUP_COUNTER_VALUE': 'float64',
                              'CURRENCY_CODE': 'str',
                              'DAILY_REDEEM_QUANTITY': 'int', 'AMC_SECURITY_SUB_TYPE1': 'str',
                              'INVEST_COMPANY_CODE': 'str',
                              'AMC_SECURITY_SUB_TYPE2': 'str', 'HOLD_MARKET_VALUE_O32': 'float64',
                              'GROUP_COUNTER_PARTY': 'str',
                              'SECURITY_SUB_TYPE': 'str', 'REPORT_DATE': 'int', 'GRADED_FUND_PRI': 'str',
                              'AMC_SECURITY_SUB_TYPE3': 'str',
                              'CURRENT_NAV_O32': 'float64', 'DAILY_PURCHASE_AMOUNT': 'float64',
                              'HOLD_COST_VALUE': 'float64',
                              'LAST_MODIFIED_DATE': 'object', 'BIUNIQUE_FLAG': 'str', 'SECURITY_NAME': 'str',
                              'AMC_SECURITY_TYPE': 'str',
                              'AGREE_LOSS_FLAG': 'str', 'GROUP_INTER_TRANS_FLAG': 'str', 'HOLD_MARKET_VALUE': 'float64',
                              'DAILY_PURCHASE_ENTITY': 'str', 'ISSUER_ENTITY': 'str', 'FREEZE_PLEDGE_VALUE': 'float64',
                              'SECURITY_TYPE': 'str'}

    TYPE_G_BASIS_PRICING = {'STORAGE_DAYS': 'float64', 'BOOK_VALUE': 'float64', 'NOTIONAL_VALUE': 'float64',
                            'MARK_THE_NUMBER': 'float64',
                            'COUNTERPARTY_BALANCES': 'float64', 'ON_THE_WAY_OF_BUYING': 'str',
                            'THE_BUYING_PRICE': 'float64',
                            'WAREHOUSING_COSTS': 'float64', 'MONEY_ANNUALIZED_YIELD': 'float64',
                            'TOTAL_COST_BUY': 'float64',
                            'COUNTER_PARTY': 'str', 'LAST_MODIFIED_DATE': 'object', 'EXCHANGE_RATE': 'float64',
                            'GROUP_COUNTER_VALUE': 'float64', 'EARLIEST_REPO': 'object', 'SELL_TOTAL': 'float64',
                            'OTHER_COSTS': 'float64',
                            'SELLING_PRICE': 'float64', 'TOTAL_REVENUE': 'float64', 'CURRENCY_CODE': 'str',
                            'CONTRACT_NO': 'str',
                            'LATEST_PRICE': 'float64', 'REMAINING_NOT_REPURCHASE': 'float64',
                            'DERIVATIVES_ACCOUNT_NUMBER': 'str',
                            'DAY_SELLING_WAY': 'str', 'BUSINESS_TYPES': 'str', 'DELTA_VALUE': 'float64',
                            'DERIVATIVES_TYPE': 'str',
                            'BUSINESS_APPLICATION': 'float64', 'CP_INTER_RATING': 'str',
                            'TRANSACTION_END_DATE': 'object',
                            'ANNUAL_COST_CAPITAL': 'float64', 'PRESS_LOSS': 'float64', 'CP_CRED_ID': 'str',
                            'AMOUN_IN_OUT': 'float64',
                            'DELIVERY_SETTLED': 'float64', 'ADJUST_VAT': 'float64', 'THE_COST_OF_CAPITAL': 'float64',
                            'MANAGE_COMPANY_CODE': 'str', 'LOAN_RATIO': 'float64', 'HOLD_ASSET': 'str',
                            'BUSINESS_USE_MONEY': 'float64',
                            'ON_EXCHANGE_FLAG': 'str', 'TARGET_NUMBER_HAND': 'float64', 'UNWIND_PRICE': 'float64',
                            'GROUP_INTER_TRANS_FLAG': 'str', 'THE_ACTUAL_AMOUNT_BORROWED': 'float64',
                            'PREMIUMS': 'float64',
                            'DAY_SELL_QUANTITY': 'float64', 'TRANSACTION_DATE': 'object',
                            'NUMBER_REPURCHASE': 'float64',
                            'PREMIUM_PRICE': 'float64', 'BUSINESS_DISCREPANCY': 'str', 'CP_CRED_TYPE': 'str',
                            'REPORT_DATE': 'int',
                            'VAT': 'float64', 'CAPITAL_OCCUPANCY_RATE': 'float64', 'RISK_EVENTS': 'str',
                            'EXCHANGE_TYPE': 'str',
                            'MAKE_THE_WAREHOUSE_PRICE': 'float64', 'GROUP_COUNTER_PARTY': 'str',
                            'REPURCHASE_PRICE': 'float64',
                            'LATEST_REPO_DATE': 'object'}

    TYPE_G_BASIS_OPTION = {'BUSINESS_USE_MONEY': 'float64', 'GROUP_COUNTER_PARTY': 'str', 'EXCHANGE_RATE': 'float64',
                           'BUSINESS_APPLICATION': 'float64', 'CAPITAL_OCCUPANCY_RATE': 'float64',
                           'MANAGE_COMPANY_CODE': 'str',
                           'DERIVATIVES_TYPE': 'str', 'HOLD_ASSET': 'str', 'LAST_MODIFIED_DATE': 'object',
                           'RISK_EVENTS': 'str',
                           'CP_INTER_RATING': 'str', 'ON_EXCHANGE_FLAG': 'str', 'VEGA': 'float64',
                           'AMOUN_IN_OUT': 'float64',
                           'DELTA_VALUE': 'float64', 'RHO': 'float64', 'ETF_50_HISTORY_282_DAY_OP_CP': 'float64',
                           'REPORT_DATE': 'int',
                           'EXCHANGE_TYPE': 'str', 'CP_CRED_ID': 'str', 'CURRENCY_CODE': 'str', 'DELTA': 'float64',
                           'BUSINESS_TYPES': 'str',
                           'BOOK_VALUE': 'float64', 'GROUP_COUNTER_VALUE': 'float64',
                           'DERIVATIVES_ACCOUNT_NUMBER': 'str',
                           'NOTIONAL_VALUE': 'float64', 'PRESS_LOSS': 'float64', 'THETA': 'float64',
                           'BUSINESS_DISCREPANCY': 'str',
                           'GAMMA': 'float64', 'PREMIUM_PRICE': 'float64', 'CP_CRED_TYPE': 'str',
                           'COUNTER_PARTY': 'str',
                           'GROUP_INTER_TRANS_FLAG': 'str'}

    TYPE_G_MANAGEMENT_WARRANT = {'REMAINING_NOT_REPURCHASE': 'float64', 'CP_CRED_ID': 'str', 'PREMIUMS': 'float64',
                                 'SELL_TOTAL': 'float64',
                                 'STORAGE_DAYS': 'float64', 'EXCHANGE_RATE': 'float64', 'UNWIND_PRICE': 'float64',
                                 'DERIVATIVES_ACCOUNT_NUMBER': 'str', 'LATEST_REPO_DATE': 'object',
                                 'LOAN_RATIO': 'float64',
                                 'SELLING_PRICE': 'float64', 'GROUP_COUNTER_PARTY': 'str',
                                 'CUMULATIVE_AMOUNT': 'float64',
                                 'SINGLE_COUNTERPARTY_LIMITS': 'float64', 'NUMBER_REPURCHASE': 'float64',
                                 'CP_INTER_RATING': 'str',
                                 'CAPITAL_OCCUPANCY_RATE': 'float64', 'DELTA_VALUE': 'float64', 'BUSINESS_TYPES': 'str',
                                 'LINE_OF': 'float64',
                                 'REPORT_DATE': 'int', 'BUSINESS_DISCREPANCY': 'str', 'THE_COST_OF_CAPITAL': 'float64',
                                 'DELIVERY_SETTLED': 'float64', 'THE_MAIN_CONTRACT': 'str', 'CP_CRED_TYPE': 'str',
                                 'ANNUAL_COST_CAPITAL': 'float64',
                                 'DERIVATIVES_TYPE': 'str', 'DAY_INSURED_AMOUNT': 'float64', 'TOTAL_REVENUE': 'float64',
                                 'CONTRACT_NO': 'str',
                                 'HOLD_ASSET': 'str', 'TRANSACTION_DATE': 'object', 'BOOK_VALUE': 'float64',
                                 'TOTAL_COST_BUY': 'float64',
                                 'SINGLE_SPECIES_OVERRUN': 'str', 'VAT': 'float64', 'CML_NUMBER_REPURCHASE': 'float64',
                                 'SIN_COUNTER_LIMITS_EXCEED': 'str', 'SINGLE_STRAIN_LIMIT': 'float64',
                                 'GROUP_COUNTER_VALUE': 'float64',
                                 'CONTRACT_MULTIPLIER': 'float64', 'EXCHANGE_TYPE': 'str',
                                 'GROUP_INTER_TRANS_FLAG': 'str',
                                 'LATEST_PRICE': 'float64', 'IF_DAY_AFTER': 'str', 'MAIN_CONTRACT_1_YEAR': 'float64',
                                 'VALUE_RECEIPT': 'float64',
                                 'ACTUAL_AMOUNT_RONGCHU': 'float64', 'EARLIEST_REPO': 'object', 'DIS_PRICE': 'float64',
                                 'RONGCHU_BALANCES': 'float64', 'AMOUNT_IN_OUT': 'float64',
                                 'DAY_SELL_QUANTITY': 'float64',
                                 'BENCHMARK_LPRICE': 'float64', 'MARK_THE_NUMBER': 'float64',
                                 'NOTIONAL_VALUE': 'float64',
                                 'DISPOSAL_LINE': 'float64', 'CML_REPURCHASE_PRICE': 'float64',
                                 'BUSINESS_USE_MONEY': 'float64',
                                 'TARGET_NUMBER_HAND': 'float64', 'TRANSACTION_END_DATE': 'object',
                                 'CONFIRMED_LINE': 'float64',
                                 'OTHER_COSTS': 'float64', 'MAIN_CONTRACT_LPRICE': 'float64', 'RISK_EVENTS': 'str',
                                 'ADJUST_VAT': 'float64',
                                 'BUSINESS_APPLICATION': 'float64', 'COUNTER_PARTY': 'str',
                                 'WAREHOUSING_COSTS': 'float64',
                                 'SINGLE_SPECIES_REMAINING': 'float64', 'CURRENCY_CODE': 'str',
                                 'REPURCHASE_PRICE': 'float64',
                                 'PREMIUM_PRICE': 'float64', 'ON_EXCHANGE_FLAG': 'str', 'LAST_MODIFIED_DATE': 'object',
                                 'SINGLE_COUNTERPARTY_REMAINING': 'float64', 'DAY_SELLING_WAY': 'str',
                                 'INITIAL_BENCHMARK_PRICE': 'float64',
                                 'MANAGE_COMPANY_CODE': 'str', 'COUNTERPARTY_BALANCES': 'float64',
                                 'MONEY_ANNUALIZED_YIELD': 'float64',
                                 'PRESS_LOSS': 'float64',
                                 # 增加字段是否约定购回 2017-10-12
                                 'REDEEM': 'int'

                                 }

    # 期权做市
    TYPE_G_OPTION_DEALER = {'PRESS_LOSS': 'float64', 'EXCHANGE_TYPE': 'str', 'RISK_EVENTS': 'str',
                            'BUSINESS_DISCREPANCY': 'str',
                            'GROUP_COUNTER_VALUE': 'float64', 'LATEST_PRICE': 'float64', 'MANAGE_COMPANY_CODE': 'str',
                            'MAIN_CONTRACT_LPRICE': 'float64', 'THETA': 'float64', 'REPORT_DATE': 'int',
                            'GAMMA': 'float64',
                            'CP_CRED_ID': 'str', 'EXCHANGE_RATE': 'float64', 'BOOK_VALUE': 'float64',
                            'NOTIONAL_VALUE': 'float64',
                            'BUSINESS_USE_MONEY': 'float64', 'DELTA': 'float64', 'FUT_DEVBY_OPT_CP_RD_IV': 'float64',
                            'GROUP_COUNTER_PARTY': 'str', 'LAST_MODIFIED_DATE': 'object', 'CP_INTER_RATING': 'str',
                            'CAPITAL_OCCUPANCY_RATE': 'float64', 'THE_MAIN_CONTRACT': 'str', 'COUNTER_PARTY': 'str',
                            'DERIVATIVES_ACCOUNT_NUMBER': 'str', 'VEGA': 'float64', 'PREMIUM_PRICE': 'float64',
                            'MAIN_CONTRACT_HISTORY_21_DAYS': 'float64', 'EFFECTIVE_RESPONSE_INQUIRY': 'float64',
                            'MAIN_CONTRACT_HISTORY_282_DAY': 'float64', 'DELTA_VALUE': 'float64', 'RHO': 'float64',
                            'ON_EXCHANGE_FLAG': 'str',
                            'BUSINESS_TYPES': 'str', 'CP_CRED_TYPE': 'str', 'GROUP_INTER_TRANS_FLAG': 'str',
                            'EFFCT_CONTINUOUS_PRICE_RATIO': 'float64', 'IMPLIED_VOLATILITY': 'float64',
                            'CURRENCY_CODE': 'str',
                            'DERIVATIVES_TYPE': 'str', 'BUSINESS_APPLICATION': 'float64', 'AMOUN_IN_OUT': 'float64'}
    # 场外衍生
    TYPE_G_OTC_DERIVATIVES = {'TARGET_NUMBER_HAND': 'int', 'OTC_DERIVATIVES_CATEGORIES': 'str',
                              'CP_INTER_RATING': 'str',
                              'TRANSACTION_END_DATE': 'object', 'COUNTERPARTY_CREDIT_AMOUNT': 'float64',
                              'IF_DAY_AFTER': 'str',
                              'EXCHANGE_RATE': 'float64', 'DAY_INSURED_AMOUNT': 'float64', 'BOOK_VALUE': 'float64',
                              'CP_CRED_TYPE': 'str',
                              'CONTRACT_NO': 'str', 'BUSINESS_APPLICATION': 'float64',
                              'TRANSACTION_CONFIRM_POSITIONS': 'str',
                              'THE_VOLATILITY': 'float64', 'LATEST_PRICE': 'float64', 'GAMMA': 'float64',
                              'PRICE_VOLATILITY_TRADE_VOL': 'float64', 'LAST_MODIFIED_DATE': 'object',
                              'OPTIONS_TRADING_DIRECTION': 'str',
                              'GROUP_COUNTER_VALUE': 'float64', 'TRANSACTION_DATE': 'object', 'CALL_PUT': 'str',
                              'CURRENCY_CODE': 'str',
                              'REPORT_DATE': 'int', 'RHO': 'float64', 'NOTIONAL_VALUE': 'float64',
                              'OTC_DERIV_POSITIONS_EXERCISE': 'float64',
                              'OTC_BENCHMARK_HISTORY_531_CP': 'float64', 'CAPITAL_OCCUPANCY_RATE': 'float64',
                              'ON_EXCHANGE_FLAG': 'str',
                              'MARK_THE_NUMBER': 'float64', 'ROYALTY_INCOME': 'float64', 'VEGA': 'float64',
                              'THE_INTEREST_RATE': 'float64',
                              'GROUP_COUNTER_PARTY': 'str', 'BUSINESS_TYPES': 'str', 'BENCHMARK_CALCULATE_VAR': 'str',
                              'MULTIPLIER': 'int',
                              'GROUP_INTER_TRANS_FLAG': 'str', 'HOLD_ASSET': 'str', 'THETA': 'float64',
                              'CP_CRED_ID': 'str',
                              'SECURITIES_ACCOUNT_NUMBER': 'str', 'AMOUN_IN_OUT': 'float64', 'MID_VOL': 'float64',
                              'TOTAL_REVENUE': 'float64',
                              'CONFIRMED_LINE': 'float64', 'PRESS_LOSS': 'float64', 'PREMIUM_PRICE': 'float64',
                              'OTC_DERIVATIVES_SETTLEMENT_WAY': 'str', 'BUSINESS_USE_MONEY': 'float64',
                              'COUNTER_PARTY': 'str',
                              'RISK_EVENTS': 'str', 'THE_OPTION_TYPE': 'str', 'OPTIONS_LAST_OBSERVATION': 'object',
                              'MANAGE_COMPANY_CODE': 'str',
                              'BUSINESS_DISCREPANCY': 'str', 'CARRYCOST': 'float64', 'DELTA_VALUE': 'float64',
                              'DIVIDENT': 'float64',
                              'DERIVATIVES_TYPE': 'str', 'CUMULATIVE_AMOUNT': 'float64', 'EXCHANGE_TYPE': 'str',
                              'TRANSACTION_CONFIRM_OPEN': 'str', 'DERIVATIVES_ACCOUNT_NUMBER': 'str',
                              'DELTA': 'float64',
                              'PRICE': 'float64',
                              'COUNTERPARTY_BALANCES': 'float64'}

    # 期货持仓表
    TYPE_POSITION = {'BOOK_VALUE': 'float64', 'FUND_AVAIL': 'float64', 'REALIZED_ACCUM_P_L': 'float64',
                     'DELIVERY_DAY': 'str',
                     'DELTA_VALUE': 'float64', 'SETTLEMENT_PRICE': 'float64', 'NET_POSITION': 'float64',
                     'TRADING_DAY': 'object',
                     'CLIENT_ID': 'str', 'SELL_VOLUME': 'float64', 'MTM_P_L': 'float64', 'NOTIONAL_VALUE': 'float64',
                     'AVG_SELL_PRICE': 'float64', 'MARGIN': 'float64', 'BUY_CHANGE': 'float64', 'CLIENT_NAME': 'str',
                     'FEE': 'float64',
                     'EXCHANGE_MARGIN': 'float64', 'SELL_CHANGE': 'float64', 'DAY_P_L': 'float64', 'CURRENCY': 'str',
                     'PRODUCT': 'str',
                     'LONG_POS': 'float64', 'ACCUM_P_L': 'float64', 'AVG_BUY_PRICE': 'float64',
                     'MARKET_VALUE_LONG': 'float64',
                     'BUY_VOLUME': 'float64', 'S_H': 'str', 'REALIZED_MTM_P_L': 'float64', 'TARGET_PRICE': 'float64',
                     'EXCHANGE_ID': 'str', 'MARKET_VALUE_SHORT': 'float64', 'EXCHANGE': 'str', 'INSTRUMENT': 'str',
                     'PAY_FEE': 'float64', 'SHORT_POS': 'float64', 'PRESS_LOSS': 'float64', 'PREMIUM_PRICE': 'float64',
                     # add on 2017-09-29, 增加两个字段，持仓手数，方向
                     'DIRECTION': 'str',
                     'LOTS': 'int',
                     'HOLD_ASSET': 'str'

                     }

    TYPE_ETFPOSITION = {'DELTA_VALUE': 'float64', 'CAPITAL_ACCOUNT': 'str', 'SETTLEMENT_PRICE_YESTERDAY': 'float64',
     'PURCHASE_COST': 'float64', 'COMBINATION_SHORT_POSITIONS': 'float64', 'BOOK_VALUE': 'float64',
     'LONG_HOLDINGS_YESTERDAY': 'float64', 'SECURITIES_ACCOUNT': 'str', 'NOTIONAL_VALUE': 'float64',
     'COVERED_LOGO': 'str', 'YESTERDAY_SHORT_POSITIONS': 'float64', 'OPTIONS_LONG_OPEN_MARKET_VALUE': 'float64',
     'INVESTORS_MARGIN': 'float64', 'PRESS_LOSS': 'float64', 'POWER_HOLDINGS': 'float64',
     'SHORT_EXCHANGE_DEPOSIT': 'float64', 'CLIENT_NAME': 'str', 'CHILD_ACCOUNT_OPTIONS': 'str', 'TRADING_DAY': 'object',
     'SETTLEMENT_PRICE': 'float64', 'PREMIUM_PRICE': 'float64', 'COMBINATION_LONG_HOLDINGS': 'float64',
     'CLIENT_ID': 'str', 'DIRECTION': 'str', 'REPORT_DATE': 'int', 'OPTION_SHORT_POSITIONS': 'float64',
     'INVESTORS_LONG_BOND': 'float64', 'CONTRACT_NUMBER_MULTIPLIER': 'int', 'SELLING_COSTS': 'float64',
     'CONTRACT_CODE': 'str',
     'HOLD_ASSET': 'str',
     'PORTFOLIO_SHORT_POSITIONS': 'float64', 'LOTS': 'int', 'EXCHANGE_LONG_DEPOSIT': 'float64',
     'COMPULSORY_HOLDINGS': 'float64', 'EXCHANGE_MARGIN': 'float64', 'COMBINATION_LONG_POSITIONS': 'float64',
     'INVESTORS_SHORT_MARGIN': 'float64', 'POSITION_TYPE': 'str', 'EXCHANGE': 'str',
     'UNDERLYING_SECURITIES_CODE': 'float64', 'SPECULATIVE_HEDGE_MARKS': 'str'}

    TYPE_HEADER = {'POSITION': TYPE_POSITION, 'G_OPTION_DEALER': TYPE_G_OPTION_DEALER,
                   'G_OTC_DERIVATIVES': TYPE_G_OTC_DERIVATIVES, 'G_MANAGEMENT_WARRANT': TYPE_G_MANAGEMENT_WARRANT,
                   'G_BASIS_OPTION': TYPE_G_BASIS_OPTION, 'G_BASIS_PRICING': TYPE_G_BASIS_PRICING,
                   'G_MANAGE_ASSET_INFO': TYPE_G_MANAGE_ASSET_INFO, 'G_CITIC_INVEST_INFO': TYPE_G_CITIC_INVEST_INFO,
                   'G_OWN_INVEST_NEEQ': TYPE_G_OWN_INVEST_NEEQ,
                   'ETFPOSITION' : TYPE_ETFPOSITION
                   }

    @staticmethod
    def read_vertical_v1(path='../excel/vertical.xlsx', header=None):
        config_path = path
        df_header = SecFileReader.HEADER[header.upper()]

        # test on 0919

        df = pd.read_excel(config_path, header=None, index_col=0)
        # print(df)
        df1 = df
        # df1 = df.iloc[:, 1:]
        # print(df1)
        df = df1.T
        # print(df)

        df.rename(columns=df_header, inplace=True)
        # print(df)
        # print(df.columns.tolist())
        # where 1 is the axis number (0 for rows and 1 for columns.)
        col_names = df.columns.tolist()
        # print(col_names)
        if 'last_modified_date' in col_names:
            df = df.drop(['last_modified_date'], axis=1)

        # df = df.dropna(subset=['last_modified_date'], axis=1)
        # 是否替换如何替换，有一些问题，如果字符串行替换为0，导入数据库会有问题
        # print(df)
        #  added on 2017-09-05
        # df = df.fillna(0)
        # df = df.replace('无', 0)
        df = df.replace('是', 1)
        df = df.replace('否', 0)
        df = df.replace('场内', 1)
        df = df.replace('场外', 0)
        # print(df)

        df.to_csv('./testcsv.csv')
        return df
    @staticmethod
    def pandas_set_type(df=None, header=None):
        t_types = SecFileReader.TYPE_HEADER[header.upper()]

        col_names = df.columns.tolist()
        for a_col in col_names:

            if a_col in t_types:
                a_type = t_types[a_col]
                if a_type is 'object':
                    continue

                else:
                    if a_type is 'str':
                        df[a_col] = df[a_col].replace('买入', '0')
                        df[a_col] = df[a_col].replace('卖出', '1')
                        df[a_col] = df[a_col].replace('是', '1')
                        df[a_col] = df[a_col].replace('否', '0')
                        df[a_col] = df[a_col].replace('场内', '1')
                        df[a_col] = df[a_col].replace('场外', '0')
                        df[a_col] = df[a_col].fillna('0')
                        df[a_col] = df[a_col].replace('无', '0')
                        df[a_col] = df[a_col].replace('人民币', 'RMB')
                        df[a_col] = df[a_col].replace('不适用', '0')

                    else:
                        df[a_col] = df[a_col].replace('买入', 0)
                        df[a_col] = df[a_col].replace('卖出', 1)
                        df[a_col] = df[a_col].replace('是', 1)
                        df[a_col] = df[a_col].replace('否', 0)
                        df[a_col] = df[a_col].replace('场内', 1)
                        df[a_col] = df[a_col].replace('场外', 0)
                        df[a_col] = df[a_col].fillna(0)
                        df[a_col] = df[a_col].replace('无', 0)
                        df[a_col] = df[a_col].replace('不适用', 0)
                    # print('col: ' + a_col)
                    # print('type: ' + a_type)
                    try:
                        df[a_col] = df[a_col].astype(a_type)
                    except Exception as e:
                        print(e)
                        print('col: ' + a_col)
                        print('type: ' + a_type)
            else:
                print('类型转换失败，没找到该字段' + '-'*10 + a_col + '-'*10)

        return df


    @staticmethod
    def read_vertical(path='../excel/vertical.xlsx', header=None):
        config_path = path
        df_header = SecFileReader.HEADER[header.upper()]

        df = pd.read_excel(config_path, header=None, index_col=0)
        # df.rename(columns=lambda x: x.strip(), inplace=True)

        # print(df)
        df1 = df


        df = df1.T
        old_col_names = df.columns.tolist()
        df.rename(columns=lambda x: x.strip(), inplace=True)
        df.rename(columns=df_header, inplace=True)
        col_names = df.columns.tolist()
        # 打印excel列名
        # print("打印excel列名:")
        # print(col_names)

        if 'last_modified_date' in col_names:
            df = df.drop(['last_modified_date'], axis=1)
        if 'LAST_MODIFIED_DATE' in col_names:
            df = df.drop(['LAST_MODIFIED_DATE'], axis=1)
        # for ta in SecFileReader.TYPE_HEADER:
        #     print(ta)
        if header in SecFileReader.TYPE_HEADER:
            df = SecFileReader.pandas_set_type(df, header)




        df.to_csv('./testcsv.csv')
        return df

    @staticmethod
    def read(path='../excel/ziguanjihua.xlsx', header=None):
        config_path = path
        df_header = SecFileReader.HEADER[header.upper()]
        # filename = os.path.basename(path)


        # modified on 2017-09-18 由于期货持仓excel格式改变不需要忽略第一第二行
        # df = pd.read_excel(config_path, header=0, skiprows=[0,1], skip_footer=1)

        if path.endswith('xls') or path.endswith('xlsx'):
            df = pd.read_excel(config_path, header=0, skip_footer=1)
        else:
            # df = pd.read_csv(config_path, header=0, skipfooter=1, encoding='utf-8', engine='python')
            df = pd.read_csv(config_path, header=0, encoding='utf-8')
            df = df.iloc[:-1, :]

        print(df.columns.tolist())
        df.rename(columns=lambda x: x.strip(), inplace=True)
        df.rename(columns=df_header, inplace=True)

        if header in SecFileReader.TYPE_HEADER:
            df = SecFileReader.pandas_set_type(df, header)

        # print(df.columns.tolist())
        df['TRADING_DAY'] = pd.to_datetime(df['TRADING_DAY'], format='%Y%m%d', errors='ignore')
        # print(df['TRADING_DAY'])
        # print(df)

        df.to_csv('./testcsv.csv')
        return df

def main():
    # m_header = 'report_date, business_type, manage_company_code, security_code, security_name, issuer_entity, exchange_type, stock_type, principle_investment, withdraw_investment, principle_hold_share, withdraw_share, hold_amount, hold_rate, hold_cost_value, hold_market_value, current_nav, neeq_component_index, earlier_neeq_component_index, daily_turnover, purchase_qty, purchase_price, neeq_com_index_w_purchase, selling_qty, selling_price, currency_code, market_make_flag, transfer_mode, restr_trade_flag, lockup_flag, suspend_flag, swaps_hedge_flag, margin_trade_flag, freeze_pledge_flag, freeze_pledge_value, group_inter_trans_flag, group_counter_party, group_counter_value, pe_ratio, pb_ratio, roe, net_profit_growth_rate, notes, last_modified_date'
    # headers = [key.strip() for key in m_header.split(',')]
    chicang_dir = '../input/中证资本期权做市/etf持仓'
    excel_path = FileReader.readfirst(chicang_dir)
    excel_path = '../input/中证资本期权做市/期货持仓/100109999投资者持仓查询_2017-10-10.xlsx.xlsx'
    # position_table = 'ETFPOSITION'
    position_table = 'POSITION'

    df_input = SecFileReader.read(path=excel_path, header=position_table)

    temp_df = df_input['TRADING_DAY']
    print(temp_df)
    # pd.to_datetime(x, format="%Y%m%d %H%M")
    #     temp_df = pd.to_datetime(df_input['TRADING_DAY'], format="%Y%m%d")

    # temp_df = pd.to_datetime(df_input['TRADING_DAY'])
    print(temp_df)
    # df_input = SecFileReader.read_vertical(path=excel_path, header=position_table)
    print(df_input)

    print("End Main threading")


if __name__ == '__main__':
    main()
