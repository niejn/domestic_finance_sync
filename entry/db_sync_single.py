#-*- code: utf-8 -*-
from bill.billRead import ParentBillList
from bill.childBillRead import ChildBillList
from config import init_config
from reader import reader
from db import insert
from db import dbmanager
from datacheck import datacheck
from bill import billRead
from reader.reader import FileReader
def store_pos_general(chicang_dir=None, hsfa_constr=None, trade_constr=None,  old_position_table='POSITION'):
    # chicang_dir = '../input/中证资本期权做市/期货持仓'

    chicang_excel_pathes = FileReader.readall(chicang_dir)
    # old_position_table = 'POSITION'
    # 不能全部删除持仓数据，只删除同一个持仓日期的持仓数据
    # delete_citicsf_tables(trade_constr, old_position_table, trade_constr)

    # 只在第一才导入时删除旧的同一个持仓日期的持仓数据，之后导入持仓不删除之前的持仓数据
    g_delete_old = True
    date = None
    for chicang_path in chicang_excel_pathes:
        date = store_position_v2(chicang_path, hsfa_constr, trade_constr, vertical=False, delete_old=g_delete_old,
                                 position_table=old_position_table
                                 )
        g_delete_old = False

    return date
def lead_in_qiquanzuoshi( hsfa_constr, trade_constr, vertical=True, sync_table_dict=None, del_same_dict=None):
    # # 中证资本期权做市--------------------------------------------------------------------------------------------------------
    excel_dir = '../input/中证资本期权做市/期权做市'
    asset_table = 'G_OPTION_DEALER'
    store_table_general(hsfa_constr, trade_constr, excel_dir, asset_table, vertical, sync_table_dict, del_same_dict)



    # # 读取金牛和中期期货对账单--------------------------------------------------------------------------------------------------------
    bill_dir_path = '../input/中证资本期权做市'
    asset_table = 'ACCOUNT_SUMMARY'
    ans = False
    ans = store_statement(asset_table, bill_dir_path, hsfa_constr, trade_constr)
    if ans:
        print('-----------导入本地数据库：金牛和中期期货对账单 成功-----------')

    # 读取期货持仓--------------------------------------------------------------------------------------------------------



    chicang_dir = '../input/中证资本期权做市/期货持仓'
    old_position_table = 'POSITION'
    date = store_pos_general(chicang_dir, hsfa_constr, trade_constr, old_position_table)


    # 增加50etf持仓导入------------------------------------------------------------------------------------------------------------
    # 读取期货持仓
    etf_chicang_dir = '../input/中证资本期权做市/etf持仓'
    etf_position_table = 'ETFPOSITION'
    etf_date = store_pos_general(etf_chicang_dir, hsfa_constr, trade_constr, etf_position_table)


    # 同步测试 中证资本期权做市 和 etf期权持仓， 组合为期货衍生品表------------------------------------------------------------------

    ans = sync_qqzs_general(date=etf_date, table_position='ETFPOSITION')
    if ans:
        print('-----------导入本地数据库：中证资本期权做市 和 etf期权持仓， 组合为期货衍生品表 成功-----------')
    # 同步测试 中证资本期权做市 和 期货持仓， 组合为期货衍生品表
    ans = sync_qqzs_general(date=date, table_position='POSITION')
    if ans:
        print('-----------导入本地数据库：中证资本期权做市 和 期货持仓， 组合为期货衍生品表 成功-----------')
    return


def store_table_general(hsfa_constr=None, trade_constr=None,excel_dir=None, asset_table=None, vertical=True,
                        sync_table_dict=None, del_same_dict=None):
    if sync_table_dict and del_same_dict:
        if asset_table.lower() in sync_table_dict and sync_table_dict[asset_table.lower()]:
            t_del = True
            if asset_table.lower() in del_same_dict:
                t_del = del_same_dict[asset_table.lower()]
            excel_path = FileReader.readfirst(excel_dir)
            ans = False
            # def data_sync_v3(db_table, excel_path, hsfa_constr, trade_constr, vertical=True, delete_old=True):
            ans = data_sync_v3(asset_table, excel_path, hsfa_constr, trade_constr, vertical=vertical, delete_old=t_del)
            if ans:
                print('-----------导入本地数据库：%s 成功-----------'%(excel_path))
        else:
            print('~~~~~~~~~~~~~~~~~~~~~~~~~忽略导入本地数据库：%s ~~~~~~~~~~~~~~~~~~~~~~~~~' % (excel_dir))
    else:
        print('~~~~~~~~~~~~~~~~~~~~~~~~~配置文件为空，忽略导入本地数据库：%s ~~~~~~~~~~~~~~~~~~~~~~~~~' % (excel_dir))
    return


def sync_zhong_zhen():
    trade_constr = 'oracle://test1:test1@10.21.68.206:1521/trade'
    hsfa_constr = 'oracle://test2:test2@10.21.68.211:1521/hsfa'
    conf_path = '../config/sync.txt'
    mconf_list = init_config.read_sync_config(conf_path)
    if mconf_list and len(mconf_list)>=2:
        sync_table_dict = mconf_list[0]
        del_same_dict = mconf_list[1]



    task_asset_table = 'T_OWN_INVEST_DERIV'
    # 统一删除中信证券推送task表，防止删除刚刚生成的task数据
    delete_tables = ['T_OWN_INVEST_DERIV']
    for table in delete_tables:
        delete_citicsf_tables(trade_constr, table, trade_constr)
    # 这三个参数是要修改的

    # 同步中证资本-仓单表------------------------------------------------------------------------------------------------
    excel_dir = '../input/中证资本-仓单'
    asset_table = 'G_MANAGEMENT_WARRANT'

    # def store_table_general(hsfa_constr=None, trade_constr=None, excel_dir=None, asset_table=None, vertical=True,
    #                         sync_table_dict=None, del_same_dict=None):
    store_table_general(hsfa_constr,trade_constr,excel_dir,asset_table, sync_table_dict=sync_table_dict, del_same_dict=del_same_dict)

    # 同步中证资本基差-场外衍生产品------------------------------------------------------------------------------------------------
    excel_dir = '../input/中证资本基差-场外衍生产品'
    asset_table = 'G_OTC_DERIVATIVES'
    store_table_general(hsfa_constr, trade_constr, excel_dir, asset_table, sync_table_dict=sync_table_dict, del_same_dict=del_same_dict)

    # 同步中证资本-基差-点价------------------------------------------------------------------------------------------------
    # excel_dir = '../input/中证资本-基差-点价'
    # asset_table = 'G_BASIS_PRICING'
    # store_table_general(hsfa_constr, trade_constr, excel_dir, asset_table)

    # 中证资本基差-期权------------------------------------------------------------------------------------------------
    # excel_dir = '../input/中证资本基差-期权'
    # asset_table = 'G_BASIS_OPTION'
    # store_table_general(hsfa_constr, trade_constr, excel_dir, asset_table)


    # # 中证资本期权做市
    lead_in_qiquanzuoshi(hsfa_constr, trade_constr, True,
                         sync_table_dict, del_same_dict)

    return

def delete_citicsf_tables(hsfa_constr=None, task_db_table=None, trade_constr=None):

    db_man = dbmanager.DBManager(source_constr=trade_constr, des_constr=hsfa_constr)
    if hsfa_constr:
        ans = db_man.delete_sou_table(tablename=task_db_table)
    if trade_constr != hsfa_constr:
        ans = db_man.delete_des_table(tablename=task_db_table)
    return


def sync_citicsf_tables(hsfa_constr, task_db_table, trade_constr):

    db_man = dbmanager.DBManager(source_constr=trade_constr, des_constr=hsfa_constr)
    ans = db_man.dbsync(tablename=task_db_table)
    return ans


#  处理中证资本表
def data_sync_v3(db_table, excel_path, hsfa_constr, trade_constr, vertical=True, delete_old=True):

    df_input = None
    # vertical means the headers is located vertically
    if vertical:
        df_input = reader.SecFileReader.read_vertical(path=excel_path, header=db_table)
    else:
        df_input = reader.SecFileReader.read(path=excel_path, header=db_table)

    db_man = dbmanager.DBManager(source_constr=trade_constr, des_constr=hsfa_constr)
    reportdate = str(df_input['REPORT_DATE'].iloc[0])
    if delete_old:
        ans = db_man.delete_report_date(constr=trade_constr, tablename=db_table, t_date=reportdate)

    ans = None
    ans = db_man.insert_sou_db(tablename=db_table, data=df_input)

    return ans


#  处理中证资本表
def data_sync_v2(db_table, excel_path, hsfa_constr, task_db_table, trade_constr, vertical=True, delete_old=True):
    # df_input = reader.file_reader.read(path=excel_path, header=mconfig[db_table.lower()])
    df_input = None
    # vertical means the headers is located vertically
    if vertical:
        df_input = reader.SecFileReader.read_vertical(path=excel_path, header=db_table)
    else:
        df_input = reader.SecFileReader.read(path=excel_path, header=db_table)
    # print(df_input)
    # constraints = init_config.read_check_config('./config/excel_data_constraint.conf', asset_table)
    # datacheck.checkdata(df_input, constraints)
    db_man = dbmanager.DBManager(source_constr=trade_constr, des_constr=hsfa_constr)
    reportdate = str(df_input['REPORT_DATE'].iloc[0])
    if delete_old:
        # 删除同步表所有数据，但是本地数据库表只删除同一个持仓日期的数据
        # ans = db_man.delete_sou_table(tablename=task_db_table)
        ans = db_man.delete_report_date(constr=trade_constr, tablename=db_table, t_date=reportdate)

        # modified on 2017-10-10 只删除同一个持仓日期的数据
        # ans = db_man.delete_sou_table(tablename=db_table)
        # ans = db_man.delete_sou_table(tablename=task_db_table)
        # ans = db_man.delete_des_table(tablename=task_db_table)
    ans = db_man.insert_sou_db(tablename=db_table, data=df_input)
    print(ans)
    # ans = db_man.dbsync(tablename=task_db_table)
    return ans


#  处理期货对账单
def store_statement(db_table, dir_path, hsfa_constr, trade_constr):


    data_list = []
    parentAccPath = dir_path + '/对账单/'
    kingNewPath = dir_path + '/金牛对账单/'

    # 读取中信期货对账单
    pbills = ParentBillList(parentAccPath)
    bills = list(pbills.getAllPBills())
    for a_bill in bills:
        data_list.append(a_bill.get_accsum())

    # 读取金牛对账单
    cbills = ChildBillList(kingNewPath)
    kn_bills = list(cbills.getAllBills())
    for a_bill in kn_bills:
        data_list.append(a_bill.get_accsum())

    db_man = dbmanager.DBManager(source_constr=trade_constr, des_constr=hsfa_constr)


    ans = db_man.insert_direct(tablename=db_table,data_list=data_list)
    print(ans)

    return ans

#  处理期货持仓
def store_position(excel_path, hsfa_constr, trade_constr, vertical=True, delete_old=False):
    # df_input = reader.file_reader.read(path=excel_path, header=mconfig[db_table.lower()])
    # 通过将当日数据导入临时持仓表TEMP_POSITION， 然后通过TEMP_POSITION与期权做市组合
    position_table = 'POSITION'
    temp_position_table = 'TEMP_POSITION'
    df_input = None
    # vertical means the headers is located vertically
    if vertical:
        df_input = reader.SecFileReader.read_vertical(path=excel_path, header=position_table)
    else:
        df_input = reader.SecFileReader.read(path=excel_path, header=position_table)
    # print(df_input)
    df_input['REPORT_DATE'] = df_input['TRADING_DAY']
    df_input['REPORT_DATE'] = df_input['REPORT_DATE'].apply(lambda x: int(x.strftime('%Y%m%d')))
    # print(df_input)
    db_man = dbmanager.DBManager(source_constr=trade_constr, des_constr=hsfa_constr)

    # tmp = df_input['TRADING_DAY'].iloc[0].strftime('%Y%m%d')
    # print(tmp)
    reportdate = df_input['TRADING_DAY'].iloc[0]
    # print(reportdate)

    if delete_old:

        # 删除同步表所有数据，但是本地数据库表只删除同一个持仓日期的数据
        # report_date TRADING_DAY 不一样
        # 删除持仓表和其他的表格不一样不能直接删除，因为持仓分几个文件导入，如果删除也把之前导入的持仓也删除了。必须
        # 在导入持仓之前统一删除。
        ans = db_man.delete_report_date(constr=trade_constr, tablename=position_table, t_date=reportdate)

    # ans = db_man.delete_sou_table(tablename=temp_position_table)
    ans = db_man.insert_sou_db(tablename=position_table, data=df_input)

    if ans:
        print(ans)
    else:
        print(excel_path + ' insert fail! ')
    reportdate = reportdate.strftime('%Y%m%d')
    return reportdate

#  处理期货持仓
def store_position_v2(excel_path, hsfa_constr, trade_constr, vertical=True, delete_old=False, position_table=None, del_date_list=None):

    df_input = None
    # vertical means the headers is located vertically
    if vertical:
        df_input = reader.SecFileReader.read_vertical(path=excel_path, header=position_table)
    else:
        df_input = reader.SecFileReader.read(path=excel_path, header=position_table)
    # print(df_input)
    # print(excel_path)
    temp_df = df_input['TRADING_DAY']
    # print(temp_df)
    df_input['REPORT_DATE'] = temp_df.apply(lambda x: x.strftime('%Y%m%d'))
    df_input['REPORT_DATE'] = df_input['REPORT_DATE'].astype('int')

    tmp = df_input['REPORT_DATE']
    reportdate = int(df_input['REPORT_DATE'].iloc[0])


    # print(df_input)
    db_man = dbmanager.DBManager(source_constr=trade_constr, des_constr=hsfa_constr)

    if del_date_list:
        if reportdate not in del_date_list:
            ans = db_man.delete_report_date(constr=trade_constr, tablename=position_table, t_date=reportdate)
    else:
        ans = db_man.delete_report_date(constr=trade_constr, tablename=position_table, t_date=reportdate)



    # print(reportdate)

    # if delete_old:
    #
    #     # 删除同步表所有数据，但是本地数据库表只删除同一个持仓日期的数据
    #     # report_date TRADING_DAY 不一样
    #     # 删除持仓表和其他的表格不一样不能直接删除，因为持仓分几个文件导入，如果删除也把之前导入的持仓也删除了。必须
    #     # 在导入持仓之前统一删除。
    #     ans = db_man.delete_report_date(constr=trade_constr, tablename=position_table, t_date=reportdate)


    ans = db_man.insert_sou_db(tablename=position_table, data=df_input)

    if ans:
        print(excel_path + ' insert success! ')
    else:
        print(excel_path + ' insert fail! ')

    return reportdate

def sync_qqzs_general(date=None, table_position = 'POSITION', table_qiquanzuoshi = 'G_OPTION_DEALER', table_task = 'T_OWN_INVEST_DERIV'):
    trade_constr = 'oracle://test1:test1@10.21.68.206:1521/trade'
    hsfa_constr = 'oracle://test2:test2@10.21.68.211:1521/hsfa'


    db_man = dbmanager.DBManager(source_constr=trade_constr, des_constr=hsfa_constr)
    # ans = db_man.sync_qiquanzuoshi(t_date=date)
    # def sync_qiquanzuoshi_general(self, t_date=None, table_position = 'POSITION', table_qiquanzuoshi = 'G_OPTION_DEALER', table_task = 'T_OWN_INVEST_DERIV'):
    ans = db_man.sync_qiquanzuoshi_general(t_date=date, table_position=table_position, table_qiquanzuoshi=table_qiquanzuoshi, table_task= table_task)
    return ans

def sync_qiquanzuoshi(date=None):
    trade_constr = 'oracle://test1:test1@10.21.68.206:1521/trade'
    hsfa_constr = 'oracle://test2:test2@10.21.68.211:1521/hsfa'


    db_man = dbmanager.DBManager(source_constr=trade_constr, des_constr=hsfa_constr)
    # ans = db_man.sync_qiquanzuoshi(t_date=date)
    # def sync_qiquanzuoshi_general(self, t_date=None, table_position = 'POSITION', table_qiquanzuoshi = 'G_OPTION_DEALER', table_task = 'T_OWN_INVEST_DERIV'):
    ans = db_man.sync_qiquanzuoshi_general(t_date=date)
    return ans