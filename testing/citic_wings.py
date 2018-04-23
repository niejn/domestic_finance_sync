import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import *
# import tushare as ts
import pandas as pd
from sqlalchemy.orm import sessionmaker, mapper
from datetime import *
from datetime import datetime
# metadata.reflect(bind=engine)
# metadata.tables.keys()
# playlist = metadata.tables['Playlist']
# from sqlalchemy import select
# s = select([playlist]).limit(10)
# engine.execute(s).fetchall()
from sqlalchemy.exc import IntegrityError


# def ship_it(order_id):
#     s = select([line_items.c.cookie_id, line_items.c.quantity])
#     s = s.where(line_items.c.order_id == order_id)
#     transaction = connection.begin()
#     cookies_to_ship = connection.execute(s).fetchall()
#     try:
#         for cookie in cookies_to_ship:
#             u = update(cookies).where(cookies.c.cookie_id == cookie.cookie_id)
#             u = u.values(quantity = cookies.c.quantity-cookie.quantity)
#             result = connection.execute(u)
#         u = update(orders).where(orders.c.order_id == order_id)
#         u = u.values(shipped=True)
#         result = connection.execute(u)
#         print("Shipped order ID: {}".format(order_id))
#         transaction.commit()
#     except IntegrityError as error:
#         transaction.rollback()
#         print(error)


def insert_data_transaction(constr='oracle://test2:test2@10.21.68.211:1521/hsfa'):
    config_path = '../excel/ziguanjihua.xlsx'
    df_header = ['report_date', 'service_type', 'invest_company_code', 'security_code', 'security_name',
                 'issuer_entity', 'hold_amount', 'hold_cost_value', 'hold_market_value', 'hold_market_value_o32',
                 'current_nav', 'current_nav_o32', 'credit_warning', 'winding_up_warning', 'security_risk_rank',
                 'redem_term', 'opening_frequency', 'daily_purchase_quantity', 'daily_purchase_amount',
                 'daily_purchase_entity', 'daily_redeem_quantity', 'daily_redeem_amount', 'daily_redeem_entity',
                 'currency_code', 'exchange_rate', 'security_type', 'security_sub_type', 'amc_security_type',
                 'amc_security_sub_type1', 'amc_security_sub_type2', 'amc_security_sub_type3', 'graded_fund_flag',
                 'graded_fund_pri', 'biunique_flag', 'agree_loss_flag', 'redem_limit_flag', 'freeze_pledge_flag',
                 'freeze_pledge_value', 'group_inter_trans_flag', 'group_counter_party', 'group_counter_value',
                 'last_modified_date']


    df = pd.read_excel(config_path, header=None, skiprows=[0])
    df.columns = df.columns.map(lambda x: df_header[int(x)])
    print(df)

    df = df.drop(['last_modified_date'],axis=1)
    # df = df[1:]
    print(df)


    engine = create_engine(constr)
    connection = engine.connect()
    transaction = connection.begin()
    try:
        df.to_sql('G_CITIC_INVEST_INFO'.lower(), engine, if_exists='append', index=False)

        transaction.commit()
    except IntegrityError as error:
        transaction.rollback()
        print(error)
    except Exception as error:
        transaction.rollback()
        print(error)
    finally:
        connection.close()
    return


def insert_data(constr='oracle://test2:test2@10.21.68.211:1521/hsfa'):
    config_path = '../excel/ziguanjihua.xls'
    df_header = ['report_date', 'service_type', 'invest_company_code', 'security_code', 'security_name', 'issuer_entity', 'hold_amount', 'hold_cost_value', 'hold_market_value', 'hold_market_value_o32', 'current_nav', 'current_nav_o32', 'credit_warning', 'winding_up_warning', 'security_risk_rank', 'redem_term', 'opening_frequency', 'daily_purchase_quantity', 'daily_purchase_amount', 'daily_purchase_entity', 'daily_redeem_quantity', 'daily_redeem_amount', 'daily_redeem_entity', 'currency_code', 'exchange_rate', 'security_type', 'security_sub_type', 'amc_security_type', 'amc_security_sub_type1', 'amc_security_sub_type2', 'amc_security_sub_type3', 'graded_fund_flag', 'graded_fund_pri', 'biunique_flag', 'agree_loss_flag', 'redem_limit_flag', 'freeze_pledge_flag', 'freeze_pledge_value', 'group_inter_trans_flag', 'group_counter_party', 'group_counter_value', 'last_modified_date']
    # df = pd.read_excel(config_path, header=None, names= df_header, skiprows=[0])

    df = pd.read_excel(config_path, header=None, skiprows=[0])
    df.columns = df.columns.map(lambda x: df_header[int(x)])
    # df.rename(columns=df_header, inplace=True)
    print(df)
    # bb.drop(['new', 'hi'], axis=1)
    # df.rename(columns=lambda x: x.replace('$', ''), inplace=True)
    df = df.drop(['last_modified_date'],axis=1)
    # df.rename(columns=('$a': 'a', '$b': 'b', '$c': 'c', '$d': 'd', '$e': 'e'}, inplace = True)
    # df = df[1:]
    print(df)


    engine = create_engine(constr)
    df.to_sql('G_CITIC_INVEST_INFO'.lower(), engine, if_exists='append', index=False)


    return
def q_oracle(constr='oracle://test2:test2@10.21.68.211:1521/hsfa'):
    test_time = datetime.now()
    print(test_time)
    engine = create_engine(constr)
    meta = MetaData(bind=engine, reflect=True)
    table = meta.tables['t_push_task_status_05']
    result2 = list(engine.execute(table.select(table.c.task_id > 2)))
    push_task = Table('t_push_task_status_05', meta, autoload=True, autoload_with=engine)
    print(push_task.columns.keys())
    print(result2)
    # from sqlalchemy import select
    s = select([push_task])
    res = engine.execute(s).fetchall()
    for row in res:
        print(row)
        # print(row.TASK_ID)

    print(meta.tables.keys())
    connection = engine.connect()

    # from sqlalchemy.sql import select
    # s = push_task.select()
    # rp = connection.execute(s)
    # results = rp.fetchall()
    # t = first_row = results[0]
    # t = first_row[1]
    # t = first_row.task_id
    # t = first_row[push_task.c.task_id]
    # s = select([func.count(push_task.c.company_code).label('company_count')])
    # rp = connection.execute(s)
    # record = rp.first()
    # print(record.keys())
    # print(rp.scalar())

    #
    # s = select([push_task.c.task_code, 'Com-' + push_task.c.company_code])
    # for row in connection.execute(s):
    #     print(row)

    # u = update(push_task).where(push_task.c.task_code.like('Test%'))
    # u = u.values(push_status = 'X')
    # result = connection.execute(u)
    # print(result.rowcount)
    # s = select([push_task])
    # result = connection.execute(s).first()
    # for key in result.keys():
    #     print('{}: {}'.format(key, result[key]))

    # from sqlalchemy import delete
    # u = delete(push_task).where(push_task.c.task_code == "Test")
    # result = connection.execute(u)
    # print(result.rowcount)
    # s = select([push_task]).where(push_task.c.task_code == "Test")
    # result = connection.execute(s).fetchall()
    # print(len(result))

    columns = [push_task.c.task_code, push_task.c.company_code, func.count(push_task.c.company_code)]
    all_tasks = select(columns)

    all_tasks = all_tasks.group_by(push_task.c.task_code,  push_task.c.company_code)
    result = connection.execute(all_tasks).fetchall()
    for row in result:
        print(row)




    # s = select([push_task.c.task_code, push_task.c.company_code]).where(push_task.c.task_code == 'Test')
    s = select([push_task]).where(
        and_(
            push_task.c.task_code.like('Test%'),
            push_task.c.company_code != 'TCom'
        )
    )

    # s = select([push_task.c.task_code, push_task.c.company_code]).where(push_task.c.task_code.like('Test%'))
    s = s.order_by(push_task.c.task_code)
    rp = connection.execute(s)
    print(rp.keys())

    for task in rp:
        print('{} - {}'.format(task.task_code, task.company_code))

    # declared as text('t_push_task_status_05'), or use
    # table('t_push_task_status_05')
    # UTC = timezone('UTC')
    ins = push_task.insert()
    task_items = [
        {
            'push_begin_date': datetime.now(),
            'task_id': '2003',
            'push_end_date': datetime.now(),
            'push_status': 'T',
            'report_date': '12345678',
            'company_code': 'TCom3',
            'task_code': 'Test7',
            'last_modified_date': datetime.now()
        },
        {
            'push_begin_date': datetime.now(),
            'task_id': '2003',
            'push_end_date': datetime.now(),
            'push_status': 'T',
            'report_date': '12345678',
            'company_code': 'TCom3',
            'task_code': 'Test8',
            'last_modified_date': datetime.now()
        }
    ]


    # CREATE UNIQUE    INDEX    T_PUSH_TASK_STATUS_05_U1    ON    T_PUSH_TASK_STATUS_05    (TASK_CODE, COMPANY_CODE)
    # executemany
    # result = connection.execute(ins, task_items[0], task_items[1])
    # cursor = connection.cursor()
    # push_task.insert
    result = connection.execute(ins, task_items)
    s = select([push_task])
    res = engine.execute(s).fetchall()
    for row in res:
        print(row)

    ins = push_task.insert().values(
        task_id="2000",
        task_code="Test3",
        company_code="TCom3",
        report_date="12345678",
        push_begin_date=datetime.now(),
        push_end_date=datetime.now(),
        push_status="T",
        last_modified_date=datetime.now()
    )
    print(str(ins))
    print(ins.compile().params)
    try:
        result = connection.execute(ins)
        print(result.inserted_primary_key)
    except IntegrityError as error:
        print('--------------------------------------------------')
        print(error, error.params)
        print('--------------------------------------------------')

    # ins = insert(push_task).values(
    #     task_id="2000",
    #     task_code="Test",
    #     company_code="TCom",
    #     report_date="12345678",
    #     push_begin_date=datetime(2017, 6, 7, 1, 3, 7),
    #     push_end_date=datetime(2017, 6, 7, 1, 3, 7),
    #     push_status="T",
    #     last_modified_date=datetime(2017, 6, 7, 1, 3, 7)
    # )
    #
    # result = connection.execute(ins)
    s = select([push_task])
    res = engine.execute(s).fetchall()
    for row in res:
        print(row)

    connection.close()
    return




def main():
    constr = 'oracle://test1:test1@10.21.68.206:1521/trade'
    insert_data_transaction(constr)
    # insert_data(constr)
    # q_oracle(constr)
    print("End Main threading")


if __name__ == '__main__':
    main()
