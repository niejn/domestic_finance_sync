#-*- code: utf-8 -*-
import pandas as pd
from sqlalchemy.orm import sessionmaker, mapper
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy import create_engine, MetaData, insert, delete
from datetime import *
from datetime import datetime
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql import select
from sqlalchemy import update

class DBManager(object):

    def __init__(self, source_constr=None, des_constr=None):
        self.__sou_constr = source_constr
        self.__des_constr = des_constr
        if source_constr:
            self.__sou_engine = create_engine(source_constr, encoding='gbk')
        if des_constr:
            self.__des_engine = create_engine(des_constr, encoding='gbk')
        return

    def get_engine(self, constr=None):
        ans = None
        if constr == self.__sou_constr:
            ans = self.__sou_engine
        if constr == self.__des_constr:
            ans = self.__des_engine
        return ans

    # 更新数据推送任务表的字段
    def update_task(self, constr=None, tablename='T_PUSH_TASK_STATUS_05', vars=None):
        # detail_date = datetime(2016, 6, 22, 11, 26, 5)
        # detail_date = datetime.today()
        # detail_date = datetime.utcnow()
        # detail_date = datetime.now()

        detail_date = datetime.now()
        from sqlalchemy import create_engine, MetaData, Table
        from sqlalchemy import and_, or_, not_


        print(tablename)
        ans = 0
        engine = self.get_engine(constr)
        connection = engine.connect()
        meta = MetaData(bind=engine)



        o_table = Table(tablename.lower(), meta, autoload=True)
        action = update(o_table).where(and_(o_table.c.company_code == 'CSG_S_05', o_table.c.task_code == 'BIZ_DATA'))
        # my_vars = {'task_id':666, 'last_modified_date': detail_date}
        action = action.values(vars)
        # action = action.values(task_id=120, last_modified_date=detail_date )
        ans = connection.execute(action)
        connection.close()

        return ans.rowcount

    # 开始推送任务
    def beg_task_sync(self, report_date='20170630', constr=None):
        ans = False
        detail_date = datetime.now()
        # S：开始，C：完成：F：失败
        vars = {'push_begin_date': detail_date, 'last_modified_date': detail_date, 'push_status': 'S', 'report_date':'20170630'}
        # update_task(self, constr=None, tablename='T_PUSH_TASK_STATUS_05', vars=None):
        self.update_task(constr=self.__sou_constr, vars=vars)
        return False

    # 结束推送任务
    def end_task_sync(self, report_date='20170630', constr=None):
        ans = False
        detail_date = datetime.now()
        # S：开始，C：完成：F：失败
        vars = {'push_end_date': detail_date, 'last_modified_date': detail_date, 'push_status': 'C', 'report_date':'20170630'}
        # update_task(self, constr=None, tablename='T_PUSH_TASK_STATUS_05', vars=None):
        self.update_task(constr=self.__sou_constr, vars=vars)
        return False

    # 推送任务异常
    def error_task_sync(self, report_date='20170630', constr=None):
        ans = False
        detail_date = datetime.now()
        # S：开始，C：完成：F：失败
        vars = {'push_end_date': detail_date, 'last_modified_date': detail_date, 'push_status': 'F', 'report_date':'20170630'}
        # update_task(self, constr=None, tablename='T_PUSH_TASK_STATUS_05', vars=None):
        self.update_task(constr=self.__sou_constr, vars=vars)
        return False

    def update_task_session(self, constr=None, tablename='T_PUSH_TASK_STATUS_05', t_date='20170630'):
        detail_date = datetime(2016, 6, 22, 11, 26, 5)
        detail_date = datetime.today()
        detail_date = datetime.utcnow()
        detail_date = datetime.now()
        # detail_date = datetime.fromtimestamp(time.time())
        # datetime.fromtimestamp(time.time())
        detail_date_1 = datetime.now()
        from sqlalchemy import create_engine, MetaData, Table
        from sqlalchemy import and_, or_, not_
        # = update(cookies).where(cookies.c.cookie_name == "chocolate chip")
        # u = u.values(quantity=(cookies.c.quantity + 120))

        print(tablename)
        ans = 0
        engine = self.get_engine(constr)
        connection = engine.connect()
        meta = MetaData(bind=engine)
        # users = Table(tablename, meta, autoload=True)
        Session = sessionmaker(bind=engine)
        session = Session()

        # 将数据库中的对象映射到对象中
        tasks = Table(tablename, meta, autoload=True)
        # session.delete(tasks)

        # These are the empty classes that will become our data classes
        class Tasks(object):
            pass
        # Tasks必须要有主键， 否则报错
        usermapper = mapper(Tasks, tasks)

        query = session.query(Tasks)
        print(query.all())
        print(query.first())
        print(query.first().task_id)
        query.filter(and_(Tasks.task_code == 'BIZ_DATA', Tasks.company_code == 'CSG_S_05')).update({Tasks.push_status: 'C', Tasks.report_date: t_date})

        # 删除所有在集合中的数据
        # session.query(Tasks).filter(Tasks.task_id.in_((0, 666, 1111, 1006, 1008))).delete(synchronize_session=False)
        # session.commit()
        session.flush()  # 写数据库，但并不提交
        # print(query.get(1).task_id)
        # session.delete(tasks)
        # session.flush()
        # session.rollback()  # 回滚
        # query.filter(Tasks.task_id == 1111).delete()
        # session.query(Tasks).filter(Tasks.task_id > 0).delete()
        # from sqlalchemy import func
        # print(session.query(func.count('*')).select_from(Tasks).scalar())
        # print(session.query(func.count('1')).select_from(Tasks).scalar())
        # print(session.query(func.count(Tasks.task_id)).scalar())
        # print(session.query(func.count('*')).filter(Tasks.task_id > 0).scalar())  # filter() 中包含 User，因此不需要指定表
        # print(session.query(func.count('*')).filter(Tasks.task_id == 1111).limit(
        #     1).scalar() == 1)  # 可以用 limit() 限制 count() 的返回数
        # print(session.query(func.sum(Tasks.task_id)).scalar())
        # print(session.query(func.now()).scalar())  # func 后可以跟任意函数名，只要该数据库支持
        # print(session.query(func.current_timestamp()).scalar())
        # print(session.query(func.md5(Tasks.task_id)).filter(Tasks.task_id == 1111).scalar())

        # jack = User(name='jack')
        # session.delete(jack)
        # session.add(jack)
        # session.commit()
        # query.filter(User.addresses.contains(someaddress))

        session.commit()


        # session.delete(tasks)
        session.close()
        connection.close()
        return

    # 根据report_date删除表中的数据
    def delete_report_date(self, constr=None, tablename='', t_date=None):
        from sqlalchemy import create_engine, MetaData, Table
        # from sqlalchemy import delete
        # metadata = MetaData(engine)

        print('根据report_date：%s 删除表 %s 中的数据: '% (t_date, tablename))
        # print(t_date)
        ans = 0
        engine = self.get_engine(constr)
        connection = engine.connect()
        meta = MetaData(bind=engine)
        o_table = Table(tablename.lower(), meta, autoload=True)
        if tablename != 'POSITION':
            action = delete(o_table).where(o_table.c.report_date == (t_date))

        else:
            action = delete(o_table).where(o_table.c.report_date == (t_date))
            # action = delete(o_table).where(o_table.c.trading_day == (t_date))
        ans = connection.execute(action)
        connection.close()
        return ans.rowcount

    def delete_table_afterdate(self, constr=None, tablename='', t_date = datetime(2017, 9, 22, 0, 0)):
        from sqlalchemy import create_engine, MetaData, Table
        # from sqlalchemy import delete
        # metadata = MetaData(engine)

        print(tablename)
        ans = 0
        engine = self.get_engine(constr)
        connection = engine.connect()
        meta = MetaData(bind=engine)
        o_table = Table(tablename.lower(), meta, autoload=True)
        action =  delete(o_table).where(o_table.c.last_modified_date >= (t_date))
        ans = connection.execute(action)
        connection.close()
        return ans.rowcount

    def delete_table(self, constr=None, tablename=''):
        print('delete all table: '+ tablename)
        ans = 0
        engine = self.get_engine(constr)
        connection = engine.connect()
        # meta = MetaData(bind=engine, reflect=True)
        # o_table = meta.tables[tablename.lower()]
        meta = MetaData(bind=engine)
        o_table = Table(tablename.lower(), meta, autoload=True)
        action = delete(o_table)
        ans = connection.execute(action)
        connection.close()
        return ans.rowcount

    #     added on 2017-09-07, 直接将数据导入数据库，不使用pandas
    def insert_direct(self, constr=None, tablename='', data_list=None):
        if not constr:
            constr = self.__sou_constr
        # print(tablename)
        ans = False
        engine = self.get_engine(constr)
        connection = engine.connect()
        # meta = MetaData(bind=engine, reflect=True)
        # o_table = meta.tables[tablename.lower()]
        meta = MetaData(bind=engine)
        o_table = Table(tablename.lower(), meta, autoload=True)
        # action = o_table.insert().values(data_list[0])
        transaction = connection.begin()

        try:
            # print(data_list)
            # ans = connection.execute(action)
            for data in data_list:
                # print(data)
                # [s.lower() if isinstance(s , str) else s for s in L ]
                data_lower = {key.lower(): val.replace('\xa0', '') if isinstance(val, str) else val for key, val in data.items()}
                # replace(u'\xa0 ', u' ')
                # print(data_lower)
                action = o_table.insert().values(data_lower)
                ans = connection.execute(action)
            transaction.commit()
            ans = True
        except IntegrityError as error:
            print(data)
            print(data_lower)
            transaction.rollback()
            print(error)
        except Exception as error:
            print(data)
            print(data_lower)
            transaction.rollback()
            print(error)
        finally:
            connection.close()




        return ans

    def delete_sou_table(self, tablename=None):
        return self.delete_table(self.__sou_constr, tablename)

    def delete_des_table(self, tablename=None):
        return self.delete_table(self.__des_constr, tablename)

    def insert_sou_db(self, tablename=None, data=None):
        # def insert_direct(self, constr=None, tablename='', data_list=None):
        # self.insert_to_db(self.__sou_constr, tablename, data)
        # print(data)
        datalist = data.to_dict('records')

        # print(datalist)
        # datalist = data.values.tolist()
        ans = self.insert_direct(self.__sou_constr, tablename, datalist)
        return ans

    def insert_des_db(self, tablename=None, data=None):
        return self.insert_to_db(self.__des_constr, tablename, data)

    # added on 2017-10-10
    def insert_sou_db_v2(self, tablename=None, data=None):
        return self.insert_to_db_v2(self.__sou_constr, tablename, data)

    # 失败提示
    def insert_to_db_v2(self, constr=None, tablename=None, data=None):
        ans = False
        if not tablename:
            return ans
        if data is None:
            return ans

        engine = self.get_engine(constr)

        connection = engine.connect()
        # transaction = connection.begin()
        try:

            data.to_sql(tablename.lower(), engine, if_exists='append', index=False)

            # transaction.commit()
            ans = True
        except IntegrityError as error:
            # transaction.rollback()
            print(error)
        except Exception as error:
            # transaction.rollback()
            print(error)
        finally:
            connection.close()

        return ans

    def insert_to_db(self, constr=None, tablename=None, data=None):
        ans = False
        if not tablename:
            return ans
        if data is None:
            return ans

        engine = self.get_engine(constr)

        connection = engine.connect()
        transaction = connection.begin()
        try:
            import sys
            print(sys.getdefaultencoding())

            print(data)
            data.to_sql(tablename.lower(), engine, if_exists='append', index=False)

            transaction.commit()
            ans = True
        except IntegrityError as error:
            transaction.rollback()
            print(error)
        except Exception as error:
            transaction.rollback()
            print(error)
        finally:
            connection.close()

        return ans
    def get_db(self, constr=None, tar_table=None):
        tar_table = tar_table.lower()
        test_time = datetime.now()
        print(test_time)
        engine = self.get_engine(constr)

        # meta = MetaData(bind=engine, reflect=True)
        # table = meta.tables[tar_table]

        # on0922, 优化反射，不反射整个数据库
        meta = MetaData(bind=engine)
        table = Table(tar_table.lower(), meta, autoload=True)

        result = list(engine.execute(table.select()))
        # print(result)

        return result
    def get_sou_db(self, tablename=None):
        return self.get_db(self.__sou_constr, tablename)
    def get_des_db(self, tablename=None):
        return self.get_db(self.__sou_constr, tablename)

    def insert_tuples(self, constr=None, tar_table=None, data=None):
        if not date:
            return
        tar_table = tar_table.lower()
        tar_table = tar_table.lower()
        test_time = datetime.now()
        print(test_time)
        engine = self.get_engine(constr)
        connection = engine.connect()
        # meta = MetaData(bind=engine, reflect=True)
        # o_table = meta.tables[tar_table]

        # 0922
        meta = MetaData(bind=engine)
        o_table = Table(tar_table.lower(), meta, autoload=True)

        ins = insert(o_table)
        result = connection.execute(ins, data)
        connection.close()
        print(result)
        return



    def dbsync(self, tablename=None):
        sync_data = self.get_sou_db(tablename)
        if sync_data:
            self.insert_tuples(self.__des_constr, tablename, sync_data)
        return

    # 从本地数据库抓取数据
    def get_source_data(self, table_name):

        return

    # 从数据库中抓取数据
    def get_data(self):

        return

    # 通过日期和投资者代码将期权做市表与持仓表join
    def sync_qiquanzuoshi(self, t_date = '20170630'):
        constr = self.__sou_constr
        # 更换为position
        # 增加etfposition
        # table_etfposition = 'ETFPOSITION'
        table_position = 'POSITION'
        table_qiquanzuoshi = 'G_OPTION_DEALER'
        table_task = 'T_OWN_INVEST_DERIV'
        table_position = table_position.lower()
        table_qiquanzuoshi = table_qiquanzuoshi.lower()
        table_task = table_task.lower()
        test_time = datetime.now()
        # print(test_time)
        engine = self.get_engine(constr)




        # meta = MetaData(bind=engine, reflect=True)
        # position = meta.tables[table_position]
        # dealer = meta.tables[table_qiquanzuoshi]
        # task_deriv = meta.tables[table_task]
        meta = MetaData(bind=engine)
        position = Table(table_position.lower(), meta, autoload=True)
        dealer = Table(table_qiquanzuoshi.lower(), meta, autoload=True)
        # added on 2017-10-12
        # etfposition = Table(table_position.lower(), meta, autoload=True)

        task_deriv = Table(table_task.lower(), meta, autoload=True)

        # 删除在从期权做市表中查询的dealer.c.the_main_contract
        columns = [dealer.c.report_date, dealer.c.manage_company_code, dealer.c.derivatives_type, dealer.c.on_exchange_flag, \
        dealer.c.exchange_type, dealer.c.counter_party, dealer.c.cp_cred_type, dealer.c.cp_cred_id, \
        dealer.c.cp_inter_rating, dealer.c.the_main_contract, position.c.notional_value, position.c.premium_price, \
        position.c.delta_value, position.c.press_loss, position.c.book_value, dealer.c.currency_code, \
        dealer.c.group_inter_trans_flag, dealer.c.group_counter_party, dealer.c.group_counter_value, \
        dealer.c.last_modified_date]

        # 删除LAST_MODIFIED_DATE
        t_keys = ['report_date','manage_company_code','derivatives_type','on_exchange_flag',
                  'exchange_type','counter_party','cp_cred_type','cp_cred_id','cp_inter_rating',
                  'currency_code','group_inter_trans_flag','group_counter_party',
                  'group_counter_value',
                  'notional_value',
                  'premium_price','delta_value','press_loss',
                  'hold_asset', 'book_value',
                  # 在持仓表中增加持仓手数和买卖方向两个字段
                  'lots', 'direction'
                  ]

        # ----------------------------
        Session = sessionmaker(bind=engine)
        session = Session()
        # dealer_first = session.query(dealer).first()
        # position_all = session.query(position).all()

        # 删除LAST_MODIFIED_DATE
        # 删除在从期权做市表中查询的dealer.c.the_main_contract
        # dealer_task_15 = session.query(dealer.c.report_date, dealer.c.manage_company_code, dealer.c.derivatives_type, dealer.c.on_exchange_flag, \
        # dealer.c.exchange_type, dealer.c.counter_party, dealer.c.cp_cred_type, dealer.c.cp_cred_id, \
        # dealer.c.cp_inter_rating, dealer.c.currency_code, \
        # dealer.c.group_inter_trans_flag, dealer.c.group_counter_party, dealer.c.group_counter_value).all()
        # where(o_table.c.trading_day == (t_date))

        # t_date = '20170630'
        dealer_task_15 = session.query(dealer.c.report_date, dealer.c.manage_company_code, dealer.c.derivatives_type,
                                       dealer.c.on_exchange_flag, \
                                       dealer.c.exchange_type, dealer.c.counter_party, dealer.c.cp_cred_type,
                                       dealer.c.cp_cred_id, \
                                       dealer.c.cp_inter_rating, dealer.c.currency_code, \
                                       dealer.c.group_inter_trans_flag, dealer.c.group_counter_party,
                                       dealer.c.group_counter_value).filter(dealer.c.report_date == (t_date)).all()

        # trading_day == (t_date) 2017-06-30
        position_task_5 = session.query(position.c.notional_value, position.c.premium_price, \
                                        position.c.delta_value, position.c.press_loss,
                                        position.c.hold_asset, position.c.book_value,
                                        # 在持仓表中增加持仓手数和买卖方向两个字段
                                        position.c.lots, position.c.direction

                                        ).filter(position.c.report_date == (t_date)).all()
        ans_tasks = []


        def row2dict(row):
            d = {}
            for column in row._fields:
                d[str(column)] = row[column]

            return d


        tt = dealer_task_15[0]
        cols = [str(column) for column in tt._real_fields]
        vals = list(dealer_task_15[0])
        a_dealer_task_15 = dict(zip(cols, vals))


        for a_position in position_task_5:
            #   --TARGET_PRICE修改为衍生品表中对应字段名字book_value
            # temp_target_price = a_position.target_price
            temp_target_price = a_position.book_value
            a_position_cols = [str(column) for column in a_position._real_fields]
            a_position_vals = list(a_position)
            a_position_dict = dict(zip(a_position_cols, a_position_vals))
            # 持仓查询excel文件：
            # 标的价格为0——期货，
            # 标的价格！=0——期权
            # 判断是否是期货，期货填D13，否则d14
            # temp = a_dealer_task_15.derivatives_type
            # a_dealer_task_15.derivatives_type = 'D13'
            if temp_target_price == 0:
                a_dealer_task_15['derivatives_type'] = 'D13'
            else:
                a_dealer_task_15['derivatives_type'] = 'D14'

            # 标的价格
            a_task = {}
            a_task.update(a_dealer_task_15)
            a_task.update(a_position_dict)
            ans_tasks.append(a_task)


        final_task = ans_tasks
        ans = self.insert_direct(constr=constr, tablename=table_task, data_list=final_task)

        return ans

    def sync_qiquanzuoshi_general(self, t_date=None, table_position = 'POSITION', table_qiquanzuoshi = 'G_OPTION_DEALER', table_task = 'T_OWN_INVEST_DERIV'):
        constr = self.__sou_constr
        # 更换为position
        # 增加etfposition
        # table_etfposition = 'ETFPOSITION'
        # table_position = 'POSITION'
        # table_qiquanzuoshi = 'G_OPTION_DEALER'
        # table_task = 'T_OWN_INVEST_DERIV'
        table_position = table_position.lower()
        table_qiquanzuoshi = table_qiquanzuoshi.lower()
        table_task = table_task.lower()
        test_time = datetime.now()
        # print(test_time)
        engine = self.get_engine(constr)

        # meta = MetaData(bind=engine, reflect=True)
        # position = meta.tables[table_position]
        # dealer = meta.tables[table_qiquanzuoshi]
        # task_deriv = meta.tables[table_task]
        meta = MetaData(bind=engine)
        position = Table(table_position.lower(), meta, autoload=True)
        dealer = Table(table_qiquanzuoshi.lower(), meta, autoload=True)
        # added on 2017-10-12
        # etfposition = Table(table_position.lower(), meta, autoload=True)

        task_deriv = Table(table_task.lower(), meta, autoload=True)

        # 删除在从期权做市表中查询的dealer.c.the_main_contract


        # 删除LAST_MODIFIED_DATE
        t_keys = ['report_date', 'manage_company_code', 'derivatives_type', 'on_exchange_flag',
                  'exchange_type', 'counter_party', 'cp_cred_type', 'cp_cred_id', 'cp_inter_rating',
                  'currency_code', 'group_inter_trans_flag', 'group_counter_party',
                  'group_counter_value',
                  'notional_value',
                  'premium_price', 'delta_value', 'press_loss',
                  'hold_asset', 'book_value',
                  # 在持仓表中增加持仓手数和买卖方向两个字段
                  'lots', 'direction'
                  ]

        # ----------------------------
        Session = sessionmaker(bind=engine)
        session = Session()


        dealer_task_15 = session.query(dealer.c.manage_company_code, dealer.c.derivatives_type,
                                       dealer.c.on_exchange_flag, \
                                       dealer.c.exchange_type, dealer.c.counter_party, dealer.c.cp_cred_type,
                                       dealer.c.cp_cred_id, \
                                       dealer.c.cp_inter_rating, dealer.c.currency_code, \
                                       dealer.c.group_inter_trans_flag, dealer.c.group_counter_party,
                                       dealer.c.group_counter_value).filter(dealer.c.report_date == (t_date)).first()
        if not dealer_task_15:
            dealer_task_15 = session.query(dealer.c.report_date, dealer.c.manage_company_code, dealer.c.derivatives_type,
                                       dealer.c.on_exchange_flag, \
                                       dealer.c.exchange_type, dealer.c.counter_party, dealer.c.cp_cred_type,
                                       dealer.c.cp_cred_id, \
                                       dealer.c.cp_inter_rating, dealer.c.currency_code, \
                                       dealer.c.group_inter_trans_flag, dealer.c.group_counter_party,
                                       dealer.c.group_counter_value).first()
        if not dealer_task_15:
            print('sync_qiquanzuoshi_general dealer_task_15 is None')
            return False
        # trading_day == (t_date) 2017-06-30
        position_task_5 = session.query(position.c.notional_value, position.c.premium_price, \
                                        position.c.delta_value, position.c.press_loss,
                                        position.c.hold_asset, position.c.book_value,
                                        # 在持仓表中增加持仓手数和买卖方向两个字段
                                        position.c.lots, position.c.direction,
                                        position.c.report_date,

                                        ).filter(position.c.report_date == (t_date)).all()
        ans_tasks = []

        def row2dict(row):
            d = {}
            for column in row._fields:
                d[str(column)] = row[column]

            return d

        tt = dealer_task_15
        print(tt)
        cols = [str(column) for column in tt._real_fields]
        vals = list(dealer_task_15)
        a_dealer_task_15 = dict(zip(cols, vals))

        for a_position in position_task_5:
            #   --TARGET_PRICE修改为衍生品表中对应字段名字book_value
            # temp_target_price = a_position.target_price
            temp_target_price = a_position.book_value
            a_position_cols = [str(column) for column in a_position._real_fields]
            a_position_vals = list(a_position)
            a_position_dict = dict(zip(a_position_cols, a_position_vals))
            # 持仓查询excel文件：
            # 标的价格为0——期货，
            # 标的价格！=0——期权，这是期货期权
            # 判断是否是期货，期货填D13，否则d14
            # 期货期权填写d14
            # 50etf期权需要填写为，买入股票期权对应D03, 卖出股票期权对应D04
            # 0 表示买， 1表示卖方向
            # temp = a_dealer_task_15.derivatives_type
            # a_dealer_task_15.derivatives_type = 'D13'
            if table_position =='etfposition' or table_position =='ETFPOSITION':
                # 0表示买入股票期权， 买入股票期权对应D03
                if a_position_dict['direction'] == '0' or a_position_dict['direction'] == 0:
                    a_dealer_task_15['derivatives_type'] = 'D03'
                else:
                    a_dealer_task_15['derivatives_type'] = 'D05'
            else:
                if temp_target_price == 0:
                    a_dealer_task_15['derivatives_type'] = 'D13'
                else:
                    a_dealer_task_15['derivatives_type'] = 'D14'

            # 标的价格
            a_task = {}
            a_task.update(a_dealer_task_15)
            a_task.update(a_position_dict)
            ans_tasks.append(a_task)

        final_task = ans_tasks
        ans = self.insert_direct(constr=constr, tablename=table_task, data_list=final_task)

        return ans
    '''
    dt = datetime.now()  
print '(%Y-%m-%d %H:%M:%S %f): ', dt.strftime('%Y-%m-%d %H:%M:%S %f')  
print '(%Y-%m-%d %H:%M:%S %p): ', dt.strftime('%y-%m-%d %I:%M:%S %p')  
print '%%a: %s ' % dt.strftime('%a')  
print '%%A: %s ' % dt.strftime('%A')  
print '%%b: %s ' % dt.strftime('%b')  
print '%%B: %s ' % dt.strftime('%B')  
print '日期时间%%c: %s ' % dt.strftime('%c')  
print '日期%%x：%s ' % dt.strftime('%x')  
print '时间%%X：%s ' % dt.strftime('%X')  
print '今天是这周的第%s天 ' % dt.strftime('%w')  
print '今天是今年的第%s天 ' % dt.strftime('%j')  
print '今周是今年的第%s周 ' % dt.strftime('%U')
'''


def sync_task_status(trade_constr, hsfa_constr, status='C'):
    db_man = DBManager(source_constr=trade_constr, des_constr=hsfa_constr)
    if status == 'C':
        db_man.end_task_sync()
    elif status == 'F':
        db_man.error_task_sync()
    else:
        db_man.beg_task_sync()

    return

def main():
    trade_constr = 'oracle://test1:test1@10.21.68.206:1521/trade'
    hsfa_constr = 'oracle://test2:test2@10.21.68.211:1521/hsfa'
    sync_task_status(trade_constr, hsfa_constr)
    db_man = DBManager(source_constr=trade_constr, des_constr=hsfa_constr)
    # constr = None, tablename = '', t_date = datetime(2017, 9, 22, 0, 0)):
    # db_man.beg_task_sync()
    # db_man.end_task_sync()
    # db_man.error_task_sync()
    # db_man.update_task_session(constr=trade_constr)
    # db_man.update_task(constr=trade_constr)
    db_man.sync_qiquanzuoshi()
    # db_man.delete_report_date(constr=trade_constr, tablename='G_OPTION_DEALER')


    print("End Main threading")


if __name__ == '__main__':
    main()


