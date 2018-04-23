#-*- code: utf-8 -*-
import pandas as pd
from sqlalchemy import (create_engine)
import pandas as pd
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker, mapper
from datetime import datetime




def insert_data_transaction(constr='oracle://test2:test2@10.21.68.211:1521/hsfa', df=None):

    print(df)


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

def main():




    print("End Main threading")


if __name__ == '__main__':
    main()



