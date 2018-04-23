import logging
import logging.handlers
import logging.config

# ----------------------------------------------------------------------
logging.config.fileConfig('logging.conf')
# root_logger = logging.getLogger('root')
# root_logger.info('root logger')
cth_logger = logging.getLogger('main')
# logger = logging.getLogger('billRead')
cth_logger.info('test main logger')
# logging.ERROR
# bill_logger = logging.getLogger('bill')
# bill_logger.error("This is warning message")


# coding:utf-8

import logging

logging.basicConfig(filename='log1.log',
                    format='%(asctime)s -%(name)s-%(levelname)s-%(module)s:%(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S %p',
                    level=logging.DEBUG)

while True:
    option = input("input a digit:")
    if option.isdigit():
        print("hehe", option)
        logging.info('option correct')
    else:
        logging.error("Must input a digit!")

# logging.debug('有bug')
# logging.info('有新的信息')
# logging.warning('警告信息')
# logging.error('错误信息')
# logging.critical('紧急错误信息')
# logging.log(10,'log')



# coding:utf-8
import logging

logger = logging.getLogger("simple_example")
logger.setLevel(logging.DEBUG)

# 输出到屏幕
ch = logging.StreamHandler()
ch.setLevel(logging.WARNING)
# 输出到文件
fh = logging.FileHandler("log2.log")
fh.setLevel(logging.INFO)
# 设置日志格式
fomatter = logging.Formatter('%(asctime)s -%(name)s-%(levelname)s-%(module)s:%(message)s')
ch.setFormatter(fomatter)
fh.setFormatter(fomatter)
logger.addHandler(ch)
logger.addHandler(fh)

logger.debug("debug message")
logger.info("info message")
logger.warning("warning message")
logger.error("error message")
logger.critical("critical message")


def main():
    print("test")

    return


if __name__ == "__main__":
    main()
