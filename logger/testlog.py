import logging
# import logging
import logging.config
# logging.warning('Watch out!')  # will print a message to the console
# logging.info('I told you so')

def log():
    logging.basicConfig(format='%(asctime)s %(message)s')
    logging.warning('is when this event was logged.')

    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    logging.warning('is when this event was logged.')

    # logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
    # logging.debug('This message should appear on the console')
    # logging.info('So should this')
    # logging.warning('And this, too')




    # getattr(logging, loglevel.upper())
    # numeric_level = getattr(logging, loglevel.upper(), None)
    # if not isinstance(numeric_level, int):
    #     raise ValueError('Invalid log level: %s' % loglevel)
    # logging.basicConfig(level=numeric_level, ...)



    # logging.basicConfig(filename='example.log', level=logging.DEBUG)
    # logging.debug('This message should go to the log file')
    # logging.info('So should this')
    # logging.warning('And this, too')
    return
def advancelog():
    logging.config.fileConfig('logging.conf')

    # create logger
    logger = logging.getLogger('simpleExample')

    # 'application' code
    logger.debug('debug message')
    logger.info('info message')
    logger.warn('warn message')
    logger.error('error message')
    logger.critical('critical message')

    # # create logger
    # logger = logging.getLogger('simple_example')
    # logger.setLevel(logging.DEBUG)
    #
    # # create console handler and set level to debug
    # ch = logging.StreamHandler()
    # ch.setLevel(logging.DEBUG)
    #
    # # create formatter
    # formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    #
    # # add formatter to ch
    # ch.setFormatter(formatter)
    #
    # # add ch to logger
    # logger.addHandler(ch)
    #
    # # 'application' code
    # logger.debug('debug message')
    # logger.info('info message')
    # logger.warning('warn message')
    # logger.error('error message')
    # logger.critical('critical message')

    return
def main():
    print("test")

    advancelog()
    # log()
    print("finished")
    return


if __name__ == "__main__":
    main()