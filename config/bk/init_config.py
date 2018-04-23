# -*- coding: utf-8 -*-
import configparser
import sys
import codecs
def read_check_config(path = './excel_data_constraint.conf', tablename = 'G_CITIC_INVEST_INFO'):
    cp = configparser.ConfigParser()
    with codecs.open(path, 'r', encoding='GB2312') as f:
        cp.read_file(f)
    constraint_config = cp.items(tablename)
    constraints = {key.lower(): value.split(',') for (key, value) in constraint_config}
    return constraints

def read_config(path = './excel_header.conf'):
    # 按类型读取配置信息：getint、 getfloat 和 getboolean
    #
    # print type(cp.getint('db', 'port'))  # <type 'int'>

    cp = configparser.ConfigParser()
    with codecs.open(path, 'r', encoding='GB2312') as f:
        cp.read_file(f)

    header_config = cp.items('Header')
    header_len = cp.items('HeaderLen')
    header_dict = {key.lower(): value.split(',') for (key, value) in header_config}
    length_dict = {key.lower(): (int(value)) for (key, value) in header_len}
    for item in header_dict:
        if item in length_dict:
            if length_dict[item] == len(header_dict[item]):
                print(item + 'config header length correct')
            else:
                print(item + 'config header length not match !')
        else:
            print(item + ' length not configed !')
    # print(header_dict['zgjh_header'])
    # print(len(header_dict['zgjh_header']))
    # print(length_dict['zgjh_header'])
    # if len(header_dict['zgjh_header']) == length_dict['zgjh_header']:
    #     print("correct")








    cp.write(sys.stdout)



    return header_dict



def main():
    read_check_config()
    read_config()
    return


if __name__ == "__main__":
    main()
