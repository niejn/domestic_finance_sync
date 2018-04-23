import os
import pandas as pd
import chardet


def read_all(path):
    files = os.listdir(path)
    textContainer = []
    for file in files:
        file = path + '/' + file
        if not os.path.isfile(file):
            continue
        if file.endswith('txt'):
            encoding = chardet.detect(open(file, 'rb').readline())['encoding']
            with open(file, 'rb') as srcFile:
                #content = fp.read().decode('utf-8')
                # content = srcFile.read().decode('gbk')

                content = srcFile.read().decode(encoding)
                # for oneline in content:
                #     oneline.decode('utf-8')
                #content = content.strip('\r')
                text = content.split('\n')


                #text = srcFile.readlines()
            textContainer.append(text)
            # srcFile.close()

    return textContainer


def main():



    ans = read_all('./bill')
    print(ans)
    return

if __name__ == "__main__":
    main()