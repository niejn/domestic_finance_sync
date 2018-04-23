
# str = "客户号 Client ID：  100109999          客户名称 Client Name：中证资本管理（深圳）有限公司"
import re

def parse_str(str=None, gap='：'):
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

    ans = {key:val  for key, val  in zip(keys, vals)}
    return ans

def main():
    line = "客户号 Client ID：  100109999          客户名称 Client Name：中证资本管理（深圳）有限公司"
    ans = parse_str(line)
    print(ans)
    keys = []
    vals = []
    parse_count = line.count("：")


    str = line.split('：')
    keys.append(str[0])
    for item in str[1:-1]:
        sp_items = item.split(maxsplit=1)
        tval = sp_items[0]
        tkey = sp_items[1]
        keys.append(tkey)
        vals.append(tval)
    vals.append(str[-1])
    ans = str[1].split(maxsplit=1)

    phanzi = re.compile(u'[\u4e00-\u9fa5（）]+');

    res = phanzi.findall(line)
    nums = re.findall(r'([a-zA-Z]*\d+)', line)

    return

if __name__ == "__main__":
    main()