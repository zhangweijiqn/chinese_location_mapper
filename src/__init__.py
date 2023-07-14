# -*- coding: utf-8 -*-
# __init__.py
import sys
import os
root_path = os.path.abspath(os.path.dirname(__file__)).split('src')[0]
sys.path.append(root_path)

# from .structures import AddrMap, Pca
# from .structures import P, C, A
from .matcher import Matcher

VERSION = (0, 5, 5)

__version__ = ".".join([str(x) for x in VERSION])

# 结果 dataframe 的列名
_PROVINCE = "省"
_PROVINCE_POS = "省_pos"

_CITY = "市"
_CITY_POS = "市_pos"

_COUNTY = "区县"
_COUNTY_POS = "区县_pos"

_TOWN = "乡镇街道"
_TOWN_POS = "乡镇_pos"

_ADDR = "地址"

_ADCODE = "adcode"

_POS_KEY = {
    _PROVINCE: _PROVINCE_POS,
    _CITY: _CITY_POS,
    _COUNTY: _COUNTY_POS,
    _TOWN: _TOWN_POS
}

rank2name = [_PROVINCE, _CITY, _COUNTY, _TOWN]
rank2pos_key = [_PROVINCE_POS, _CITY_POS, _COUNTY_POS, _TOWN_POS]


class AddrInfo:

    RANK_PROVINCE = 0
    RANK_CITY = 1
    RANK_COUNTY = 2
    RANK_TOWN = 3
    RANK_STREET = 4

    def __init__(self, name, adcode, longitude, latitude) -> None:
        self.name = name
        # adcode总共12位，前 6 位代表省市区三级，后面3位代表乡镇，最后3位可能为村/街道
        self.adcode = adcode
        self.longitude = longitude
        self.latitude = latitude

        # rank 代表行政区划级别 0: 省 1: 市 2: 县
        if self.adcode[:6].endswith("0000"):
            self.rank = AddrInfo.RANK_PROVINCE
        elif self.adcode[:6].endswith("00"):
            self.rank = AddrInfo.RANK_CITY
        elif self.adcode.endswith("000000"):
            self.rank = AddrInfo.RANK_COUNTY
        elif self.adcode.endswith("000"):
            self.rank = AddrInfo.RANK_TOWN
        else:
            self.rank = AddrInfo.RANK_STREET

    def belong_to(self, other):
        """通过 adcode 判断当前 addr 是否属于 other"""
        return self.adcode.startswith(other.adcode[:(other.rank+1) * 2])


# 停用词包括: 省, 市, 特别行政区, 自治区.
# 之所以 区 和 县 不作为停用词，是因为 区县 数目太多, 去掉 "区" 字 或者 "县" 字后很容易误配
def _init_data(stop_key="([省市]|特别行政区|自治区)$") -> (dict, Matcher, dict):
    # 加载自定义高优映射词典
    # myumap: 当只有区的信息时， 且该区存在同名时， 指定该区具体是哪一个，字典的 key 为区名，value 为 adcode， 比如 {"朝阳区": "110105"}
    myumap = {}
    with open('src/resources/myumap.csv', encoding='utf-8', errors='ignore') as fp:
        lines = fp.read()
        for l in lines.split('\n'):
            if not l.startswith('#'):
                fields = l.strip().split(',')
                myumap[fields[0]] = fields[1]


    # 加载特殊的简写（字典中无法匹配的情况）,主要是几个少数民族自治区
    special_abbre = {}
    with open('src/resources/special_abbre.csv', encoding='utf-8', errors='ignore') as fp:
        lines = fp.read()
        for l in lines.split('\n'):
            if not l.startswith('#'):
                fields = l.strip().split(',')
                special_abbre[fields[0]] = fields[1]

    # 加载名称黑名单，此名单内的不必须匹配后缀（主要是省事），比如合作市
    black_names = []
    with open('src/resources/black_names', encoding='utf-8', errors='ignore') as fp:
        lines = fp.read()
        for l in lines.split('\n'):
            if not l.startswith('#'):
                black_names.append(l.strip())

    # 加载机构黑名单，此名单内的返回第二列的
    black_orgs = []
    with open('src/resources/black_orgs', encoding='utf-8', errors='ignore') as fp:
        lines = fp.read()
        for l in lines.split('\n'):
            if not l.startswith('#'):
                black_orgs.append(l.strip())

    # 加载全球地域及行政代码
    ad_map = {}
    matcher = Matcher(stop_key, special_abbre, black_names, black_orgs)
    from pkg_resources import resource_stream
    with resource_stream(__name__, 'resources/location_codes_level4_global.csv') as csv_stream:
        from io import TextIOWrapper
        import csv
        text = TextIOWrapper(csv_stream, encoding='utf8')
        adcodes_csv_reader = csv.DictReader(text)
        for record_dict in adcodes_csv_reader:
            addr_info = AddrInfo(
                name=record_dict["name"],
                adcode=record_dict["adcode"],
                longitude=record_dict["longitude"],
                latitude=record_dict["latitude"])
            if len(record_dict["name"]) <= 1:   #为了避免1个字地域名称的产生错误匹配，剔除
                continue
            ad_map[record_dict["adcode"]] = addr_info
            matcher.add_addr_info(addr_info)
    matcher.complete_add()

    return ad_map, matcher, myumap


ad_2_addr_dict, matcher, myumap = _init_data()


def transform(location_strs, index=None, pos_sensitive=False):
    """将地址描述字符串转换以"省","市","区"信息为列的DataFrame表格
        Args:
            locations:地址描述字符集合,可以是list, Series等任意可以进行for in循环的集合
                      比如:["徐汇区虹漕路461号58号楼5楼", "泉州市洛江区万安塘西工业区"]
            index:可以通过这个参数指定输出的DataFrame的index,默认情况下是range(len(data))
            pos_sensitive:如果为True则会多返回三列，分别提取出的省市区在字符串中的位置，如果字符串中不存在的话则显示-1

        Returns:
            一个Pandas的DataFrame类型的表格，如下：
               |省    |市   |区    |地址                 |adcode   |
               |上海市|市辖区|徐汇区|虹漕路461号58号楼5楼   |310104 |
               |福建省|泉州市|洛江区|万安塘西工业区        |350504 |
    """
    from collections.abc import Iterable

    if not isinstance(location_strs, Iterable):
        from .exceptions import InputTypeNotSuportException
        raise InputTypeNotSuportException(
            'location_strs参数必须为可迭代的类型(比如list, Series等实现了__iter__方法的对象)')

    import pandas as pd
    result = pd.DataFrame(
             [_get_one_addr(sentence, pos_sensitive) for sentence in location_strs],
             index=index)

    return tidy_order(result, pos_sensitive)


def transform_text_with_addrs(text_with_addrs, index=None, pos_sensitive=False):
    """将含有多个地址的长文本中的地址全部提取出来
         Args:
             text_with_addrs: 一个字符串，里面可能含有多个地址
             index:可以通过这个参数指定输出的DataFrame的index,默认情况下是range(len(data))
             pos_sensitive:如果为True则会多返回三列，分别提取出的省市区在字符串中的位置，如果字符串中不存在的话则显示-1

    """
    import pandas as pd
    # result = pd.DataFrame(filter(lambda record: record[_ADCODE] is not None,_extract_addrs(text_with_addrs, pos_sensitive, truncate_pos=False,
    #                                      new_entry_when_not_belong=True)),index=index)  # filter 的判空似乎没有必要，还会引起下面的报错
    result = pd.DataFrame(_extract_addrs(text_with_addrs, pos_sensitive, truncate_pos=False, new_entry_when_not_belong=True),index=index)
    return tidy_order(result, pos_sensitive)


def tidy_order(df, pos_sensitive):
    """整理顺序,唯一作用是让列的顺序好看一些"""
    if pos_sensitive:
        return df.loc[:, (_PROVINCE, _CITY, _COUNTY,_TOWN, _ADDR, _ADCODE, _PROVINCE_POS, _CITY_POS,
                              _COUNTY_POS, _TOWN_POS)]
    else:
        return df.loc[:, (_PROVINCE, _CITY, _COUNTY, _TOWN, _ADDR, _ADCODE)]


class MatchInfo:

    def __init__(self, attr_infos, start_index, end_index) -> None:
        self.attr_infos = attr_infos
        self.start_index = start_index
        self.end_index = end_index


def empty_record(pos_sensitive: bool):
    empty = {_PROVINCE: None, _CITY: None, _COUNTY: None, _TOWN: None,  _ADDR: None, _ADCODE: None}
    if pos_sensitive:
        empty[_PROVINCE_POS] = -1
        empty[_CITY_POS] = -1
        empty[_COUNTY_POS] = -1
        empty[_TOWN_POS] = -1
    return empty


def pos_setter(pos_sensitive):
    def set_pos(res, rank, pos):
        res[rank2pos_key[rank]] = pos

    def empty(res, rank, pos): pass
    return set_pos if pos_sensitive else empty


def _get_one_addr(sentence, pos_sensitive):
    addrs = _extract_addrs(sentence, pos_sensitive)
    # 取首次出现的地址
    return next(addrs)


def _extract_addrs(sentence, pos_sensitive, truncate_pos=True, new_entry_when_not_belong=False) -> dict:
    """提取出 sentence 中的所有地址"""
    # 空记录
    if not isinstance(sentence, str) or sentence == '' or sentence is None or matcher.is_black_org(sentence):
        yield empty_record(pos_sensitive)
        return


    set_pos = pos_setter(pos_sensitive)

    # 从大地区向小地区匹配
    res = empty_record(pos_sensitive)
    last_info = None
    adcode = None
    truncate_index = -1
    for match_info in matcher.iter(sentence):
        # 当没有省市等上级地区限制时, 优先选择的区的 adcode
        first_adcode = matcher.get(myumap.get(match_info.origin_value))
        if first_adcode:
            first_adcode = first_adcode[1][0].adcode
        cur_addr = match_info.get_match_addr(last_info, first_adcode)
        if cur_addr:
            set_pos(res, match_info.get_rank(), match_info.start_index)
            last_info = cur_addr
            adcode = cur_addr.adcode
            truncate_index = match_info.end_index
            # 匹配到了level4级别停止
            if cur_addr.rank == AddrInfo.RANK_TOWN:
                update_res_by_adcode(res, adcode)
                res[_ADDR] = sentence[truncate_index + 1:] if truncate_pos else ""
                res[_ADCODE] = adcode
                yield res
                res = empty_record(pos_sensitive)
                last_info = None
                adcode = None
                truncate_index = -1
        elif new_entry_when_not_belong:
            # 当找不到可以匹配的地址时,新建新的数据项
            update_res_by_adcode(res, adcode)
            res[_ADDR] = sentence[truncate_index + 1:] if truncate_pos else ""
            res[_ADCODE] = adcode
            yield res
            addr = match_info.get_match_addr(None, first_adcode)
            res = empty_record(pos_sensitive)
            set_pos(res, match_info.get_rank(), match_info.start_index)
            last_info = addr
            adcode = addr.adcode
            truncate_index = match_info.end_index

    if adcode is None:
        yield res
        return

    update_res_by_adcode(res, adcode)
    res[_ADDR] = sentence[truncate_index + 1:] if truncate_pos else ""
    res[_ADCODE] = adcode
    yield res


def _fill_adcode(adcode):
    if len(adcode) <= 12:
        return '{:0<12s}'.format(adcode)
    return adcode

def adcode_name(part_adcode: str):
    addr = ad_2_addr_dict.get(_fill_adcode(part_adcode))
    return None if addr is None else addr.name


def update_res_by_adcode(res: dict, adcode: str):

    if len(adcode) > 12:
        res[_PROVINCE] = adcode_name(adcode)
        res[_PROVINCE] = '国际'
        res[_CITY] = '国际'
        return

    if adcode[:6].endswith("0000"):
        res[_PROVINCE] = adcode_name(adcode[:2])
        return

    if adcode[:6].endswith("00"):
        res[_PROVINCE] = adcode_name(adcode[:2])
        res[_CITY] = adcode_name(adcode[:4])
        return

    if adcode.endswith("000000"):
        res[_PROVINCE] = adcode_name(adcode[:2])
        res[_CITY] = adcode_name(adcode[:4])
        res[_COUNTY] = adcode_name(adcode[:6])
        return

    if adcode.endswith("000"):
        res[_PROVINCE] = adcode_name(adcode[:2])
        res[_CITY] = adcode_name(adcode[:4])
        res[_COUNTY] = adcode_name(adcode[:6])
        res[_TOWN] = adcode_name(adcode[:9])
        return
