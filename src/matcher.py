import ahocorasick
import re


class MatchInfo:

    def __init__(self, attr_infos, start_index, end_index, origin_value) -> None:
        self.attr_infos = attr_infos
        self.start_index = start_index
        self.end_index = end_index
        self.origin_value = origin_value

    def get_match_addr(self, parent_addr, first_adcode=None):
        if parent_addr:
            return next(filter(lambda attr: attr.belong_to(parent_addr), self.attr_infos), None)
        elif first_adcode:
            res = next(filter(lambda attr: attr.adcode[:2] == first_adcode[:2], self.attr_infos), None)
            return res if res else self.attr_infos[0]
        else:
            return self.attr_infos[0]
        # 此处如果想全部返回，则有待优化

    def get_rank(self):
        return self.attr_infos[0].rank

    def get_one_addr(self):
        return self.attr_infos[0]

    def __repr__(self) -> str:
        return "from {} to {} value {}".format(self.start_index, self.end_index, self.origin_value)


class Matcher:

    def __init__(self, stop_re, special_abbre, black_names, black_orgs):
        self.ac = ahocorasick.Automaton() #创建一个自动机,提取出包含知识库中实体的所有子串
        self.stop_re = stop_re
        self.special_abbre = special_abbre
        self.black_names = black_names
        self.black_orgs = black_orgs

    def _abbr_name(self, origin_name):
        names = [origin_name]
        name = self.special_abbre.get(origin_name, 0)
        if name and name != origin_name:
            names.append(name)

        # stop_key="([省市]|特别行政区|自治区)$")
        # 将stop_re内匹配到的进行替换，比如 北京市替换为背景，河北省替换为河北
        # 之所以 区 和 县 不作为停用词，是因为 区县 数目太多, 去掉 "区" 字 或者 "县" 字后很容易误配，所以比如四川于都，无法匹配，只能匹配四川于都县
        # 当前解决方案，白名单配置方式：将   于都县-->于都   配置到特殊名称里，将来可以手工整理一份县级名称列表配置进去
        name = re.sub(self.stop_re, '', origin_name)
        # 黑名单，对于省市的情况，不进行替换，比如合作市，不添加合作; 单个字的为了防止和正常混淆，不添加匹配
        if name in self.black_names or len(name) < 2 or name == origin_name:
            return names
        names.append(name)

        return names

    def _add_word(self, abbr_name, addr_info):
        share_list = []
        if not abbr_name in self.ac:
            share_list.append(addr_info)
            self.ac.add_word(abbr_name, (abbr_name, share_list))
        else:
            # 同名共享一个share_list, 此时不必 ac.add_word
            share_list = self.get(abbr_name)[1]
            share_list.append(addr_info)

    def add_addr_info(self, addr_info):
        # 区名可能重复,所以会添加多次
        abbr_names = self._abbr_name(addr_info.name)

        for abbr_name in abbr_names:
            self._add_word(abbr_name, addr_info)

    # 增加地址的阶段结束,之后不会再往对象中添加地址
    def complete_add(self):
        self.ac.make_automaton()

    def get(self, key):
        if key is None:
            return key
        return self.ac.get(key)

    def is_black_org(self, sentence):
        for org in self.black_orgs:
            if sentence.find(org) >= 0:
                return True
        return False

    def iter(self, sentence):
        prev_start_index = None
        prev_match_info = None
        prev_end_index = None
        for end_index, (original_value, attr_infos) in self.ac.iter(sentence):
            # print(end_index,original_value, attr_infos[0].name, attr_infos[0].adcode, attr_infos[0].rank)
            # start_index 和 end_index 是左闭右闭的
            start_index = end_index - len(original_value) + 1
            if prev_end_index is not None and end_index <= prev_end_index:
                continue

            cur_match_info = MatchInfo(attr_infos, start_index, end_index, original_value)
            # 如果遇到的是全称, 会匹配到两次, 简称一次, 全称一次,所以要处理下
            if prev_match_info is not None:
                if start_index == prev_start_index:
                    yield cur_match_info
                    prev_match_info = None
                else:
                    yield prev_match_info
                    prev_match_info = cur_match_info
            else:
                prev_match_info = cur_match_info
            prev_start_index = start_index
            prev_end_index = end_index

        if prev_match_info is not None:
            yield prev_match_info


