#coding:utf-8
import sys
import os
root_path = os.path.abspath(os.path.dirname(__file__)).split('src')[0]
sys.path.append(root_path)
from src import transform, transform_text_with_addrs
from src import _PROVINCE, _CITY, _COUNTY, _TOWN, myumap

import pandas as pd
import re

def read_file(input_file, sep=","):
    print('opening file: ', input_file)
    try:
        df = pd.read_csv(input_file, header=0, low_memory=False, sep = sep)  # header=None表示原始文件数据没有列索引，这样的话read_csv会自动加上列索引
    except Exception as e:
        print("===================================> read_file " + input_file + " exception: " + str(e))
        return None
    print('len_csv', len(df))
    return df

def read_dir(input_dir, is_subdir=False, sep=','):
    l = 0
    df1 = pd.DataFrame()
    file_list = os.listdir(input_dir)
    for file in file_list:  # 遍历文件夹
        if os.path.isdir(input_dir + "/" + file):
            if is_subdir:
                sub_file_list = os.listdir(input_dir + "/" + file)
                print("打开文件：", input_dir + "/" + file, "文件数目", len(sub_file_list))
                for fs in sub_file_list:
                    if not os.path.isdir(input_dir + "/" + file + "/" + fs):
                        print('打开文件：', input_dir + "/" + file + "/" + fs)
                        df2 = read_file(input_dir + "/" + file + "/" + fs, sep=sep)
                        if df2 is not None:
                            l = l + len(df2)
                            df1 = pd.concat([df1, df2], axis=0, ignore_index=True)  # 将df2数据与df1合并
        else:
            df2 = read_file(input_dir + "/" + file)
            if df2 is not None:
                l = l + len(df2)
                df1 = pd.concat([df1, df2], axis=0, ignore_index=True)  # 将df2数据与df1合并
    # df1.to_csv("data.csv")
    print(len(df1))
    return df1


def extract_cn(line, with_punctuation=True):
    if not isinstance(line, str):
        return ''
    line = re.sub('\r|\n|\t', '', line)
    if with_punctuation:
        cop = re.compile("[^\u4e00-\u9fa5^，。！：；？]")     # 带标点
        #cop = re.compile("[^\u4e00-\u9fa5^a-z^，。！：；？]")     # 带标点带英文
    else:
        cop = re.compile("[^\u4e00-\u9fa5]")                # 去除标点
    return cop.sub("", line)


def set_df_text(df, field_list, cn_clean):
    if df is None or not set(field_list).issubset(set(df.columns)):
        print("during split field_list exception: ", set(field_list), set(df.columns),
                set(field_list).issubset(set(df.columns)))
        return
    # df.dropna(axis=0, how='any', subset=field_list, inplace=True)
    for index, f in enumerate(field_list):
        if cn_clean:
            df[f] = df[f].apply(lambda x: extract_cn(x))
        if index == 0:
            df['text'] = df[f]
        else:
            df['text'] = df['text'] + df[f]
    # index_names = df[(df['text'] is None) | (df['text'] == '') | (pd.isna(df['text']) is True)].index
    # print('drop exception index_names:', index_names)
    # drop these given row indexes from dataFrame
    # df.drop(index_names, inplace=True)

    df = df.dropna(subset=['text'])      # 去除 text 为空的行，不能用来预测，会报错

    print("set_df_text len:", len(df))
    return df

def str_none(x):
    if x is None:
        return ''
    return x

def infer(sentence):
    df = transform([sentence], pos_sensitive=False, umap=myumap)

    loc = df.loc[0] #暂时先设置策略选取第一个

    province = loc[_PROVINCE]
    city = loc[_CITY]
    county = loc[_COUNTY]
    town = loc[_TOWN]

    # return '|'.join([str_none(province), str_none(city), str_none(county), str_none(town)])
    return [str_none(province), str_none(city), str_none(county), str_none(town)]


def batch_infer(data_path, save_path, sep=','):
    if data_path.find('.') > 0:  # 有后缀则认为是文件，因为文件必须带后缀
        df = read_file(data_path, sep=sep)
    else:
        df = read_dir(data_path, is_subdir=False, sep=sep)
    df_text = set_df_text(df, ['title', 'content'], cn_clean=False)
    # import pdb
    # pdb.set_trace()

    df_text[[_PROVINCE,_CITY,_COUNTY,_TOWN]] = df_text['text'].apply(lambda x: infer(x)).apply(pd.Series)
    df_text.to_csv(save_path, index = False)
    print('save result to:', save_path)

    df_p = df_text[df_text['province'] == df_text[_PROVINCE]]
    df_c = df_text[df_text['city'] == df_text[_CITY]]
    print('province match:', len(df_p), len(df_text), len(df_p) / len(df_text))
    print('city match:', len(df_c), len(df_c), len(df_c) / len(df_c))


def test():
    location = "为加快推进乡村振兴战略落地生根，金塔镇新时代文明实践所紧盯镇党委政府提升人居环境工作要求，以“洁、净、美”为目标，积极发动全镇广大党员群众及志愿者服务队开展人居环境整治活动，为乡村振兴“美颜”，为幸福生活助力。持续在镇村开展了“全域无垃圾”集中宣传活动，在微信交流群、显示屏滚动播放宣传标语11条，并依托各村新时代文明实践站，将环境卫生整治融入群众日常生产生活过程，营造了“环境关系你我他，垃圾治理靠大家”的浓厚氛围，使“清洁家园、从我做起”的理念深入人心，强化广大人民群众在环境卫生整治中的主体意识，吸引发动全镇干部群众主动参与到全域无垃圾治理中来。开展环境卫生整治以来，金塔镇进一步加大人力财力投入，建成垃圾集中收集点24个，依托各村新时代文明实践志愿者服务队，结合每村确立的3-5人公益性岗位，为环境卫生的彻底整治提供了人员保证。今年以来，金塔镇坚持以春季绿化植树、环保问题整改等为整治重点，充分发挥党员干部示范引领作用和村级保洁员的职能作用，积极引导群众广泛参与，切实做到了高标准、严要求、全覆盖，不断推进人居环境整治工作逐步提升。以酒航路、金梧路、金羊路沿线为重点，宣传引导带动人员对辖区内硬化路边、田间地头和房前屋后的垃圾柴草进行大整治、大清理，调运机械车辆25辆，清理各类垃圾2500余方，清理卫生死角400余处。在抓好环境卫生集中整治的基础上，各村还修订完善了5项环境卫生整治长效机制，并依托新时代文明实践活动落细落实，结合巾帼家美积分超市建设、志愿者服务时长激励机制等合理设置奖励积分，提高群众参与环境卫生整治的积极性，保障了环境卫生整治长效有序进行。"
    locations = ["昭觉县气象台2023年05月31日23时55分解除雷电黄色预警信号。", "目前影响我市的强雷雨云团已明显减弱，陆丰市气象台2023年5月31日23时50分解除陆丰市、华侨区暴雨黄色预警信号。",
                 location]

    df = transform(locations, pos_sensitive=True, umap=myumap)  # cut=False 会造成效率低下
    # df = cpca.transform(locations, pos_sensitive=True, cut=False, umap=myumap)    # cut=False 会造成效率低下
    # df2 = transform_text_with_addrs(location)

    # import pdb
    # pdb.set_trace()
    print(df)

    print('\n\n\n')

    df = pd.DataFrame(locations, columns=['text'])
    # import pdb
    # pdb.set_trace()
    df[[_PROVINCE,_CITY,_COUNTY,_TOWN]] = df['text'].apply(lambda x: infer(x)).apply(pd.Series)
    print(df)

    # 省_pos，市_pos和区_pos三列大于-1的部分就代表提取的位置。-1则表明这个字段是靠程序推断出来的，或者没能提取出来。

    # cpca.province_area_map.get_relational_addrs(('江苏省', '鼓楼区'))

    # from cpca import drawer
    #  drawer.draw_locations(df[cpca._ADCODE], "df.html")   # df为上一段代码输出的df

if __name__ == '__main__':
    # test()
    batch_infer('data/origin_files/article_m6_half.txt', 'data/result/article_m6_half.csv', sep='^')