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
    df_text = df_text.drop('text', axis=1)

    df_text.to_csv(save_path, index = False)
    print('save result to:', save_path)

    df_p = df_text[df_text['province'] == df_text[_PROVINCE]]
    df_c = df_text[df_text['city'] == df_text[_CITY]]
    print('province match:', len(df_p), len(df_text), len(df_p) / len(df_text))
    print('city match:', len(df_c), len(df_text), len(df_c) / len(df_text))

    df_diff = df_text[(df_text['province'] != df_text[_PROVINCE]) | (df_text['city'] != df_text[_CITY])]
    df_diff.to_csv(save_path + '.diff.csv', index=False)


def test():
    # location = "为加快推进乡村振兴战略落地生根，金塔镇新时代文明实践所紧盯镇党委政府提升人居环境工作要求，以“洁、净、美”为目标，积极发动全镇广大党员群众及志愿者服务队开展人居环境整治活动，为乡村振兴“美颜”，为幸福生活助力。持续在镇村开展了“全域无垃圾”集中宣传活动，在微信交流群、显示屏滚动播放宣传标语11条，并依托各村新时代文明实践站，将环境卫生整治融入群众日常生产生活过程，营造了“环境关系你我他，垃圾治理靠大家”的浓厚氛围，使“清洁家园、从我做起”的理念深入人心，强化广大人民群众在环境卫生整治中的主体意识，吸引发动全镇干部群众主动参与到全域无垃圾治理中来。开展环境卫生整治以来，金塔镇进一步加大人力财力投入，建成垃圾集中收集点24个，依托各村新时代文明实践志愿者服务队，结合每村确立的3-5人公益性岗位，为环境卫生的彻底整治提供了人员保证。今年以来，金塔镇坚持以春季绿化植树、环保问题整改等为整治重点，充分发挥党员干部示范引领作用和村级保洁员的职能作用，积极引导群众广泛参与，切实做到了高标准、严要求、全覆盖，不断推进人居环境整治工作逐步提升。以酒航路、金梧路、金羊路沿线为重点，宣传引导带动人员对辖区内硬化路边、田间地头和房前屋后的垃圾柴草进行大整治、大清理，调运机械车辆25辆，清理各类垃圾2500余方，清理卫生死角400余处。在抓好环境卫生集中整治的基础上，各村还修订完善了5项环境卫生整治长效机制，并依托新时代文明实践活动落细落实，结合巾帼家美积分超市建设、志愿者服务时长激励机制等合理设置奖励积分，提高群众参与环境卫生整治的积极性，保障了环境卫生整治长效有序进行。"
    # location = '''<div class="rich_media_content js_underline_content                       autoTypeSetting24psection            " id="js_content"><section><section powered-by="xiumi.us"><section><img class="rich_pages wxw-img" data-ratio="1.4333333333333333" data-s="300,640" src="https://rmrbcmsonline.peopleapp.com/rb_recsys/img/2023/0601/c87aef477dcbaac8fbc2f459b46b9029_848561977478344704.jpeg" data-type="jpeg" data-w="900"></section></section><section powered-by="xiumi.us"><section><section powered-by="xiumi.us"><p>5月31日上午，在“六一”国际儿童节到来之际，习近平总书记来到北京育英学校，看望慰问师生，向全国广大少年儿童祝贺节日。习近平总书记强调，少年儿童是祖国的未来，是中华民族的希望。新时代中国儿童应该是有志向、有梦想，爱学习、爱劳动，懂感恩、懂友善，敢创新、敢奋斗，德智体美劳全面发展的好儿童。希望同学们立志为强国建设、民族复兴而读书，不负家长期望，不负党和人民期待。</p><p><br></p><p>　　北京育英学校是一所同新中国一起成长的学校。</p><p><br></p><p>　　北京育英学校1948年创办于河北西柏坡，前身为“中共中央直属机关供给部育英小学校”。校名中的“育英”二字，取自于《孟子·尽心上》“得天下英才而教育之”。1949年，学校随中共中央一起迁入北京。</p><p><br></p><p>　　自建校以来，育英学校一直受到党中央的亲切关怀。1952年“六一”国际儿童节，毛泽东同志为学校题词“好好学习 好好学习”。不少党和国家领导人也曾对学校发展作出过重要指示。</p><p><br></p><p>　　走过70余载光阴，如今，育英学校已成为一所涵盖小学、初中、高中学段的集团化学校。学校始终坚持党的教育方针，全面落实立德树人根本任务，以“行为规范、热爱学习、阳光大气、关心社稷、勇于担当”作为学生培养目标，不断践行为党育人、为国育才的初心和使命。</p><p><br></p><p>　　传承红色基因，学校以“弘扬我国优秀传统文化和红色文化”为主旨，积极建设学校文化，推动一代代学生在文化的浸润和熏陶中，补足成长的“精神之钙”；</p><p><br></p><p>　　紧跟时代步伐，学校以服务国家经济社会发展作为人才培养方向，并结合办学实际，聚焦“科学特色”高中建设，在全校师生中营造热爱科学的良好氛围。</p><p><br></p><p>　　办学至今，育英学校培养了数以万计的合格毕业生，其中不乏优秀专家学者、科技工作者，以及奋战在各行各业的领军人物和中坚力量。</p><p><br></p><p>　　今年“六一”前夕，习近平总书记来到育英学校，体现了总书记对我国教育事业一以贯之的重视，对少年儿童茁壮成长深深的牵挂。</p><p><br></p><p>　　从北京市海淀区民族小学，到重庆市石柱土家族自治县中益乡小学；从陕西省平利县老县镇中心小学，到湖南省汝城县文明瑶族乡第一片小学……党的十八大以来，习近平总书记多次在考察调研中走进小学，来到少年儿童中间。</p><p><br></p><p>　　“坚持德智体美劳全面发展”“人生最重要的志向应该同祖国和人民联系在一起”“现在孩子普遍眼镜化，这是一个隐忧”“要树立健康第一的教育理念”……习近平总书记的殷切期望和谆谆教诲，为孩子们的成长提供了阳光雨露。</p><p><br></p><p>　　今年全国两会期间，习近平总书记参加江苏代表团审议。“新安旅行团”的母校淮安市新安小学校长张大冬代表，向总书记介绍了学校传承红色基因，引领孩子们从小听党话、跟党走的情况。</p><p><br></p><p>　　“基础教育，承担着非常光荣艰巨的历史任务。”习近平总书记强调，新时代教育工作者要努力把青少年培养成为中国特色社会主义的建设者和接班人。</p><p><br></p><p>　　就在两天前，习近平总书记在中共中央政治局第五次集体学习时，再次对基础教育提出要求：“基础教育既要夯实学生的知识基础，也要激发学生崇尚科学、探索未知的兴趣，培养其探索性、创新性思维品质。”</p><p><br></p><p>　　如今的育英学校不断进行校园环境改造，图书馆、阅读馆、艺体馆等硬件设施齐备，为学生全面发展提供优质物质保障。与此同时，学校还建立起一流的科学实验室，为特色学科发展提供强大支撑，也为教育服务国家经济社会发展打下坚实基础。</p><p><br></p><p>　　由育英学校放眼全国，我国教育普及水平实现历史性跨越，各级教育普及程度达到或超过中高收入国家平均水平，其中学前教育、义务教育达到世界高收入国家平均水平，教育事业迸发蓬勃活力，升腾崭新气象。</p><p><br></p><p>　　教育兴则国家兴，教育强则国家强。</p><p><br></p></section></section></section><p powered-by="xiumi.us"><br></p><section powered-by="xiumi.us"><section><section powered-by="xiumi.us"><section><section powered-by="xiumi.us"><p><br></p></section></section></section></section><section><section powered-by="xiumi.us"><section></section></section></section><section><section powered-by="xiumi.us"><section><section powered-by="xiumi.us"><p><br></p></section></section></section></section></section><section powered-by="xiumi.us"><p><br></p><p>记者：王鹏</p><p>视觉 | 编辑：张爱芳、王秋韵</p><p>新华社国内部出品</p><p><br></p></section></section><p><mp-style-type data-value="3" /></p></div>'''
    location = '''<div class="rich_media_content js_underline_content                       autoTypeSetting24psection            " id="js_content"><h1 data-mpa-powered-by="yiban.io"><img class="rich_pages wxw-img __bg_gif" data-backh="155" data-backw="500" data-galleryid="" data-ratio="0.31" src="https://rmrbcmsonline.peopleapp.com/rb_recsys/img/2023/0601/f37399361ea24fc26daafda12b4e9a45_848561979617439744.gif" data-type="gif" data-w="500"></h1><section><img class="__bg_gif rich_pages wxw-img" data-fileid="506815744" data-ratio="0.1875" src="https://rmrbcmsonline.peopleapp.com/rb_recsys/img/2023/0601/f37399361ea24fc26daafda12b4e9a45_848561980439523328.gif" data-type="gif" data-w="128" data-width="10%"><span>&nbsp;</span></section><section><span>5月31日，市委书记马占才调研督导环城北路项目建设及沿线绿化美化、人居环境改善、环境风貌整治工作，并针对现场发现的问题逐一分析研究提出具体工作要求。市政府副市长马继超，市领导马忠国一同调研。</span><span></span></section><section><img class="rich_pages wxw-img" data-cropselx1="0" data-cropselx2="578" data-cropsely1="0" data-cropsely2="385" data-galleryid="" data-ratio="0.6666666666666666" data-s="300,640" src="https://rmrbcmsonline.peopleapp.com/rb_recsys/img/2023/0601/f37399361ea24fc26daafda12b4e9a45_848561981341298688.jpeg" data-type="jpeg" data-w="1080"><span></span></section><section><span></span></section><section><span>马占才强调，环城北路是着眼建设四通八达的畅通临夏、加快建成全市“一环、五横、十纵”路网格局谋划实施的重点交通项目，属地镇村和部门要紧密结合“三抓三促”行动，把道路沿线绿化美化、环境风貌整治、人居环境改善等作为提升城市品位、推动民生改善的重要抓手，加强公路沿线环境卫生专项整治，提升区域绿化水平和景观档次，确保道路沿线整洁有序、生态优美，以点带面充分展现临夏市魅力花都·公园城市形象。要坚持路景相融，依托山体走势、自然环境和道路护坡，高起点高标准谋划实施造林绿化、环境美化、墙体艺术设计、拆旧排危、棚户区改造等工作，真正把环城北路打造成集交通、景观、生态、人文于一体的靓丽风景廊道。要提高工作效率，加快施工进度，凝集各方合力，及时协调解决项目建设过程中存在的困难问题，确保工程按照时间节点高标准、高质量实施。</span><span></span></section><p><img class="rich_pages wxw-img js_insertlocalimg" data-ratio="0.6666666666666666" data-s="300,640" src="https://rmrbcmsonline.peopleapp.com/rb_recsys/img/2023/0601/f37399361ea24fc26daafda12b4e9a45_848561982360514560.jpeg" data-type="jpeg" data-w="1080"></p><section><span></span></section><section><span></span></section><section><span>市直相关部门单位参加。</span></section><section data-role="paragraph"><section><section data-width="100%"><section><span><section><span>记者：朱振斌 王宇辰 马孝诚</span></section><section><span>新媒体编辑：马亚飞</span></section><section><span>审核：朱琳&nbsp;蒲晶晶 沙腾</span></section></span></section></section></section></section><section data-role="paragraph"><p><img class="rich_pages wxw-img" data-ratio="1.1925925925925926" data-s="300,640" src="https://rmrbcmsonline.peopleapp.com/rb_recsys/img/2023/0601/f37399361ea24fc26daafda12b4e9a45_848561983476199424.jpeg" data-type="jpeg" data-w="1080"></p><section><img class="rich_pages wxw-img" data-ratio="0.5" src="https://rmrbcmsonline.peopleapp.com/rb_recsys/img/2023/0601/f37399361ea24fc26daafda12b4e9a45_848561984482832384.jpeg" data-type="jpeg" data-w="1080" data-width="100%"></section><section><strong><span>转载请注明出处！</span></strong></section><section><section><section><section><section><img class="__bg_gif rich_pages wxw-img" data-fileid="506819166" data-type="gif" data-width="100%" data-ratio="0.6865079365079365" data-w="252" src="https://rmrbcmsonline.peopleapp.com/rb_recsys/img/2023/0601/f37399361ea24fc26daafda12b4e9a45_848561985451716608.gif"></section><section data-brushtype="text" hm_fix="261:339"><strong>点分享！点收藏！点点赞！点在看<strong>！</strong></strong></section></section></section></section></section></section><p><mp-style-type data-value="3" /></p></div>'''
    locations = ["荷兰限制进口？对来自欧盟以外的不安全产品实施更严格的规定",
                 "5月31日上午，在“六一”国际儿童节到来之际，习近平总书记来到北京育英学校，看望慰问师生，向全国广大少年儿童祝贺节日。习近平总书记强调，少年儿童是祖国的未来，是中华民族的希望。新时代中国儿童应该是有志向、有梦想，爱学习、爱劳动，懂感恩、懂友善，敢创新、敢奋斗，德智体美劳全面发展的好儿童。希望同学们立志为强国建设、民族复兴而读书，不负家长期望，不负党和人民期待。</p><p><br></p><p>　　北京育英学校是一所同新中国一起成长的学校。</p><p><br></p><p>　　北京育英学校1948年创办于河北西柏坡，前身为“中共中央直属机关供给部育英小学校”。校名中的“育英”二字，取自于《孟子·尽心上》“得天下英才而教育之”。1949年，学校随中共中央一起迁入北京。</p><p><br></p><p>　　自建校以来，育英学校一直受到党中央的亲切关怀。1952年“六一”国际儿童节，毛泽东同志为学校题词“好好学习 好好学习”。不少党和国家领导人也曾对学校发展作出过重要指示。</p><p><br></p><p>　　走过70余载光阴，如今，育英学校已成为一所涵盖小学、初中、高中学段的集团化学校。学校始终坚持党的教育方针，全面落实立德树人根本任务，以“行为规范、热爱学习、阳光大气、关心社稷、勇于担当”作为学生培养目标，不断践行为党育人、为国育才的初心和使命。</p><p><br></p><p>　　传承红色基因，学校以“弘扬我国优秀传统文化和红色文化”为主旨，积极建设学校文化，推动一代代学生在文化的浸润和熏陶中，补足成长的“精神之钙”；</p><p><br></p><p>　　紧跟时代步伐，学校以服务国家经济社会发展作为人才培养方向，并结合办学实际，聚焦“科学特色”高中建设，在全校师生中营造热爱科学的良好氛围。",
                 "昭觉县气象台2023年05月31日23时55分解除雷电黄色预警信号。", "目前影响我市的强雷雨云团已明显减弱，陆丰市气象台2023年5月31日23时50分解除陆丰市、华侨区暴雨黄色预警信号。",
                 location]

    df = transform([location], pos_sensitive=True, umap=myumap)  # cut=False 会造成效率低下
    # df = transform([location], pos_sensitive=True, cut=False, umap=myumap)    # cut=False 会造成效率低下
    df2 = transform_text_with_addrs(location)

    # import pdb
    # pdb.set_trace()
    print(df,'\n')
    print(df2)

    # print('\n\n\n')
    # df = pd.DataFrame(locations, columns=['text'])
    # df[[_PROVINCE,_CITY,_COUNTY,_TOWN]] = df['text'].apply(lambda x: infer(x)).apply(pd.Series)
    # print(df)




    # 省_pos，市_pos和区_pos三列大于-1的部分就代表提取的位置。-1则表明这个字段是靠程序推断出来的，或者没能提取出来。

    # cpca.province_area_map.get_relational_addrs(('江苏省', '鼓楼区'))

    # from cpca import drawer
    #  drawer.draw_locations(df[cpca._ADCODE], "df.html")   # df为上一段代码输出的df

if __name__ == '__main__':
    test()
    # batch_infer('data/origin_files/article_m6_half.txt', 'data/result/article_m6_half.csv', sep='^')