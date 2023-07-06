#coding:utf-8
import sys
import os
root_path = os.path.abspath(os.path.dirname(__file__)).split('cpca')[0]
sys.path.append(root_path)
from cpca import transform, transform_text_with_addrs, myumap

if __name__ == '__main__':

    location = "为加快推进乡村振兴战略落地生根，金塔镇新时代文明实践所紧盯镇党委政府提升人居环境工作要求，以“洁、净、美”为目标，积极发动全镇广大党员群众及志愿者服务队开展人居环境整治活动，为乡村振兴“美颜”，为幸福生活助力。持续在镇村开展了“全域无垃圾”集中宣传活动，在微信交流群、显示屏滚动播放宣传标语11条，并依托各村新时代文明实践站，将环境卫生整治融入群众日常生产生活过程，营造了“环境关系你我他，垃圾治理靠大家”的浓厚氛围，使“清洁家园、从我做起”的理念深入人心，强化广大人民群众在环境卫生整治中的主体意识，吸引发动全镇干部群众主动参与到全域无垃圾治理中来。开展环境卫生整治以来，金塔镇进一步加大人力财力投入，建成垃圾集中收集点24个，依托各村新时代文明实践志愿者服务队，结合每村确立的3-5人公益性岗位，为环境卫生的彻底整治提供了人员保证。今年以来，金塔镇坚持以春季绿化植树、环保问题整改等为整治重点，充分发挥党员干部示范引领作用和村级保洁员的职能作用，积极引导群众广泛参与，切实做到了高标准、严要求、全覆盖，不断推进人居环境整治工作逐步提升。以酒航路、金梧路、金羊路沿线为重点，宣传引导带动人员对辖区内硬化路边、田间地头和房前屋后的垃圾柴草进行大整治、大清理，调运机械车辆25辆，清理各类垃圾2500余方，清理卫生死角400余处。在抓好环境卫生集中整治的基础上，各村还修订完善了5项环境卫生整治长效机制，并依托新时代文明实践活动落细落实，结合巾帼家美积分超市建设、志愿者服务时长激励机制等合理设置奖励积分，提高群众参与环境卫生整治的积极性，保障了环境卫生整治长效有序进行。"
    locations = ["昭觉县气象台2023年05月31日23时55分解除雷电黄色预警信号。", "目前影响我市的强雷雨云团已明显减弱，陆丰市气象台2023年5月31日23时50分解除陆丰市、华侨区暴雨黄色预警信号。",
                 location ]
    df = transform(locations, pos_sensitive=True, umap=myumap)    # cut=False 会造成效率低下
    # df = cpca.transform(locations, pos_sensitive=True, cut=False, umap=myumap)    # cut=False 会造成效率低下

    # import pdb
    # pdb.set_trace()
    print(df)

    print('\n\n\n')

    # df2 = transform_text_with_addrs(location)
    # print(df2)

    # 省_pos，市_pos和区_pos三列大于-1的部分就代表提取的位置。-1则表明这个字段是靠程序推断出来的，或者没能提取出来。

    #cpca.province_area_map.get_relational_addrs(('江苏省', '鼓楼区'))

    # from cpca import drawer
    #  drawer.draw_locations(df[cpca._ADCODE], "df.html")   # df为上一段代码输出的df