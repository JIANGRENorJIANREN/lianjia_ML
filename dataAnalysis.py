# -*- coding:utf-8 -*-

import pandas as pd

#数据加工处理
def handle_datas(path):
    '''

    :param path: 需要处理的文件的路径
    :return:处理好的文件
    '''
    datas = pd.read_csv(path)

    #将houseInfo拆解，提取有用信息
    houseInfo = list(datas['houseInfo'])
    temp = []
    for x in houseInfo:
        temp.append(x.split('|'))
    df = pd.DataFrame(temp, columns=['houseName', 'room_counts', 'area', 'direction', 'else1', 'else2', 'elevator'])
    datas = pd.concat([datas, df], axis=1)

    #拆解positionInfo，提取有用信息
    positionInfo = list(datas['positionInfo'])
    temp = []
    for x in positionInfo:
        temp.append(x.split('-'))
    df = pd.DataFrame(temp, columns=['position', 'address'])
    datas = pd.concat([datas, df], axis=1)

    #删除无用行
    index_deleted = datas[datas['haskey'] == 'haskey'].index   #寻找所有haskey列中值为haskey的元素的行的index(有点拗口)
    datas = datas.drop(index_deleted)

    # 删除列'Unkown name:0, taxfree, haskey, title, houseInfo, positionInfo'
    datas = datas.drop(columns=[datas.columns[0]])
    datas = datas.drop(columns=['taxfree', 'haskey', 'title', 'houseInfo', 'positionInfo'], axis=1)

    datas.to_csv('/home/wangf/PycharmProjects/lianjiawang/houseInfo_clean.csv')


