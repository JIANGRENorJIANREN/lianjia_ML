#-*- coding:utf-8 -*-

import urllib.request
import pandas as pd
import lxml.html
import time
import re
import html
import threading

def downloader(url, retries):
    print('downloading %s'%url)
    try:
        html = urllib.request.urlopen(url).read()
    except urllib.request.URLError as e:
        print('download %s error'%url)
        html = None
        if retries > 0:
            if hasattr(e, 'code') and 500 <= e.code < 600:
                return downloader(url, retries-1)
    return html



def lianjia_crawler(max_threads = 10):
    '''
    :param max_threads: 最大线程数量
    :return:数据存入csv文件
    '''
    # 爬取链接集合
    urls = []
    for i in range(1, 101):
        urls.append('https://cd.lianjia.com/ershoufang/pg{0}/'.format(i))

    def process_queue():
        while True:
            try:
                url = urls.pop()
            except IndexError:
                print('download completed')
                break
            else:
                h = downloader(url, 5)
                if html is not None:
                    houses = {}
                    tree = lxml.html.fromstring(h)

                    # 标题
                    title = tree.cssselect('li.clear > div.info.clear > div.title')
                    title = list(map(lxml.html.HtmlElement.text_content, title))
                    houses['title'] = title

                    # 房屋简介
                    houseInfo = tree.cssselect('li.clear > div.info.clear > div.address > div.houseInfo')
                    houseInfo = list(map(lxml.html.HtmlElement.text_content, houseInfo))
                    houses['houseInfo'] = houseInfo

                    # 楼层信息
                    positionInfo = tree.cssselect('li.clear > div.info.clear > div.flood > div.positionInfo')
                    positionInfo = list(map(lxml.html.HtmlElement.text_content, positionInfo))
                    houses['positionInfo'] = positionInfo

                    # 地铁, 交税信息, 看房时间
                    tag = re.findall('<div class="tag">(.*?)</div><div class="priceInfo">', html.unescape(h.decode()))
                    subway = []
                    taxfree = []
                    haskey = []
                    for x in tag:
                        # 地铁
                        swy = re.findall('<span class="subway">(.*?)</span>', str(x))
                        subway.extend(swy) if len(swy) else subway.extend('无')
                        # 交税信息
                        tf = re.findall('<span class="taxfree">(.*?)</span>', str(x))
                        taxfree.extend(tf) if len(tf) else taxfree.extend('无')
                        # 看房时间
                        hk = re.findall('<span class="haskey">(.*?)</span>', str(x))
                        haskey.extend(hk) if len(hk) else haskey.extend('无')

                    houses['subway'] = subway
                    houses['taxfree'] = taxfree
                    houses['haskey'] = haskey

                    # 总价
                    totalPrice = tree.cssselect('li.clear > div.info.clear > div.priceInfo > div.totalPrice')
                    totalPrice = list(map(lxml.html.HtmlElement.text_content, totalPrice))
                    houses['totalPrice'] = totalPrice

                    # 单价
                    unitPrice = tree.cssselect('li.clear > div.info.clear > div.priceInfo > div.unitPrice')
                    unitPrice = list(map(lxml.html.HtmlElement.text_content, unitPrice))
                    houses['unitPrice'] = unitPrice

                    # 将房屋信息转换为DataFrame格式
                    df = pd.DataFrame(houses)

                    df.to_csv('/home/wangf/PycharmProjects/lianjiawang/house_infoes.csv', mode='a')

    threads = []
    #开始爬取
    while urls or threads:
        for thread in threads:
            if not thread.is_alive():
                threads.remove(thread)

        while urls and len(threads) < max_threads:
            thread = threading.Thread(target=process_queue)
            thread.setDaemon(True)
            thread.start()
            threads.append(thread)
        time.sleep(1)


