# coding:utf-8
__author__ = 'xxj'

import sys
import time
import math
import os
from rediscluster import StrictRedisCluster
import json
import re
import Queue
import lxml.etree
import requests
import re
import datetime
import threading
from threading import Lock
from Queue import Empty
from requests.exceptions import ReadTimeout, ConnectionError, ConnectTimeout

reload(sys)
sys.setdefaultencoding('utf-8')
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36',
}
PROXY_IP_Q = Queue.Queue()     # 代理ip队列
KEYWORD_QUEUE = Queue.Queue()    # 关键词队列
THREAD_PROXY_MAP = {}    # 线程与代理关系


def get_proxy_ips(num):
    '''
    获取代理ip
    :return:
    '''
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36'
        }
        url = 'https://proxyapi.mimvp.com/api/fetchsecret.php?orderid=860068921904605585&num={num}&http_type=3&result_fields=1,2,3&result_format=json'.format(num=num)
        response = requests.get(url=url, headers=headers, timeout=10)
        ip_list = json.loads(response.text).get('result')
        if not ip_list:
            print time.strftime('[%Y-%m-%d %H:%M:%S]'), '获取代理ip异常：', response.text
            content_json = response.json()
            code_msg = content_json.get('code_msg')  # 异常信息
            code_msg = code_msg.encode('utf-8')
            search_obj = re.search(r'.*?，【(.*?)秒】', code_msg, re.S)
            stop_time = search_obj.group(1)
            stop_time = int(stop_time)
            print '代理ip接口限制,限制时间为：', stop_time, '秒'
            time.sleep(stop_time)
            return get_proxy_ips(num)
        for ip in ip_list:
            ip = ip.get('ip:port')
            proxies = {
                'http': "http://8c84700fa7d2:kgvavaeile@{ip}".format(ip=ip),
                # 'https': "http://8c84700fa7d2:kgvavaeile@{ip}".format(ip=ip)
            }
            PROXY_IP_Q.put(proxies)
    except BaseException as e:
        print 'BaseException：', '代理ip异常'
        time.sleep(60)
        return get_proxy_ips(num)


def get(url, desc, count, thread_name, lock):
    '''
    重试模块
    :param url:url
    :param desc: url描述
    :param count: 重试次数
    :param thread_name: 线程名称
    :param lock: 锁
    :return: 响应对象
    '''
    for i in xrange(count):
        proxies = THREAD_PROXY_MAP.get(thread_name)
        response = r(url, desc, i, proxies, thread_name, lock)
        if response is None:    # 异常
            pass
        elif response.status_code == 200:
            return response
    return None


def r(url, desc, i, proxies, thread_name, lock):
    try:
        print '{t} {desc} {url} count：{i} proxie：{proxies} thread_name：{thread_name}'.\
            format(t=time.strftime('[%Y-%m-%d %H:%M:%S]'), desc=desc, url=url, i=i, proxies=proxies,
                   thread_name=thread_name)
        response = requests.get(url=url, headers=headers, proxies=proxies, timeout=10)
    except BaseException as e:
        with lock:
            print '{t} BaseException {url} 异常：{e_info}'.format(t=time.strftime('[%Y-%m-%d %H:%M:%S]'), url=url,
                                                                  e_info=e)
            THREAD_PROXY_MAP.pop(thread_name)
            if PROXY_IP_Q.empty():
                get_proxy_ips(100)
                print '重新获取代理接口的ip数量：{}'.format(PROXY_IP_Q.qsize())
            proxies = PROXY_IP_Q.get(False)
            print '切换代理ip：{}'.format(proxies)
            THREAD_PROXY_MAP[thread_name] = proxies
            response = None
    return response


def sogou(lock, fileout):
    type_def = {'search': globals()['search_hot'], 'weixin': globals()['weixin_hot']}
    while not KEYWORD_QUEUE.empty():
        try:
            thread_name = threading.currentThread().name
            if not THREAD_PROXY_MAP.get(thread_name):
                proxies = PROXY_IP_Q.get(False)
                THREAD_PROXY_MAP[thread_name] = proxies

            line_type = KEYWORD_QUEUE.get(False)    # 类型对应来源词
            if line_type.has_key('search'):    # 执行搜索热度接口
                line = line_type.get('search')
                keyword = line.split('\t')[0]  # 关键词
                # print '关键词：', keyword
                keyword = keyword.lower()  # 英文关键词中的大写需要转换为小写，关键词中出现大写字母无法搜索到指数
                search_def = type_def.get('search')
                search_hot_content = search_def(keyword, line, thread_name, lock)    # 搜索热度指数接口
                if search_hot_content is not None:
                    data_write_file(lock, fileout, search_hot_content)  # 数据写入文件接口
            elif line_type.has_key('weixin'):    # 执行微信热度接口
                line = line_type.get('weixin')
                keyword = line.split('\t')[0]  # 关键词
                # print '关键词：', keyword
                keyword = keyword.lower()  # 英文关键词中的大写需要转换为小写，关键词中出现大写字母无法搜索到指数
                weixin_def = type_def.get('weixin')
                weixin_hot_content = weixin_def(keyword, line, thread_name, lock)    # 微信热度指数接口
                if weixin_hot_content is not None:
                    data_write_file(lock, fileout, weixin_hot_content)  # 数据写入文件接口
            else:
                print '执行相关热度接口异常。。。'

        except Empty as e:
            pass

        # except BaseException as e:
        #     print '搜狗'


def data_write_file(lock, fileout, content):
    with lock:
        fileout.write(content)
        fileout.write('\n')
        fileout.flush()


def search_hot(keyword, line, thread_name, lock):
    '''

    :param keyword: 抓取
    :param line: 存储
    :param thread_name: 线程名称
    :param lock: 锁
    :return:
    '''
    # keyword = 'Anniversary Portraits'
    search_hot_ls = []
    search_hot_url_demo = 'http://zhishu.sogou.com/index/searchHeat?kwdNamesStr={kw}&timePeriodType=MONTH&dataType=SEARCH_ALL&queryType=INPUT'
    search_hot_url = search_hot_url_demo.format(kw=keyword)
    # print '{t}搜索热度url：{url}{proxies}'.format(t=time.strftime('[%Y-%m-%d %H:%M:%S]'), url=search_hot_url,
    #                                               proxies=proxies)
    # response = requests.get(url=search_hot_url, headers=headers, proxies=proxies, timeout=10)
    response = get(search_hot_url, '搜索热度url：', 5, thread_name, lock)
    if response is not None:
        response_text = response.text
        response_obj = lxml.etree.HTML(response_text)
        wordwra = response_obj.xpath('//p[@class="wordwra"]')    # 判断搜狗是否收录该词
        if wordwra:
            print '判断搜狗没有收录该词'
            return None
        else:
            search_obj = re.search(r'root.SG.data = (.*?);\n', response_text, re.S)
            if search_obj:
                data = search_obj.group(1)
                # print 'data：', data, type(data)
                data_json = json.loads(data)
                # print 'data_json：', data_json, type(data_json)
                pv_list = data_json.get('pvList')[0]
                # print 'pv_list：', pv_list
                for pv_d in pv_list:
                    date = pv_d.get('date')    # 时间
                    # print '时间：', date
                    pv = pv_d.get('pv')    # pv值
                    # print 'pv值：', pv
                    data_dict = {'pv': pv, 'date': date}
                    search_hot_ls.append(data_dict)
                content_dict = {'data_ls': search_hot_ls, 'type': '搜索热度', 'line': line}
                content = json.dumps(content_dict, ensure_ascii=False)
                return content

            else:
                print '正则获取异常。。。'
                return None
    else:
        print '搜狗热度搜索的response is None'
        return None


def weixin_hot(keyword, line, thread_name, lock):
    # keyword = '英雄联盟'
    weixin_hot_ls = []
    weixin_hot_url_demo = 'http://zhishu.sogou.com/index/media/wechat?kwdNamesStr={kw}&timePeriodType=MONTH&dataType=MEDIA_WECHAT&queryType=INPUT'
    weixin_hot_url = weixin_hot_url_demo.format(kw=keyword)
    # print '微信热度url：', weixin_hot_url, proxies
    # print '{t}微信热度url：{url}{proxies}'.format(t=time.strftime('[%Y-%m-%d %H:%M:%S]'), url=weixin_hot_url,
    #                                               proxies=proxies)
    # response = requests.get(url=weixin_hot_url, headers=headers, proxies=proxies, timeout=10)
    response = get(weixin_hot_url, '微信热度url：', 5, thread_name, lock)
    if response is not None:
        print response.status_code
        response_text = response.text
        response_obj = lxml.etree.HTML(response_text)
        wordwra = response_obj.xpath('//p[@class="wordwra"]')  # 判断搜狗是否收录该词
        if wordwra:
            print '判断搜狗没有收录该词'
            return None
        else:
            search_obj = re.search(r'root.SG.data = (.*?);\n', response_text, re.S)
            if search_obj:
                data = search_obj.group(1)
                # print 'data：', data
                data_json = json.loads(data)
                # print 'data_json：', data_json
                pv_list = data_json.get('pvList')[0]
                # print 'pv_list：', pv_list
                for pv_d in pv_list:
                    date = pv_d.get('date')  # 时间
                    # print '时间：', date
                    pv = pv_d.get('pv')  # pv值
                    # print 'pv值：', pv
                    article_num = pv_d.get('articleNum')
                    # print 'articleNum：', article_num
                    official_accounts_num = pv_d.get('officialAccountsNum')
                    # print 'officialAccountsNum：', official_accounts_num
                    read_times = pv_d.get('readTimes')
                    # print 'readTimes：', read_times
                    data_dict = {'date': date, 'pv': pv, 'articleNum': article_num,
                                 'officialAccountsNum': official_accounts_num, 'readTimes': read_times}
                    weixin_hot_ls.append(data_dict)
                content_dict = {'data_ls': weixin_hot_ls, 'type': '微信热度', 'line': line}
                content = json.dumps(content_dict, ensure_ascii=False)
                return content

            else:
                print '正则获取异常。。。'
                return None
    else:
        print '搜狗微信指数的response is None'
        return None


def main():
    lock = Lock()
    yesterday = datetime.date.today() + datetime.timedelta(-1)
    date = yesterday.strftime('%Y%m%d')
    file_time = time.strftime('%Y%m%d')
    keyword_file_dir = r'/ftp_samba/112/file_4spider/bdzs_keyword/'  # 游戏的来源目录
    keyword_file_name = r'bdzs_keyword_{date}_1.txt'.format(date=date)  # 游戏的来源文件名
    keyword_file_path = os.path.join(keyword_file_dir, keyword_file_name)
    keyword_file_path = r'C:\Users\xj.xu\Desktop\bdzs_keyword_20190331_1.txt'
    print '获取来源文件：', keyword_file_path
    while True:
        if os.path.exists(keyword_file_path):
            break
        time.sleep(60)
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), '游戏文件路径：', keyword_file_path
    keyword_file = open(keyword_file_path, 'r')

    zhishu_type_ls = ['search', 'weixin']
    for line in keyword_file:
        line = line.strip()
        if line:
            for zhishu_type in zhishu_type_ls:
                line_type = {zhishu_type: line}    # 指数类型对应来源词
                KEYWORD_QUEUE.put(line_type)
    print '数据来源关键词的数量：', KEYWORD_QUEUE.qsize()

    get_proxy_ips(100)  # 代理ip接口
    proxy_num = PROXY_IP_Q.qsize()
    print '代理ip数量：', proxy_num

    dest_path = '/ftp_samba/112/spider/fanyule_two/sogou/'  # linux上的文件目录
    if not os.path.exists(dest_path):
        os.makedirs(dest_path)
    dest_file_name = os.path.join(dest_path, 'sogou_' + file_time)
    tmp_file_name = os.path.join(dest_path, 'sogou_' + file_time + '.tmp')
    fileout = open(tmp_file_name, 'a')

    threads = []
    for i in xrange(50):
        t = threading.Thread(target=sogou, args=(lock, fileout))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()

    try:
        fileout.flush()
        fileout.close()
    except IOError as e:
        time.sleep(1)
        fileout.close()
    os.rename(tmp_file_name, dest_file_name)


if __name__ == '__main__':
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'start'
    main()
    # search_hot(1)
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'end'