# coding:utf8
# 下载肖像权协议
import pymysql.cursors
from urllib import urlretrieve
import urllib
import upyun
import socket
import time

# 下载所有国际分发所需要的肖像权协议
def up_download(connection, start, end):
    try:
        with connection.cursor() as cursor:
         sql = """SELECT e.PI_ID, h.UR_Pic
         FROM (SELECT PI_ID FROM (
         SELECT DISTINCT a.PI_ID FROM pic_distribute a
         JOIN user_distribute b ON a.E_User=b.E_User
         JOIN pic_index c ON a.PI_ID=c.PI_ID AND c.PI_Type=1
         WHERE c.PI_State=1 
         AND a.A_State =0 
         ORDER BY a.E_Time
         ) pic_distribute) e 
         LEFT JOIN pic_index d ON d.PI_ID=e.PI_ID
         INNER JOIN pic_priminfo f ON f.PI_ID=e.PI_ID  
         INNER JOIN pic_priminfo_en g ON g.PI_ID=e.PI_ID
         INNER JOIN user_release h ON h.UR_ID=f.PPI_Release
         INNER JOIN pic_type AS i ON g.PPI_Type1=i.PT_ID
         LEFT JOIN pic_type AS j ON g.PPI_Type2=j.PT_ID ORDER BY e.PI_ID LIMIT %s, %s"""
         cursor.execute(sql, (start, end))
         results = cursor.fetchall()
         return results
    except Exception:
        print ('mysql error: 获取待分发图片列表失败')

# def filter_up(pics, up):
#     if(url.find('.jpg') != -1 or url.find('.JPG') or url.find('.jpeg')):
#         return url
#     else:
#         return url



def auto_down(url, filename):
    try:
        urlretrieve(url, filename)
    except urllib.ContentTooShortError:
        print 'Network conditions is not good.Reloading.'
        auto_down(url, filename)
    except socket.error:
        print  'socket error sleep 10s'
        time.sleep(10)
        auto_down(url, filename)

# 数据库连接配置
connection = pymysql.connect(host='localhost',user='root',password='123456',db='origino',charset='utf8',cursorclass=pymysql.cursors.DictCursor)
imagenets = ['Dreamstime',
            'Alamy',
            'Pond5',
            'Depositphotos',
            'Canstockphotos',
            'Shutterstock',
            '123RF',
            'Fotolia',
            'iStockphotos']
start = 2728
end = 100000
try:
    pics = up_download(connection, start, end)
finally:
    connection.close()
    up = upyun.UpYun('originoo-1', 'shgim', 'mao110400', timeout=3600, endpoint=upyun.ED_AUTO)
    count = 2728
    for pic in pics:
        if(pic['UR_Pic'].strip() != ''):
            if(pic['UR_Pic'].find('.jpg') or pic['UR_Pic'].find('.jpeg') or pic['UR_Pic'].find('.JPG')):
                url = pic['UR_Pic']+'!/fw/1440?_upd=true'
                path = 'f:\\model_release\\'+str(pic['PI_ID'])+'.jpg'
                auto_down(url, path)
                count += 1
                print "已下载数量: "+str(count)+"图片编号："+str(pic['UR_Pic'])
            else:
                src = pic['UR_Pic'].replace('http://originoo-1.b0.upaiyun.com//', '')
                foot = pic['UR_Pic'].split('.')[-1]
                with open('f:\\model_release\\'+pic['PI_ID']+'.'+foot, 'wb') as f:
                    up.get(src, f)
                    count += 1
                    print "已下载数量: " + str(count) + "文件编号：" + str(pic['UR_Pic'])

