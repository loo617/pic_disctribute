# coding:utf8
# 下载国际分发图片
import pymysql.cursors
from urllib import urlretrieve
import urllib
import socket
import time

# 下载所有国际分发所需要的图片
def select_wait_distribute(connection, start, end):
    try:
        with connection.cursor() as cursor:
         # Read a single record
         sql = """SELECT DISTINCT(a.PI_ID),c.PI_SrcUrl,c.PI_FieldID,a.E_Time FROM pic_distribute a
         JOIN user_distribute b ON a.E_User=b.E_User
         JOIN pic_index c ON a.PI_ID=c.PI_ID AND c.PI_Type=1
         WHERE c.PI_State=1  
         AND a.A_State =0 
         ORDER BY a.E_Time"""
         sql += " LIMIT %s, %s"
         cursor.execute(sql, (start, end))
         results = cursor.fetchall()
         return results
    except Exception:
        print ('mysql error: 获取待分发图片列表失败')


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
start = 0
end = 100000
try:
    pics = select_wait_distribute(connection, start, end)
finally:
    connection.close()
    count = 0
    for pic in pics:
        url = pic['PI_SrcUrl']+'!'+pic['PI_FieldID']+'?_upd=true'
        path = 'f:\\distribute\\'+str(pic['PI_ID'])+'.jpg'
        auto_down(url, path)
        count += 1
        print "已下载数量: "+str(count)+"图片编号："+str(pic['PI_SrcUrl'])
