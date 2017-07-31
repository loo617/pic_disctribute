# coding:utf8
# 下载还没有下载的国际分发图片
import os
import pymysql.cursors
from urllib import urlretrieve
import urllib
import socket
import time

# 获取文件夹中所有文件名
def GetFileList(dir, fileList):
    newDir = dir
    if os.path.isfile(dir):
        fileList.append(dir.decode('gbk'))
    elif os.path.isdir(dir):
        for s in os.listdir(dir):
            # 如果需要忽略某些文件夹，使用以下代码
            # if s == "xxx":
            # continue
            newDir = os.path.join(dir, s)
            GetFileList(newDir, fileList)
    return fileList

# 下载所有国际分发所需要的图片
def select_wait_distribute(connection):
    try:
        with connection.cursor() as cursor:
         # Read a single record
         sql = """SELECT DISTINCT(a.PI_ID),c.PI_SrcUrl,c.PI_FieldID,a.E_Time FROM pic_distribute a
         JOIN user_distribute b ON a.E_User=b.E_User
         JOIN pic_index c ON a.PI_ID=c.PI_ID AND c.PI_Type=1
         WHERE c.PI_State=1  
         AND a.A_State =0 
         ORDER BY a.E_Time"""
         cursor.execute(sql)
         results = cursor.fetchall()
         return results
    except Exception:
        print ('mysql error: 获取待分发图片列表失败')

# 通过文件名获取所有已下载图片id
def select_download_picid(pics, rm1, rm2):
    pic_ids = []
    for pic in pics:
        pic_id = pic.replace(rm1, '').replace(rm2, '')
        pic_ids.append(int(pic_id))
    return pic_ids

# 获取没有下载的图片id
def select_not_download_picid(total_ids, download_ids):
    for id in download_ids:
        total_ids.remove(id)
    return total_ids

# 通过图片id继续下载未下载的图片:
def download_remain_pics(picids, connection):
    try:
        with connection.cursor() as cursor:
            sql = "SELECT PI_ID,PI_SrcUrl,PI_FieldID FROM pic_index WHERE PI_ID IN ("+picids+")"
            cursor.execute(sql)
            results = cursor.fetchall()
            return results
    except Exception:
        print ('mysql error:  断点下载图片失败')

# 自动下载
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
count = 0
total_pics = select_wait_distribute(connection)
# 所有图片id
total_ids = []
for pic in total_pics:
    total_ids.append(pic['PI_ID'])
download_pics = GetFileList('f:\\distribute\\', [])
rm1 = 'f:\\distribute\\'
rm2 = '.jpg'
#已下载图片id
download_ids = select_download_picid(download_pics, rm1, rm2)
#图片还剩id
not_download_ids = select_not_download_picid(total_ids, download_ids)
ids = ''
for id in not_download_ids:
    ids += str(id)
    ids += ','
ids = ids[:-1]
pics = download_remain_pics(ids, connection)
connection.close()
for pic in pics:
    if pic['PI_SrcUrl'] != '':
        url = pic['PI_SrcUrl'] + '!' + pic['PI_FieldID'] + '?_upd=true'
        path = 'f:\\distribute\\' + str(pic['PI_ID']) + '.jpg'
        auto_down(url, path)
        count += 1
        print "已下载数量: " + str(count) + "图片编号：" + str(pic['PI_SrcUrl'])

