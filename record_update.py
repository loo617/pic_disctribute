# coding:utf8
# 更新国际分发图片状态
import pymysql.cursors
import os

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

# 通过文件名获取所有已下载图片id
def select_download_picid(pics, rm1, rm2):
    pic_ids = []
    for pic in pics:
        pic_id = pic.replace(rm1, '').replace(rm2, '')
        pic_ids.append(int(pic_id))
    return pic_ids

def upt_pic_distribute(connection, imagenet, ids):
    try:
        with connection.cursor() as cursor:
         sql = "UPDATE pic_distribute SET "+imagenet+"=1 WHERE PI_ID IN ("
         sql += ids
         sql += ")"
         row = cursor.execute(sql,(ids))
         connection.commit()
         return row
    except Exception:
        print ('mysql error')


# 数据库连接配置
connection = pymysql.connect(host='localhost',user='root',password='',db='origino',charset='utf8',cursorclass=pymysql.cursors.DictCursor)
imagenets = ['Dreamstime',
            'Alamy',
            'Pond5',
            'Depositphotos',
            'Canstockphotos',
            'Shutterstock',
            '123RF',
            'Fotolia',
            'iStockphotos']
i = 0
try:
    download_pics = GetFileList('f:\\'+imagenets[i]+'\\', [])
    rm1 = 'f:\\'+imagenets[i]+'\\'
    rm2 = '.jpg'
    # 已下载图片id
    download_ids = select_download_picid(download_pics, rm1, rm2)
    rows = upt_pic_distribute(connection, imagenets[i], download_ids)
    print ('更新记录数:'+rows)
finally:
    connection.close()
