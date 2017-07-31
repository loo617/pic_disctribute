# coding:utf8
import pymysql.cursors
import os
import shutil

# 筛选各个图库需分发的图片
def filter_pics(connection, storage):
    try:
        with connection.cursor() as cursor:
            sql = """SELECT e.PI_ID,f.PPI_Intro AS CN_Intro,f.PPI_Name AS CN_Name, g.PPI_Intro AS EN_Intro, g.PPI_Name AS EN_Name, h.UR_Name, i.PT_Name AS cate1
            , j.PT_Name AS cate2 
            FROM (SELECT PI_ID FROM (
            SELECT DISTINCT a.PI_ID FROM pic_distribute a
            JOIN user_distribute b ON a.E_User=b.E_User
            JOIN pic_index c ON a.PI_ID=c.PI_ID AND c.PI_Type=1
            WHERE c.PI_State=1 
            AND a.A_State =0 AND b."""+storage+"""=1
            AND a."""+storage+"""=0
            ORDER BY a.E_Time
            ) pic_distribute) e 
            LEFT JOIN pic_index d ON d.PI_ID=e.PI_ID
            INNER JOIN pic_priminfo f ON f.PI_ID=e.PI_ID  
            INNER JOIN pic_priminfo_en g ON g.PI_ID=e.PI_ID
            LEFT JOIN user_release h ON h.UR_ID=d.E_User
            INNER JOIN pic_type AS i ON g.PPI_Type1=i.PT_ID
            LEFT JOIN pic_type AS j ON g.PPI_Type2=j.PT_ID"""
            cursor.execute(sql)
            results = cursor.fetchall()
            return results
    except Exception:
        print ('mysql error')

# 拷贝指定图片到指定文件夹内
def copy_dir(pics, storage):
    for pic in pics:
        src = 'f:\\distribute\\'+str(pic['PI_ID'])+'.jpg'
        if os.path.exists(src):
            dst = 'f:\\'+storage
            if os.path.exists(dst):
                if os.path.exists(dst+'\\'+str(pic['PI_ID'])+'.jpg'):
                    print '已存在图片: ' + dst + '\\' + str(pic['PI_ID'])
                else:
                    shutil.copyfile(src, dst+'\\'+str(pic['PI_ID'])+'.jpg')
                    print '已拷贝图片: ' + dst + '\\' + str(pic['PI_ID'])
            else:
                os.mkdir(dst)
                if os.path.exists(dst+'\\'+str(pic['PI_ID'])+'.jpg'):
                    print '已存在图片: ' + dst + '\\' + str(pic['PI_ID'])
                else:
                    shutil.copyfile(src, dst+'\\'+str(pic['PI_ID'])+'.jpg')
                    print '已拷贝图片: ' + dst + '\\' + str(pic['PI_ID'])
        else:
            print str(pic['PI_ID'])+' 图片不在目录内'


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
# 第几个图片库
i = 2
try:
    pics = filter_pics(connection, imagenets[i])
finally:
    connection.close()
    copy_dir(pics, imagenets[i])
