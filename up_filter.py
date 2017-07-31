# coding:utf8
import pymysql.cursors
import os
import shutil

# 筛选各个图库需分发的肖像权协议
def filter_mrs(connection, storage):
    try:
        with connection.cursor() as cursor:
            sql = """SELECT e.PI_ID, h.UR_Pic
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
            INNER JOIN user_release h ON h.UR_ID=f.PPI_Release
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
        foot = pic['UR_Pic'].split('.')[-1]
        src = 'f:\\model_release\\'+str(pic['PI_ID'])+'.'+foot
        if os.path.exists(src):
            dst = 'f:\\mrs\\'+storage
            if os.path.exists(dst):
                shutil.copyfile(src, dst+'\\'+str(pic['PI_ID'])+'.'+foot)
                print '已拷贝图片: '+dst+'\\'+str(pic['PI_ID'])
            else:
                os.mkdir(dst)
                shutil.copyfile(src, dst+'\\'+str(pic['PI_ID'])+'.'+foot)
                print '已拷贝图片: '+dst+'\\'+str(pic['PI_ID'])
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

# for imagenet in imagenets:
#     pics = filter_mrs(connection, imagenet)
#     copy_dir(pics, imagenet)
pics = filter_mrs(connection, imagenets[6])
copy_dir(pics, imagenets[6])
connection.close()
