"生成目录和下载图片"
from urllib.request import urlretrieve
import time
import sys,os
from log import *
mylog=MyLog()
def mkdir(dirpath):
    isExists=os.path.exists(dirpath)
    if not isExists:
        os.makedirs(dirpath) 
        mylog.debug(f'{dirpath}目录成功创建')
       
    else:
        mylog.debug(f'{dirpath}目录已存在')
        
def download_images(image_url: str,save_path: str):
    if not os.path.isfile(save_path):
        try:
            urlretrieve(image_url,save_path)
        except Exception as e:
            mylog.error(f'下载图片{repr(e)}')
            return 0
        else:
            mylog.debug(f'{save_path}已经下载成功')
    else:
        mylog.debug(f'{save_path}已存在')
    return 1

if __name__ == "__main__":
    save_path='D:\\spiter/image/3101150211/shouyeimage.jpg'
    image_url='https://image1.ljcdn.com/hdic-resblock/aeb5df5e-f3a9-4b6b-80a7-e2d2ff43f366.jpg.232x174.jpg'
    download_images(image_url,save_path)

