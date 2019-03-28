from mainspider import *
import threading

"明天在其他函数里面加一个district参数，并且完成返回信息的修改"
#get_subdistrictinfo('xuhui',1,10)
#get_subdistrictdetails()
#get_xiaoquhousesinfo()
#get_xiaoquhousesdetails()
#download_bigimages('xiaoqu')

#get_subdistrictinfo('pudong',4,4)
#get_subdistrictinfo('changning',1,10)
def mkinfothread(threadtarget):#threadtarget为mainspider中的函数
    threads=[]
    files=range(len(CODE))
    for i in CODE:
        t=threading.Thread(target=threadtarget,args=(i,))
        t.setName(f'{i}thread')
        threads.append(t)
    for i in files:
        threads[i].start()
    for i in files:
        threads[i].join()
    print('end')
def mkpicthread():#爬取图片线程
    files=range(len(CODE))
    xiaoqupicthreads=[]
    fangzipicthreads=[]
    for i in CODE:
        txiaoqu=threading.Thread(target=download_bigimages,args=('xiaoqu',i))
        txiaoqu.setName(f'{i}xiaoqu')
        tfangzi=threading.Thread(target=download_bigimages,args=('fangzi',i))
        tfangzi.setName(f'{i}fangzi')
        xiaoqupicthreads.append(txiaoqu)
        fangzipicthreads.append(tfangzi)
    for i in files:
        # xiaoqupicthreads[i].start()
        fangzipicthreads[i].start()
    for i in files:
        # xiaoqupicthreads[i].join()
        fangzipicthreads[i].join()
    print('end')

if __name__ == "__main__":
   # mkinfothread(get_xiaoquhousesinfo)
    #mkinfothread(get_xiaoquhousesdetails)
    #mkpicthread()
    pass