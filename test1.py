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
threads=[]
files=range(len(CODE))
for i in CODE:
   
    t=threading.Thread(target=get_subdistrictdetails,args=(i,))
    t.setName(f'{i}thread')
    threads.append(t)
if __name__ == "__main__":
    for i in files:
        threads[i].start()
    for i in files:
        threads[i].join()
    print('end')