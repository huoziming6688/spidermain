"爬虫主文件"
import re
import requests
from bs4 import BeautifulSoup
from headers import get_heaters
import pymysql
import sys,os
from log import MyLog
import time,random
from download_pic import *
import traceback

RANDOM_DELAY='False'
STATICFILEDIR='D:\spider'
HOST='localhost'
USER='root'
PWD=''
DB='spiderdb'
mylog=MyLog()
#17个区
CODE={'minhang':'12','baoshan':'13','xuhui':'04','putuo':'07','yangpu':'10','changning':'05','songjiang':'17','jiading':'14','huangpu':'01','jingan':'06','hongkou':'09','qingpu':'18','fengxian':'20','jinshan':'16','chongming':'51','pudong':'15','zhabei':'08'}
def random_delay():
    if RANDOM_DELAY:
        time.sleep(random.randint(0, 2))

def get_subdistrictinfo(district,page1,page2):
    "查询小区基本信息，填充subdistrict表并下载小区首页图片"
    #小区编码


    districtid='3101'+CODE[district]
    db = pymysql.connect(HOST,USER,PWD,DB)
    cursor=db.cursor()
    cursor.execute('select distinct districtid,page from subdistrict')
    pagehaven = cursor.fetchall()
    for page in range(page1,page2+1):
        if ((districtid,page)not in pagehaven):
            sid='3101'+CODE[district]+str((page-1)*30+1).zfill(4)      
            url='https://sh.lianjia.com/xiaoqu/'+f'{district}/pg{page}'       
            headers = get_heaters()
            payload = {'from': 'rec'}
            try:
                r=requests.get(url,timeout=10,headers=headers,params=payload).content
                #等待
                random_delay()
            except:
                mylog.error(f'{traceback.format_exc()}')
                sys.exit(0)
            soup=BeautifulSoup(r)
            url_ul=soup.find('ul',attrs={'class':'listContent'})
            url_all=url_ul.find_all('a',attrs={'class':'img'})
            infolist=[]
            for i in url_all:
                
                
               
                url=(i['href'])
                title=i.img['alt']
                imageurl=i.img['data-original']
                pattern=re.compile(r'\d+')
                yuanid=pattern.findall(url)[0]

                info={'yuanid':yuanid,'sname':title,'url':url,'imageurl':imageurl}
                infolist.append(info)
                
                # sql=f"insert into subdistrict(sid,sname,url,imageurl) values ('{id}','{title}','{url}','{imageurl}')"
                # sqllist.append(sql)
            
            for info in infolist:
                #生成小区图片文件夹
                imagedir=f'{STATICFILEDIR}/image/L{sid}'
                mkdir(imagedir)
                #下载小区首页图
                imagepath=imagedir+'/shouyeimage.jpg'
                if info['imageurl']!='':
                    if not os.path.isfile(imagepath):
                        try:
                            download_images(info['imageurl'],imagepath)
                            
                        except:
                            mylog.error(f'{traceback.format_exc()}')
                         
                    # except:
                    #     mylog.error(f'{sid}首页图片下载失败')
                sql=f"insert into subdistrict(sid,yuanid,sname,url,imageurl,districtid,page,imagedir) values ('L{sid}','{info['yuanid']}','{info['sname']}','{info['url']}','{info['imageurl']}','{districtid}','{page}','{imagedir}')"

                try:  
                    cursor.execute(sql)
                    
                    sid=str(int(sid)+1)
                    
                except:
                    db.rollback()
                    mylog.error(f'{traceback.format_exc()}')
            #这个地方有可能出错
            db.commit()
            mylog.debug(f'第{page}页已保存')
        else:
            mylog.debug(f'第{page}页已存在')
    
    mylog.info(f'{district}区第{page1}到第{page2}（包含）已经保存完毕')
    db.close()

def get_subdistrictdetails(district):
    "获取某个区subdistrict表中已有小区的详情，填充subdistrict_detail，subdistrict_location和xqimageurl表"
    results=[]
    #num=1
    did='3101'+CODE[district]
    db=pymysql.connect(HOST,USER,PWD,DB)
    cursor = db.cursor()
    sql=f"select x.sid,url from subdistrict as x where x.sid not in (select y.sid from subdistrict_detail as y) and x.districtid={did} order by x.sid"
    try:
        # 执行SQL语句
        cursor.execute(sql)
        results = cursor.fetchall()
    except:
        mylog.error("Error: unable to fetch data")
    if len(results)!=0:
        mylog.debug(f'{results[-1][0]}')
        for row in results:
            
            details=dict()
            details['sid']=row[0]
            xqurl=row[1]
            headers = get_heaters()
            payload = {'from': 'rec'}
            try:
                r=requests.get(xqurl,timeout=10,headers=headers,params=payload).content
                #等待
                random_delay()
            except:
                mylog.error(f"{details['sid']}未完成")
                sys.exit(0)
            soup=BeautifulSoup(r)
            #获取小区地址
            if(soup.find('div',attrs={'class':'detailDesc'})):
                details['address']=soup.find('div',attrs={'class':'detailDesc'}).text
            else:details['address']=''
            #获取小区大图的链接
            imagelist=[]
            try:
                for imageurl in (soup.find('ol',attrs={'id':'overviewThumbnail'}).find_all('li')):           
                    imagelist.append(imageurl['data-src'])   
            except:pass      
            #获取小区均价
            if(soup.find('span',attrs={'class':'xiaoquUnitPrice'})):
                details['unitprice']=soup.find('span',attrs={'class':'xiaoquUnitPrice'}).text
            else:details['unitprice']=0
            #建筑时间，建筑类型，管理费，管理公司，开发商，总楼栋，总房屋获取
            list1=[]
            try:
                for i in (soup.find_all('span',attrs={'class':'xiaoquInfoContent'})):
                    soup2=BeautifulSoup(str(i))
                    list1.append(soup2.span.text)
                details['bulit_time']=list1[0]
                details['bulit_type']=list1[1]
                details['manage_fee']=list1[2]
                details['manage_company']=list1[3]
                details['delevoper']=list1[4]
                details['total_buildings']=list1[5]
                details['total_houses']=list1[6]
            except:
                details['bulit_time']=''
                details['bulit_type']=''
                details['manage_fee']=''
                details['manage_company']=''
                details['delevoper']=''
                details['total_buildings']=''
                details['total_houses']=''
            #获取小区经纬度
            try:
                location=soup.find(text=re.compile("resblockPosition"))
                pattern=re.compile(r'resblockPosition:\'(.+?)\'')
                details['jingdu']=pattern.findall(location)[0].split(',')[0]
                details['weidu']=pattern.findall(location)[0].split(',')[1]
            except:
                details['jingdu']=''
                details['weidu']=''
            sql1=f"insert into subdistrict_detail(sid,avg_price,built_time,built_type,manage_fee,manage_company,developer,total_buildings,total_houses,subaddress) values ('{details['sid']}','{float(details['unitprice'])}','{details['bulit_time']}','{details['bulit_type']}','{details['manage_fee']}','{details['manage_company']}','{details['delevoper']}','{details['total_buildings']}','{details['total_houses']}','{details['address']}')"
            sql2=f"insert into `subdistrict location`(sid,jingdu,weidu) values ('{details['sid']}','{details['jingdu']}','{details['weidu']}')"
            try:
                cursor.execute(sql1)
                cursor.execute(sql2)
                db.commit()
            except:
                db.rollback()
                mylog.error(f'sql1,sql2执行失败')
                mylog.error(f'{traceback.format_exc()}')
            imageorder=1
            for xiaoquimageurl in imagelist:
                sql3=f"insert into xqimageurl(sid,bigimageurl,imageorder) values('{details['sid']}','{xiaoquimageurl}','{imageorder}')"
                try:
                    cursor.execute(sql3)
                    imageorder+=1
                    db.commit()
                except Exception as e:
                    db.rollback()
                    mylog.error(f'{repr(e)}')
            mylog.debug(f"{details['sid']}已经保存完毕")
        mylog.info(f'{results[0][0]}到{results[-1][0]}(包含)保存完毕')
    else:
        mylog.debug(f'{district}区subdistrict表中的所有小区详情已存在')
    db.close()

def get_xiaoquhousespage(zufangurl:str):
    headers=get_heaters()
    payload = {'from': 'rec'}
    try:
        r=requests.get(zufangurl,timeout=10,params=payload,headers=headers).text
    except:
        mylog.error(f'{zufangurl}获取失败')
        mylog.error(f'{traceback.format_exc()}')
    if re.search(r'data-totalPage=(\d+)',r):
        totalpage=re.search(r'data-totalPage=(\d+)',r).group(1)
        
    else:
        totalpage=0
    return totalpage

def get_xiaoquhousesinfo(district):
    "获取某区subdistrict表中已有小区的房源，填充house表,下载house首页图"
    results=[]
    did='3101'+CODE[district]
    db=pymysql.connect(HOST,USER,PWD,DB)
    cursor = db.cursor()
    sql="select x.sid,x.yuanid from subdistrict as x where x.sid not in (select subdistrictid from house) and (x.issearch = 0 or x.issearch is NULL) x.districtid={did} order by x.sid"
    try:
        # 执行SQL语句
        cursor.execute(sql)
        results = cursor.fetchall()
    except:
        mylog.error(f'{traceback.format_exc()}')
    if len(results)!=0:    
        for row in results:
            order=str(1).zfill(4)
            subdistrictid=row[0]
            xqyuanid=row[1]
            zufangurl1=f'https://sh.lianjia.com/zufang/c{xqyuanid}/'
            #查找出小区房源的页码数
            total_page=get_xiaoquhousespage(zufangurl1)
            if total_page:
                for page in range(1,int(total_page)+1):
                    
                    
                    zufangurl2=f'http://sh.lianjia.com/zufang/pg{page}c{xqyuanid}'
                    headers = get_heaters()
                    payload = {'from': 'rec'}
                    try:
                        r=requests.get(zufangurl2,timeout=10,headers=headers,params=payload).content
                        #等待
                        random_delay()
                    except:
                        mylog.error(f"{subdistrictid}房源第{page}页未完成")
                        mylog.error(f'{traceback.format_exc()}')
                        sys.exit(0)
                    soup=BeautifulSoup(r)
                    for i in soup.find_all('a',attrs={'class':'content__list--item--aside'}):
                        houseinfo=dict()
                        houseinfo['subdistrictid']=subdistrictid
                        houseinfo['hid']=f'{subdistrictid}{order}'
                        houseinfo['hyuanid']=re.search(r'href=\"/zufang/(.*?)\.',str(i)).group(1)
                        houseinfo['hurl']="http://sh.lianjia.com"+re.search(r'href=\"(.*?)\"',str(i)).group(1)
                        houseinfo['htitle']=re.search(r'<img alt="(.*?)\"',str(i)).group(1)
                        houseinfo['himageurl']=re.search(r' data-src="(.*?)\"',str(i)).group(1)
                        houseimagedir=f"{STATICFILEDIR}/image/{subdistrictid}/{houseinfo['hid']}"
                        mkdir(houseimagedir)
                        download_images(houseinfo['himageurl'],f'{houseimagedir}/househouyeimage.jpg')
                        houseinfo['himagedir']=houseimagedir
                        sql1=f"insert into house(hid,subdistrictid,hyuanid,hurl,htitle,himageurl,himagedir) values('{houseinfo['hid']}','{houseinfo['subdistrictid']}','{houseinfo['hyuanid']}','{houseinfo['hurl']}','{houseinfo['htitle']}','{houseinfo['himageurl']}','{houseinfo['himagedir']}')"
                        try:
                            cursor.execute(sql1)
                            
                        except Exception as e:
                            mylog.error(f'{repr(e)}')
                        else:
                            mylog.debug(f"{houseinfo['hid']}保存完毕")
                            order=str(int(order)+1).zfill(4)
                    
                    mylog.debug(f'{subdistrictid}小区第{page}页保存完毕')
                



                db.commit()
                mylog.debug(f'{subdistrictid}小区保存完毕')
            else:
                mylog.debug(f'{subdistrictid}小区无房源')
            try:
                sqlup_issearch=f"update subdistrict set issearch = 1 where sid ='{subdistrictid}'"
                cursor.execute(sqlup_issearch)
                db.commit()
            except:
                mylog.error(str(traceback.format_exc()))
        mylog.info(f'{results[0][0]}到{results[-1][0]}保存完毕')
    else:
        mylog.debug(f'{district}表中subdistrict表中的所有小区已经爬取完毕')
    db.close()
            
                
def get_xiaoquhousesdetails(district):
    hidhead='L3101'+CODE[district]+'%'
    "获取house表中的房源的详情，填充house_detail，house_facility和himageurl表"
    hresults=[]
    db=pymysql.connect(HOST,USER,PWD,DB)
    cursor = db.cursor()
    sql=f"select x.hid,hurl from house as x where x.hid not in (select y.hid from house_detail as y) and x.hid like {hidhead} order by x.hid"
    try:
        # 执行SQL语句
        cursor.execute(sql)
        hresults = cursor.fetchall()
    except Exception as e:
        mylog.error(str(repr(e)))
    if len(hresults)!=0:
        mylog.debug(f'{hresults[-1][0]}')
        for row in hresults:
            himageurllist=[]
            housedetails={'hid':row[0]}
            facilities={'hid':row[0]}
            hurl=row[1]
            headers = get_heaters()
            payload = {'from': 'rec'}
            try:
                r=requests.get(hurl,timeout=10,headers=headers,params=payload).text
                #等待
                random_delay()
            except:
                mylog.error(f"{housedetails['hid']}未完成")
                sys.exit(0)
            if re.search(r'房源上架时间 (\d+\-\d+\-\d+)',r):
                soup=BeautifulSoup(r)
                housedetails['hpubdate']=re.search(r'房源上架时间 (\d+\-\d+\-\d+)',r).group(1)
                housedetails['hprice']=re.search(r'<p class="content__aside--title"><span>(\d+)',r).group(1)
                housedetails['hdirection']=re.search(r'<span><i class="orient"></i>(.*?)<',r).group(1)
                housedetails['htype']=re.search(r'<span><i class="typ"></i>(.*?)<',r).group(1)
                housedetails['hdirection']=re.search(r'<span><i class="orient"></i>(.*?)<',r).group(1)
                housedetails['htype']=re.search(r'<span><i class="typ"></i>(.*?)<',r).group(1)
                housedetails['harea']=re.search(r'<span><i class="area"></i>(\d+)',r).group(1)
                housedetails['hcontact_info']=re.search(r'<div class="phone">(.*?)<',r).group(1)
                housedetails['hfloor']=re.search(r'<li class="fl oneline">楼层：(.*?)<',r).group(1)
                if soup.find('p',attrs={'data-el':'houseComment'})!= None:
                    housedetails['description']=soup.find('p',attrs={'data-el':'houseComment'})['data-desc'].replace('\n','')
                else:housedetails['description']=''
                for i in soup.find_all('div',attrs={'class':"content__article__slide__item"}):
                    himageurl=re.search(r'data-src=\"(.*?)"',str(i)).group(1)
                    himageurllist.append(himageurl)
                facilities['parkinglot']=[1,'NULL'][re.search(r'<li class="fl oneline">车位：(.*?)<',r).group(1)=='暂无数据']
                facilities['lift']=[0,1][re.search(r'<li class="fl oneline">电梯：(.*?)<',r).group(1)=='有']
                facilities['gas']=[0,1][re.search(r'<li class="fl oneline">燃气：(.*?)<',r).group(1)=='有']
                facilities['TV']=[0,1][re.search(r'<li class="fl oneline television(.*?) "',r).group(1)=='']
                facilities['freezer']=[0,1][re.search(r'<li class="fl oneline refrigerator(.*?) "',r).group(1)=='']    
                facilities['washer']=[0,1][re.search(r'<li class="fl oneline washing_machine(.*?) "',r).group(1)=='']  
                facilities['airconditioner']=[0,1][re.search(r'<li class="fl oneline air_conditioner(.*?) "',r).group(1)=='']
                facilities['water_heater']=[0,1][re.search(r'<li class="fl oneline water_heater(.*?) "',r).group(1)==''] 
                facilities['heating']=[0,1][re.search(r'<li class="fl oneline heating(.*?) "',r).group(1)=='']   
                facilities['wifi']=[0,1][re.search(r'<li class="fl oneline wifi(.*?) "',r).group(1)=='']
                sql1=f"insert into house_detail(hid,hprice,hdirection,htype,harea,hfloor,hpubdate,hcontact_info,description) "\
                    f"values('{housedetails['hid']}','{housedetails['hprice']}','{housedetails['hdirection']}','{housedetails['htype']}',"\
                    f"'{housedetails['harea']}','{housedetails['hfloor']}','{housedetails['hpubdate']}','{housedetails['hcontact_info']}','{housedetails['description']}')"
                try:
                    cursor.execute(sql1)
                except Exception as e:
                    mylog.error(str(repr(e)))
                sql2=f"insert into house_facility(hid,washer,TV,airconditioner,freezer,heating,wifi,lift,gas,water_heater,parkinglot)"\
                    f"values('{facilities['hid']}','{facilities['washer']}','{facilities['TV']}','{facilities['airconditioner']}',"\
                    f"'{facilities['freezer']}','{facilities['heating']}',"\
                    f"'{facilities['wifi']}','{facilities['lift']}','{facilities['gas']}','{facilities['water_heater']}',{facilities['parkinglot']})"
                try:
                    cursor.execute(sql2)
                except Exception as e:
                    mylog.error(str(repr(e)))   
                #print(sql1,sql2)
                himageorder=1         
                for himageurl in himageurllist:
                    sql3=f"insert into himageurl(hid,hbigimageurl,himageorder) "\
                        f"values('{housedetails['hid']}','{himageurl}','{himageorder}')"
                    # print(sql3)
                    try:
                        cursor.execute(sql3)
                        himageorder+=1
                    except Exception as e:
                        mylog.error(str(repr(e)))
                db.commit()
                mylog.debug(f"{housedetails['hid']}详细信息已保存")
            else:
                mylog.error(f"{housedetails['hid']}已失效")
        mylog.info(f'{hresults[0][0]}到{hresults[-1][0]}(包含)保存完毕')
    else:
        mylog.debug(f'{district}区house表中的所有房子详情已存在')
    db.close()


def download_bigimages(flag1):
    db = pymysql.connect(HOST,USER,PWD,DB)
    cursor=db.cursor()
    if flag1=='xiaoqu':
        sqlget_bigimages=f"select sid,bigimageurl,imageorder from xqimageurl where (isdownload = 0 or isdownload is NULL) order by sid"
        try:
            cursor.execute(sqlget_bigimages)
            resultbigimages=cursor.fetchall()
        except:
            print(1)
        for row in resultbigimages:
            sid=row[0]
            bigimageurl=row[1]
            bigimageorder=row[2]
            save_path=f"{STATICFILEDIR}/image/{sid}/{bigimageorder}.jpg"
            isdownload=download_images(bigimageurl,save_path)
            sqlup_isdownload=f"update xqimageurl set isdownload = {isdownload} where sid ='{sid}' and imageorder='{bigimageorder}'"
            cursor.execute(sqlup_isdownload)
            db.commit()

        
    if flag1=='fangzi':
        sqlget_bigimages=f"select hid,hbigimageurl,himageorder from himageurl where (isdownload = 0 or isdownload is NULL) order by hid"
        cursor.execute(sqlget_bigimages)
        resultbigimages=cursor.fetchall()
        for row in resultbigimages:
            hid=row[0]
            hbigimageurl=row[1]
            hbigimageorder=row[2]
            save_path=f"{STATICFILEDIR}/image/{hid[0:-4]}/{hid}/{hbigimageorder}.jpg"
            isdownload=download_images(hbigimageurl,save_path)
            sqlup_isdownload=f"update himageurl set isdownload = {isdownload} where hid ='{hid}' and himageorder='{hbigimageorder}'"
            cursor.execute(sqlup_isdownload)
            db.commit()
    db.close()







if __name__ == "__main__":
    for i in CODE:
        #get_subdistrictinfo(i,1,10)
        get_subdistrictdetails(i)
    #get_subdistrictinfo('pudong',4,10)
    #get_subdistrictdetails()
   # download_bigimages('xiaoqu')
    # get_subdistrictinfo('pudong',1,5)
    # get_xiaoquhousesinfo()