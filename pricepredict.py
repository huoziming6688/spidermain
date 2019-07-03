import pymysql
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.model_selection import GridSearchCV
from sklearn.ensemble import RandomForestRegressor 
from sklearn.externals import joblib
import matplotlib.pyplot as plt
HOST='localhost'
USER='root'
PWD=''
DB='spiderdb'
db = pymysql.connect(HOST,USER,PWD,DB)
sql=f'select x.hid,x.hprice,x.hdirection,htype,harea,hfloor,washer,TV,airconditioner,freezer,heating,wifi,lift,gas,water_heater,jingdu,weidu from house_detail as x,house as y,house_facility as z,subdistrict as m,`subdistrict location` as n where x.hid=y.hid and y.hid=z.hid and y.subdistrictid=m.sid and m.sid=n.sid'
data = pd.read_sql_query(sql,db)
data=data.apply(lambda x:x.replace('',np.nan).replace('朝向暂无',np.nan))
data=data.dropna()
# data=data.reindex(np.random.permutation(data.index))
data['floor'],data['maxfloor']=data['hfloor'].str.split('/').str
data['maxfloor']=data['maxfloor'].str.replace('层','')
data=data.join(pd.get_dummies(data['floor']))
data=data.join(data['hdirection'].str.split(pat=' ',expand=True))
data.rename(columns={0:'direction'}, inplace = True)
target=data['hprice']
data=data.join(pd.get_dummies(data['direction']))
hids=data['hid']
data.drop(['hprice','hid','htype','hfloor','floor','hdirection','direction',1,2,3,],axis=1,inplace=True)
X_test=data
y_test=target

# plt.rcParams["font.sans-serif"]=["SimHei"]
# plt.rcParams["axes.unicode_minus"]=False
grid_search=joblib.load('d:\\model.pkl')
# features = X_train.columns
# importance = grid_search.best_estimator_.feature_importances_
# fi = pd.Series(importance,index = features)
# fi = fi.sort_values(ascending = False)
# ten = fi[:10]
# fig = plt.figure(figsize = (16,9)) 
# ax = fig.add_subplot(1,1,1,facecolor = "whitesmoke",alpha = 0.2)
# ax.grid(color = "grey",linestyle=":",alpha = 0.8,axis = "y")
# ax.barh(ten.index,ten.values,color = "dodgerblue")
# ax.set_xticklabels([0.0,0.1,0.2,0.3,0.4,0.5,0.6],fontsize = 22)
# ax.set_yticklabels(ten.index,fontsize = 22)
# ax.set_xlabel("importance",fontsize = 22)
# plt.show()
results=grid_search.predict(X_test)
resultlist=list()
hidlist=list()
for hid in hids:
    
    hidlist.append(hid)
for result in results:
   
    resultlist.append(result)
    cursor=db.cursor()
for i in range(0,len(hidlist)):
    sql=f"update house_detail set hpredictprice = {int(round(resultlist[i]))} where hid='{hidlist[i]}'"
    cursor.execute(sql)
    db.commit()
db.close()