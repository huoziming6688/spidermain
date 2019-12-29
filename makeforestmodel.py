import pymysql
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.model_selection import GridSearchCV
from sklearn.ensemble import RandomForestRegressor 
# from sklearn.externals import joblib
import joblib
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
data=data.reindex(np.random.permutation(data.index))
data['floor'],data['maxfloor']=data['hfloor'].str.split('/').str
data['maxfloor']=data['maxfloor'].str.replace('层','')
data=data.join(pd.get_dummies(data['floor']))
data=data.join(data['hdirection'].str.split(pat=' ',expand=True))
data.rename(columns={0:'direction'}, inplace = True)
target=data['hprice']
data=data.join(pd.get_dummies(data['direction']))
data.drop(['hprice','hid','htype','hfloor','floor','hdirection','direction',1,2,3,],axis=1,inplace=True)
# X_train,X_test,y_train,y_test=train_test_split(data,target,random_state=1)
X_train=data[0:7500]
y_train=target[0:7500]
X_test=data[7500:]
y_test=target[7500:]

param_grid = {"n_estimators":[5,10,50,100,200,500],"max_depth":[5,10,50,100,200,500]}
grid_search=GridSearchCV(RandomForestRegressor(),param_grid,cv=5)
grid_search.fit(X_train,y_train)
joblib.dump(grid_search,'model_1.pkl')
print(grid_search.best_params_)
print(grid_search.best_score_)
