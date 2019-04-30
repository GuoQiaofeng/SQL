# _*_ coding:utf-8 _*_

import datetime
import MySQLdb
import os
import fileinput


conn = MySQLdb.connect("数据库地址", "用户名", "密码", "库", charset='utf8' )
cursor = conn.cursor()

header=['Station_Id_C','Year','Mon','Day','Hour','PRS','PRS_Sea','PRS_Max','PRS_Min','WIN_S_Max','WIN_S_Inst_Max','WIN_D_INST_Max','WIN_D_Avg_2mi','WIN_S_Avg_2mi','WIN_D_Avg_10mi','WIN_S_Avg_10mi','WIN_D_S_Max','TEM','TEM_Max','TEM_Min','RHU','VAP','RHU_Min','PRE_1h','CLO_Cov','CLO_Cov_Low']

sqlVal=''
sqlElements=''
sqlUpdate=''


for i in range(len(header)):
	sqlVal+=','+header[i]
	sqlElements+=',%s'
	sqlUpdate+=','+header[i]+'=values('+header[i]+')'
	
sqlVal=sqlVal.replace(",", "", 1)
sqlElements=sqlElements.replace(",", "", 1)
sqlUpdate=sqlUpdate.replace(",", "", 1)


for root, dirs, files in os.walk(os.path.join('./')):
	for name in files:
		fileName = os.path.join(root, name)	 
		if (name.endswith('csv')):
			print(fileName)
			sql_value=[]
			
			c=0
			for line in fileinput.input(fileName):
				if (c==0):
					c=c+1
					continue

				contentList = line.split(',')
				
				sqlData=''
				tp=[]
				for i in range(len(header)):
					tp.append(contentList[i])
							
				sql_value.append(tuple(tp));
				
			sql='insert into 数据表 ('+sqlVal+') values('+sqlElements+') ON DUPLICATE KEY UPDATE '+sqlUpdate
			cursor.executemany(sql, tuple(sql_value))
			conn.commit()