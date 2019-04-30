# _*_ coding:utf-8 _*_
import pymssql
import os
import fileinput

conn = pymssql.connect("202.121.178.170", "gqf", "Sjtu2019", "test")
cursor = conn.cursor()

header = ['province', 'city', 'city_code', 'station', 'station_code', 'level', 'pollutions', 'aqi', 'so2', 'so2_24h',
          'no2', 'no2_24h', 'co', 'co_24h', 'o3', 'o3_24h',	'o3_8h', 'o3_8h_24h', 'pm10', 'pm10_24h', 'pm2_5', 'pm2_5_24h',
          'longitude', 'latitude', 'pubtime']

sqlVal = ''
sqlElements = ''
sqlUpdate = ''

for i in range(len(header)):
    sqlVal += ',' + header[i]
    sqlElements += ',%s'
    sqlUpdate += ',' + header[i] + '=values(' + header[i] + ')'

sqlVal = sqlVal.replace(",", "", 1)
sqlElements = sqlElements.replace(",", "", 1)
sqlUpdate = sqlUpdate.replace(",", "", 1)

for root, dirs, files in os.walk(os.path.join('./')):
    for name in files:
        fileName = os.path.join(root, name)
        if name.endswith('csv'):
            print(fileName)
            sql_value = []
            c = 0
            for line in fileinput.input(fileName, openhook=fileinput.hook_encoded("utf-8", "surrogateescape")):
                if c == 0:
                    c = c + 1
                    continue

                contentList = line.split(',')
                tp = []
                for i in range(len(header)):
                    tp.append(contentList[i])

                sql_value.append(tuple(tp))

            cursor.execute("""
            IF OBJECT_ID('test', 'U') IS NOT NULL
                DROP TABLE test
            CREATE TABLE test (
                province VARCHAR(100),
                city VARCHAR(100),
                city_code VARCHAR(100),
                station VARCHAR(100),
                station_code VARCHAR(100),
                level VARCHAR(100),
                pollutions VARCHAR(100),
                aqi VARCHAR(100),
                so2 VARCHAR(100),
                so2_24h VARCHAR(100),
                no2 VARCHAR(100),
                no2_24h VARCHAR(100),
                co VARCHAR(100),
                co_24h VARCHAR(100),
                o3 VARCHAR(100),
                o3_24h VARCHAR(100),
                o3_8h VARCHAR(100),
                o3_8h_24h VARCHAR(100),
                pm10 VARCHAR(100),
                pm10_24h VARCHAR(100),
                pm2_5 VARCHAR(100),
                pm2_5_24h VARCHAR(100),
                longitude VARCHAR(100),
                latitude VARCHAR(100),
                pubtime VARCHAR(100)
            )
            """)
            cursor.executemany(
                "INSERT INTO test VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                tuple(sql_value))
            conn.commit()
