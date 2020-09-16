import csv
import mysql.connector
import contextlib

#接続する
connect = mysql.connector.connect(
    user='root',
    password='leon0704',
    host='localhost',
    database='kishoudata'
)

# コネクションが切れた時に再接続してくれるよう設定
connect.ping(reconnect=True)

# 接続できているかどうか確認
print(connect.is_connected())

#カーソルを作成
cursor = connect.cursor()

#もしテーブルが存在しない場合は作成
cursor.execute("CREATE TABLE IF NOT EXISTS data(data char(20), kion char(10), number1 char(5), number2 char(5))")
#テーブルの初期化
cursor.execute("TRUNCATE TABLE data")

with open("data.csv", "r") as file:
    reader = csv.reader(file)
    for row in reader:
#最初の行は取り除く
#データの挿入
        cursor.execute('INSERT INTO data (data, kion, number1, number2) VALUES (%s,%s,%s,%s)', row)


#変更を反映させる
connect.commit()

#中身を確認する
# cursor.execute('select * from scale')
# result = cursor.fetchall()
# print(result)

cursor.close()
connect.close()