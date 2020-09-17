import csv
import mysql.connector
import contextlib

#接続する
connect = mysql.connector.connect(
    user='root',
    password='leon0704',
    host='localhost',
    database='Master'
)

# コネクションが切れた時に再接続してくれるよう設定
connect.ping(reconnect=True)

# 接続できているかどうか確認
print(connect.is_connected())

#カーソルを作成
cursor = connect.cursor()

#中身を書き込む
cursor.execute("select * from Master into outfile 'D:Inturn/PreMaster.csv' fields terminated by ',' lines terminated by '\r\n';")
#result = cursor.fetchall()
#print(result, file = f)


#もしテーブルが存在しない場合は作成
cursor.execute("CREATE TABLE IF NOT EXISTS Master(base_num int(10), base_name char(10), parameter1 int(10), parameter2 int(10), parameter3 int(10))")
#テーブルの初期化
cursor.execute("TRUNCATE TABLE Master")

with open("MasterDB.csv", "r") as f:
    reader = csv.reader(f)
    for row in reader:
#最初の行は取り除く
#データの挿入
        cursor.execute("INSERT INTO Master (base_num, base_name, parameter1, parameter2, parameter3) VALUES (%d,%s,%d,%d,%d)", row)


#変更を反映させる
connect.commit()

cursor.close()
connect.close()