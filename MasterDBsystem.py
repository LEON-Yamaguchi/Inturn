import csv
import mysql.connector
import contextlib
import os
import pandas as pd
import json

slave_num = 3
d_slave = {}
d_slave_num = {}
d_slave_1 = {}
d_slave_2 = {}
d_slave_3 = {}
master_df = {}

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
#print(connect.is_connected())

#ファイルの削除
if os.path.exists("D:Inturn/PreMaster.csv") == True:
    os.remove("D:Inturn/PreMaster.csv")
if os.path.exists("D:Inturn/database.csv") == True:
    os.remove("D:Inturn/database.csv")

#カーソルを作成
cursor = connect.cursor()

#中身を書き込む
cursor.execute("select * from Master into outfile 'D:Inturn/PreMaster.csv' fields terminated by ',' lines terminated by '\r\n';")
#result = cursor.fetchall()
#print(result, file = f)


#もしテーブルが存在しない場合は作成
#cursor.execute("CREATE TABLE IF NOT EXISTS Master(base_num char(10), base_name char(10), parameter1 char(10), parameter2 char(10), parameter3 char(10))")
#テーブルの初期化
cursor.execute("TRUNCATE TABLE Master")

with open("MasterDB.csv", "r") as f:
    reader = csv.reader(f)
    for row in reader:
#最初の行は取り除く
#データの挿入
        cursor.execute("INSERT INTO Master (base_num, base_name, parameter1, parameter2, parameter3) VALUES (%s,%s,%s,%s,%s)", row)

#変更を反映させる
connect.commit()

#CSVファイルの差分検出
def execute():
    df1 = pd.read_csv('D:Inturn/PreMaster.csv')
    df2 = pd.read_csv('MasterDB.csv')
    diff_df = dataframe_difference(df1, df2)
    diff_df.to_csv('MasterUpgrade.csv')
    #print(diff_df['base_name'])
    global d_slave, d_slave_num, d_slave_1, d_slave_2, d_slave_3
    global master_df
    master_df = diff_df
    print(master_df)
    d_slave = dict(diff_df['base_name'])
    d_slave_num = dict(diff_df['base_num'])
    d_slave_1 = dict(diff_df['parameter1'])
    d_slave_2 = dict(diff_df['parameter2'])
    d_slave_3 = dict(diff_df['parameter3'])
    #for num in range(slave_num):
        #if ((num) in d_slave.keys()) == True:
            #print(d_slave[num])


def dataframe_difference(df1, df2, which=None):
    """Find rows which are different between two DataFrames."""
    comparison_df = df1.merge(df2, indicator=True, how='right')
    if which is None:
        diff_df = comparison_df[comparison_df['_merge'] != 'both']
    else:
        diff_df = comparison_df[comparison_df['_merge'] == which]
    return diff_df
    # return comparison_df

if __name__ == '__main__':
    execute()


cursor.close()
connect.close()

#変更された基地局に接続
for num in range(slave_num):
    if ((num) in d_slave.keys()) == True:
        connect = mysql.connector.connect(
            user='root',
            password='leon0704',
            host='localhost',
            database=d_slave[num]
        )
        # 接続できているかどうか確認
        #print(connect.is_connected())
        #カーソルを作成
        cursor = connect.cursor()

        test = d_slave[num]
        #print(test.strip())
        #変数情報のリスト化
        row = [int(d_slave_num[num]), d_slave[num], int(d_slave_1[num]), int(d_slave_2[num]), int(d_slave_3[num])]

        #print(row)
        #テーブルに挿入
        intable=("INSERT INTO {} (base_num, base_name, parameter1, parameter2, parameter3) VALUES (%s,%s,%s,%s,%s)").format(test)
        cursor.execute(intable, row)
        #print("INSERT INTO %s (base_num, base_name, parameter1, parameter2, parameter3) VALUES (%s,%s,%s,%s,%s)" % row)

        
        #変更を反映させる
        connect.commit()

        cursor.close()
        connect.close()

