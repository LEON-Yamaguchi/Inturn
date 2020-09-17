import mysql.connector as mydb

# コネクションの作成
conn = mydb.connect(
    host='localhost',
    port='3306',
    user='root',
    password='leon0704',
    database='kishoudata'
)

# コネクションが切れた時に再接続してくれるよう設定
conn.ping(reconnect=True)

# 接続できているかどうか確認
print(conn.is_connected())

# DB操作用にカーソルを作成
cur = conn.cursor()
