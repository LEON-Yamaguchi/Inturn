import MySQLdb

# 接続する
conn = MySQLdb.connect(
     unix_socket = '/Applications/MAMP/tmp/mysql/mysql.sock',
    user='root',
    passwd='leon0704',
    host='localhost',
    db='mysql')

# 接続を閉じる
con.close
