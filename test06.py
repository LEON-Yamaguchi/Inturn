import mysql.connector
import csv
import configparser

inifile = configparser.ConfigParser()
inifile.read("config/kishoudata.ini")
dbcon = mysql.connector.connect(
  database=inifile.get("kishoudata", "kishoudata"),
  user=inifile.get("kishoudata", "root"),
  password=inifile.get("kishoudata", "leon0704"),
  host=inifile.get("kishoudata", "localhost")
)
dbcur = dbcon.cursor()
with open('data.csv') as csvfile:
  reader = csv.reader(csvfile)
  next(reader, None)
  for row in reader:
    dbcur.execute('INSERT INTO restaurants_payments_methods (restaurant_id, payment_method) VALUES(%s, "%s")' % tuple(row))
dbcon.commit()
