import csv
with open('csv/data.csv', newline='') as csvfile:
    read = csv.reader(csvfile)
    for row in read:
        print(row)