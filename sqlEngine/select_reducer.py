#!/usr/bin/python3
import sys
print()
print()

f = open("schema.txt","r")
colType = dict()
for line in f:
	col_type = line.split(",")
	for v in col_type:
		colType[v.split(":")[0].lower()] = v.split(":")[1].lower()

all_col = ""

for col in colType.keys():
	all_col = all_col + col + " "

values = []
count = 1
for lines in sys.stdin:

    rows = lines.strip("\n").split(",")
    if(rows[0] == rows[1]):
        if(count):
            print(all_col)
            print()
            count = 0
        print(rows[0])

    else:
        col = rows[0]
        value = rows[1]
        if(count):
            print(col)
            print()
            count = 0
        print(value)
