#!/usr/bin/python3
import sys

count=0
for lines in sys.stdin:
	rows = lines.strip("\n").split(",")
	col = rows[0]
	count=count+1
	if(count>4):
		print(col,col,sep=',')
