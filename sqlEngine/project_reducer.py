#!/usr/bin/python3
import sys
print()
print()

values = []
count = 1
for lines in sys.stdin:

    column,value = lines.strip("\n").split(",")

    if(count):
        print(column)
        print()
        count = 0

    
    if(value not in values):
        values.append(value)
        print(value)
    
