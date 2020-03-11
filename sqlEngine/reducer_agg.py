#!/usr/bin/python3
import sys

print()
print()

#SYSARG[1] = 0 ->min
#SYSARG[1] = 1 ->max
#SYSARG[1] = 2 ->count

agg_cond= int(sys.argv[1])

maxi=0
c=1
count=0
for line in sys.stdin:
	if(('error' in line.strip('\n').split(' ') or 'Error' in line.strip('\n').split(' ')) and line.strip('\n').strip(' ').split(',')[0]):
		print(line)
		break
	else:
		if(agg_cond == 0):
			if(c):
				mini=int(line.strip(' ').strip('\n').split(',')[1])
				c=0
			else:
				value=int(line.strip(' ').strip('\n').split(',')[1])
				if(mini>value):
					mini = value
		elif(agg_cond == 1):
			value=int(line.strip(' ').strip('\n').split(',')[1])
			if(maxi<value):
					maxi = value
		elif(agg_cond == 2):
			count=count+1

if(agg_cond==0):
	print("Min Value",mini)
elif(agg_cond==1):
	print("Max Value",maxi)
else:
	print("Count:",count)
