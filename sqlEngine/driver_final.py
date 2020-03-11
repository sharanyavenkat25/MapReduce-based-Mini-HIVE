#!/usr/bin/python3
import sys
import os
import subprocess
import logging
import re

#Possible commands
#1) SELECT * FROM database_name/table_name.csv; --> PROJECT statement
#2) SELECT column_name FROM database_name/table_name.csv; --> PROJECT STATEMENT
#3) SELECT column_name FROM database_name/table_name.csv WHERE column_name = value; --> PROJECT WITH SELECT
#4) SELECT * FROM database_name/table_name.csv WHERE column_name = value;
#5) SELECT column_name FROM database_name/table_name.csv WHERE column_name = value AGGREGATE_BY count(,max,min);


#where sys arg[1] =1
#	sys arg[2] =*/column name if * then pass all


sys.stderr=open("ErrorLogs.txt","a")

print("----------------------------------------------------------")
print("\t\t\tMINI HIVE")
print("----------------------------------------------------------")
print("\n")
print("\t\t----------GUIDE----------")
print("\t\tQUICK SQL COMMANDS LOOK UP")
print("\t 1)LOAD \t 2)SELECT \t 3)exit")
print("\t\t-------------------------")
iteration=0
success=0
while(1):
	print("\n>>>",end="")
	q=input()
	query=q.split()
	query_len=len(query)
	if(iteration==0):
		if(query[0] == "LOAD"):
			filename=query[1]
			m=re.search(r"\(([A-Za-z0-9:,_ ]+)\)",q)
			if(type(m)==type(None)):
				print("\nMESSAGE : *****Error in Syntax*****\n")
			else:
				f=open("schema.txt","w")
				l=m.group(1)
				print(m.group(1))
				f.write(l)
				f.close()
				success=1
			if(success==1):
					print("MESSAGE : Schema Loaded Successfully")
		else:
			print("\nMESSAGE : *****Please load the database/table first*****\n")
	else:
		if(query[0]=="LOAD"):
			if(success==1):
				print("\nMESSAGE : *****Database/table already loaded*****\n")
			if(success==0):
				filename=query[1]
				m=re.search(r"\(([A-Za-z0-9:,_ ]+)\)",q)
				if(type(m)==type(None)):
					print("\nMESSAGE : *****Error in Syntax*****\n")
				else:
					f=open("schema.txt","w")
					l=m.group(1)
					print(m.group(1))
					f.write(l)
					f.close()
					success=1
				if(success==1):
					print("MESSAGE : Schema Loaded Successfully")
		
		if(query[0]=="SELECT"):
			#print(len(query), query[4])
			if(query[3]!=filename):
				print("\nMESSAGE : *****The table does not exist or has not been loaded*****\n")
				break
			#Simple Select
			if(query[1]=="*" and query_len==4):
				print("Running...")
				cmd="hadoop jar /home/hadoop/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.1.2.jar -files example.py,select_reducer.py -mapper \"python3 example.py 2 all \" -reducer \"python3 select_reducer.py \" -input {} -output /plx 2>Hadoop_Logs".format(filename)
				pr=subprocess.check_output(cmd,shell=True)
				cmd="hdfs dfs -cat /plx/part-00000 2>Hadoop_Logs"
				pr=subprocess.check_output(cmd,shell=True)
				print((pr).decode('ascii'))
				cmd="hdfs dfs -rm -r /plx 2>Hadoop_Logs"
				pr=subprocess.check_output(cmd,shell=True)
				#os.system(cmd)
				#print(pr)


			#Project query
			elif(query[1]!="*" and query_len==4):
				project_col=query[1]
				print("Running...")
				cmd="hadoop jar /home/hadoop/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.1.2.jar -files example.py,project_reducer.py -mapper \"python3 example.py 0 {} \" -reducer \"python3 project_reducer.py \" -input {} -output /plx 2>Hadoop_Logs".format(project_col,filename)
				pr=subprocess.check_output(cmd,shell=True)
				#print(pr)
				cmd="hdfs dfs -cat /plx/part-00000 2>Hadoop_Logs"
				pr=subprocess.check_output(cmd,shell=True)
				print((pr).decode('ascii'))
				cmd="hdfs dfs -rm -r /plx 2>Hadoop_Logs"
				pr=subprocess.check_output(cmd,shell=True)
	
		    #Select with where
			elif(query[4]=="WHERE" and query_len==8):
				def symbol(s):
					switcher= {
						"<":"l",
						">":"g",
						"<=":"le",
						">=":"ge",
						"=":"="
		   			}
					return switcher.get(s)
				
				proj_col=query[1]
				cond_col=query[5]
				#cond=query[5]
				cond_val=query[7]
				sign=symbol(query[6])
				#print(sign)
				if(proj_col=="*"):
					print("Running...")
					cmd="hadoop jar /home/hadoop/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.1.2.jar -files example.py,select_reducer.py -mapper \"python3 example.py 1 all {} {} {} \" -reducer \"python3 select_reducer.py \" -input {} -output /plx 2>Hadoop_Logs".format(cond_col,sign,cond_val,filename)
					#print(cmd)
					pr=subprocess.check_output(cmd,shell=True)
					cmd="hdfs dfs -cat /plx/part-00000 2>Hadoop_Logs"
					pr=subprocess.check_output(cmd,shell=True)
					print((pr).decode('ascii'))
					cmd="hdfs dfs -rm -r /plx 2>Hadoop_Logs"
					pr=subprocess.check_output(cmd,shell=True)
				else:
						print("Running...")
						cmd="hadoop jar /home/hadoop/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.1.2.jar -files example.py,select_reducer.py -mapper \"python3 example.py 1 {} {} {} {} \" -reducer \"python3 select_reducer.py \" -input {} -output /plx 2>Hadoop_Logs".format(proj_col,cond_col,sign,cond_val,filename)
						pr=subprocess.check_output(cmd,shell=True)
						cmd="hdfs dfs -cat /plx/part-00000 2>Hadoop_Logs"
						pr=subprocess.check_output(cmd,shell=True)
						print((pr).decode('ascii'))
						cmd="hdfs dfs -rm -r /plx 2>Hadoop_Logs"
						pr=subprocess.check_output(cmd,shell=True)

			#AGGREGATE FUNCTIONS
			#min
			elif('AGGREGATE_BY' in query and query[-1]=='min' and query[1]!="*"):
				if(len(query) == 6):
					project_col=query[1]
					print("Running...")
					cmd="hadoop jar /home/hadoop/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.1.2.jar -files example.py,project_reducer.py -mapper \"python3 example.py 0 {} \" -reducer \"python3 reducer_agg.py 0 \" -input {} -output /plx 2>Hadoop_Logs".format(project_col,filename)
					pr=subprocess.check_output(cmd,shell=True)
					#print(pr)
					cmd="hdfs dfs -cat /plx/part-00000 2>Hadoop_Logs"
					pr=subprocess.check_output(cmd,shell=True)
					print((pr).decode('ascii'))
					cmd="hdfs dfs -rm -r /plx 2>Hadoop_Logs"
					pr=subprocess.check_output(cmd,shell=True)
				elif(len(query) == 10):
						def symbol(s):
							switcher= {
								"<":"l",
								">":"g",
								"<=":"le",
								">=":"ge",
								"=":"="
		   					}
							return switcher.get(s)
				
						proj_col=query[1]
						cond_col=query[5]
						#cond=query[5]
						cond_val=query[7]
						sign=symbol(query[6])
						#print(sign)
						print("Running...")
						cmd="hadoop jar /home/hadoop/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.1.2.jar -files example.py,select_reducer.py -mapper \"python3 example.py 1 {} {} {} {} \" -reducer \"python3 reducer_agg.py 0 \" -input {} -output /plx 2>Hadoop_Logs".format(proj_col,cond_col,sign,cond_val,filename)
						pr=subprocess.check_output(cmd,shell=True)
						cmd="hdfs dfs -cat /plx/part-00000"
						pr=subprocess.check_output(cmd,shell=True)
						print((pr).decode('ascii'))
						cmd="hdfs dfs -rm -r /plx 2>Hadoop_Logs"
						pr=subprocess.check_output(cmd,shell=True)

			#max
			elif('AGGREGATE_BY' in query and query[-1]=='max' and query[1]!="*"):
				if(len(query) == 6):
					project_col=query[1]
					print("Running...")
					cmd="hadoop jar /home/hadoop/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.1.2.jar -files example.py,project_reducer.py -mapper \"python3 example.py 0 {} \" -reducer \"python3 reducer_agg.py 1 \" -input {} -output /plx 2>Hadoop_Logs".format(project_col,filename)
					pr=subprocess.check_output(cmd,shell=True)
					#print(pr)
					cmd="hdfs dfs -cat /plx/part-00000 2>Hadoop_Logs"
					pr=subprocess.check_output(cmd,shell=True)
					print((pr).decode('ascii'))
					cmd="hdfs dfs -rm -r /plx 2>Hadoop_Logs"
					pr=subprocess.check_output(cmd,shell=True)
				elif(len(query) == 10):
						def symbol(s):
							switcher= {
								"<":"l",
								">":"g",
								"<=":"le",
								">=":"ge",
								"=":"="
		   					}
							return switcher.get(s)
				
						proj_col=query[1]
						cond_col=query[5]
						#cond=query[5]
						cond_val=query[7]
						sign=symbol(query[6])
						#print(sign)
						print("Running...")
						cmd="hadoop jar /home/hadoop/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.1.2.jar -files example.py,select_reducer.py -mapper \"python3 example.py 1 {} {} {} {} \" -reducer \"python3 reducer_agg.py 1 \" -input {} -output /plx 2>Hadoop_Logs".format(proj_col,cond_col,sign,cond_val,filename)
						pr=subprocess.check_output(cmd,shell=True)
						cmd="hdfs dfs -cat /plx/part-00000 2>Hadoop_Logs"
						pr=subprocess.check_output(cmd,shell=True)
						print((pr).decode('ascii'))
						cmd="hdfs dfs -rm -r /plx 2>Hadoop_Logs"
						pr=subprocess.check_output(cmd,shell=True)

			#count
			elif('AGGREGATE_BY' in query and query[-1]=='count'):
				if(query[1]=="*" and query_len==6):
					print("Running...")
					cmd="hadoop jar /home/hadoop/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.1.2.jar -files example.py,select_reducer.py -mapper \"python3 example.py 2 all \" -reducer \"python3 reducer_agg.py 2\" -input {} -output /plx 2>Hadoop_Logs".format(filename)
					pr=subprocess.check_output(cmd,shell=True)
					cmd="hdfs dfs -cat /plx/part-00000 2>Hadoop_Logs"
					pr=subprocess.check_output(cmd,shell=True)
					print((pr).decode('ascii'))
					cmd="hdfs dfs -rm -r /plx 2>Hadoop_Logs"
					pr=subprocess.check_output(cmd,shell=True)
					#os.system(cmd)
					#print(pr)
				elif(len(query) == 6 and query[1]!="*"):
					project_col=query[1]
					print("Running...")
					cmd="hadoop jar /home/hadoop/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.1.2.jar -files example.py,project_reducer.py -mapper \"python3 example.py 0 {} \" -reducer \"python3 project_reducer.py 2\" -input {} -output /plx 2>Hadoop_Logs".format(project_col,filename)
					pr=subprocess.check_output(cmd,shell=True)
					#print(pr)
					#cmd="hdfs dfs -cat /plx/part-00000 2>Hadoop_Logs"
					#pr=subprocess.check_output(cmd,shell=True)
					#print((pr).decode('ascii'))
					#cmd="hdfs dfs -rm -r /plx 2>Hadoop_Logs"
					#pr=subprocess.check_output(cmd,shell=True)
					cmd="hadoop jar /home/hadoop/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.1.2.jar -files mapper.py,project_reducer.py -mapper \"python3 mapper.py\" -reducer \"python3 reducer_agg.py 2 \" -input /plx/part-00000 -output /pli 2>Hadoop_Logs".format(project_col,filename)
					pr=subprocess.check_output(cmd,shell=True)
					#print(pr)
					cmd="hdfs dfs -cat /pli/part-00000 2>Hadoop_Logs"
					pr=subprocess.check_output(cmd,shell=True)
					print((pr).decode('ascii'))
					cmd="hdfs dfs -rm -r /plx 2>Hadoop_Logs"
					pr=subprocess.check_output(cmd,shell=True)
					cmd="hdfs dfs -rm -r /pli 2>Hadoop_Logs"
					pr=subprocess.check_output(cmd,shell=True)
				elif(len(query) == 10):
						def symbol(s):
							switcher= {
								"<":"l",
								">":"g",
								"<=":"le",
								">=":"ge",
								"=":"="
		   					}
							return switcher.get(s)
				
						proj_col=query[1]
						cond_col=query[5]
						#cond=query[5]
						cond_val=query[7]
						sign=symbol(query[6])
						#print(sign)
						if(proj_col=="*"):
							print("Running...")
							cmd="hadoop jar /home/hadoop/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.1.2.jar -files example.py,select_reducer.py -mapper \"python3 example.py 1 all {} {} {} \" -reducer \"python3 reducer_agg.py 2\" -input {} -output /plx 2>Hadoop_Logs".format(cond_col,sign,cond_val,filename)
							#print(cmd)
							pr=subprocess.check_output(cmd,shell=True)
							cmd="hdfs dfs -cat /plx/part-00000 2>Hadoop_Logs"
							pr=subprocess.check_output(cmd,shell=True)
							print((pr).decode('ascii'))
							cmd="hdfs dfs -rm -r /plx 2>Hadoop_Logs"
							pr=subprocess.check_output(cmd,shell=True)
						else:
							print("Running...")
							cmd="hadoop jar /home/hadoop/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.1.2.jar -files example.py,select_reducer.py -mapper \"python3 example.py 1 {} {} {} {} \" -reducer \"python3 reducer_agg.py 2\" -input {} -output /plx 2>Hadoop_Logs".format(proj_col,cond_col,sign,cond_val,filename)
							pr=subprocess.check_output(cmd,shell=True)
							cmd="hdfs dfs -cat /plx/part-00000 2>Hadoop_Logs"
							pr=subprocess.check_output(cmd,shell=True)
							print((pr).decode('ascii'))
							cmd="hdfs dfs -rm -r /plx 2>Hadoop_Logs"
							pr=subprocess.check_output(cmd,shell=True)
				else:
					print("Invalid query")

		if(query[0]=="exit"):
			break;
	
	iteration+=1


		
	
	
	

	


