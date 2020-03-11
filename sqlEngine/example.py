#!/usr/bin/python3
import sys

colType = dict()
with open("schema.txt") as file:
	line = file.read().split("\n")
	if(isinstance(line,list)):
		line = line[0]
	col_type = line.split(",")
	for v in col_type:
		colType[v.split(":")[0].lower()] = v.split(":")[1].lower()

all_col = []

for col in colType.keys():
	all_col.append(col)

#Possible commands
#1) SELECT * FROM database_name/table_name.csv; --> PROJECT statement
#2) SELECT column_name FROM database_name/table_name.csv; --> PROJECT STATEMENT
#3) SELECT column_name FROM database_name/table_name.csv WHERE column_name = value; --> PROJECT WITH SELECT
#4) SELECT * FROM database_name/table_name.csv WHERE column_name = value;

#SYSARG[1] = 0 ->PROJECT STATEMENT
#SYSARG[1] = 1 ->SELECT STATEMENTS WITH WHERE CONDITION
#SYSARG[1] = 2 ->SIMPLE SELECT STATEMENT

# 'l' implies < condtion
# 'g' implies > condition
# 'le' and 'ge' represent <= and >= conditions respectively.

count = 0
for lines in sys.stdin:
	#lines=lines.encode('utf-8')
	if(int(sys.argv[1]) == 0): #PROJECT QUERY 
		
		columns = lines.strip("\n").split(",")
		column = sys.argv[2] #COLUMN TO PROJECT
		if(count == 0):
			if(column in colType.keys()):
				index = all_col.index(column) #INDEX OF COLUMN
				count = 1
			else:
				print(-2," Column not there Error",sep=",")
				break
		
		print(column,columns[index],sep=",")


	elif(int(sys.argv[1]) == 1):
		
		columns = lines.strip("\n").split(",")
		proj_column = (sys.argv[2])
		
		condition_col = str(sys.argv[3]) #COLUMN IN THE CONDITION STATEMENT
		
		condition = sys.argv[4]#CONDITION USED
		value = sys.argv[5]#VALUE IN THE CONDITION
		#print(sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4],sys.argv[5],condition_col)
		if(condition_col not in colType.keys()):
			print(-2,"column error",sep=",")
			break
		else:
			index = all_col.index(condition_col) #GET INDEX OF CONDITION COLUMN

		if(colType[condition_col].lower() == "int"):
			try:
				value = int(value)
			except:
				print(-1,"schema error",sep=",")
				break
		elif(colType[condition_col].lower() == "float"):
			try:
				value = float(value)
			except:
				print(-1,"schema error",sep=",")
				break
		elif(colType[condition_col].lower() == "string"):		
			pass
		else:
			print(-1,"schema error",sep=",")
			break
		
		

		if(proj_column in "all"):
			
			if(condition == "="):
				#print(11)
				if(colType[condition_col].lower() == "int" ):
					if(int(columns[index]) == value):
						t = ""
						for c in columns:
							t = t + c + " "
						print(t,t,sep=",")
				elif(colType[condition_col].lower() == "float" ):
					#print("HIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII")
					if(float(columns[index]) == value):
						t = ""
						for c in columns:
							t = t + c + " "
						print(t,t,sep=",")
				else:
					#print(11)
					#print(columns[index],value)
					if(str(columns[index]) == value):
						t = ""
						for c in columns:
							t = t + c + " "
						print(t,t,sep=",")
				


			elif(condition == "g"):
				if(colType[condition_col].lower() == "int" ):
					if(int(columns[index]) > value):
						t = ""
						for c in columns:
							t = t + c + " "
						print(t,t,sep=",")
				elif(colType[condition_col].lower() == "float" ):
					if(float(columns[index]) > value):
						t = ""
						for c in columns:
							t = t + c + " "
						print(t,t,sep=",")
				else:
					if((columns[index]) == value):
						t = ""
						for c in columns:
							t = t + c + " "
						print(t,t,sep=",")

			elif(condition == "l"):
				if(colType[condition_col].lower() == "int" ):
					if(int(columns[index]) < value):
						t = ""
						for c in columns:
							t = t + c + " "
						print(t,t,sep=",")
				elif(colType[condition_col].lower() == "float" ):
					if(float(columns[index]) < value):
						t = ""
						for c in columns:
							t = t + c + " "
						print(t,t,sep=",")
				else:
					if((columns[index]) == value):
						t = ""
						for c in columns:
							t = t + c + " "
						print(t,t,sep=",")

			elif(condition == "ge"):
				if(colType[condition_col].lower() == "int" ):
					if(int(columns[index]) >= value):
						t = ""
						for c in columns:
							t = t + c + " "
						print(t,t,sep=",")
				elif(colType[condition_col].lower() == "float" ):
					if(float(columns[index]) >= value):
						t = ""
						for c in columns:
							t = t + c + " "
						print(t,t,sep=",")
				else:
					if((columns[index]) == value):
						t = ""
						for c in columns:
							t = t + c + " "
						print(t,t,sep=",")

			elif(condition == "le"):
				if(colType[condition_col].lower() == "int" ):
					if(int(columns[index]) <= value):
						t = ""
						for c in columns:
							t = t + c + " "
						print(t,t,sep=",")
				
				elif(colType[condition_col].lower() == "float" ):
					if(float(columns[index]) <= value):
						t = ""
						for c in columns:
							t = t + c + " "
						print(t,t,sep=",")
				else:
					if((columns[index]) == value):
						t = ""
						for c in columns:
							t = t + c + " "
						print(t,t,sep=",")

			else:
				print(-3,"Error",sep=",")
				break

		else:
			#print(1)
			if(proj_column not in colType.keys()):
				print(-2,"project column error in select ",sep=",")
				break
			
			index_p = all_col.index(proj_column) #INDEX OF PROJECT COLUMN

			if(condition == "="):
				if(colType[condition_col].lower() == "int" ):
					if(int(columns[index]) == value):
						print(proj_column,columns[index_p],sep=",")

				elif(colType[condition_col].lower() == "float" ):
					if(float(columns[index]) == value):
						print(proj_column,columns[index_p],sep=",")
				else:
					#print(colType[condition_col],value)
					if(columns[index] == value):
						print(proj_column,columns[index_p],sep=",")

			elif(condition == "g"):
				if(colType[condition_col].lower() == "int" ):
					if(int(columns[index]) > value):
						print(proj_column,columns[index_p],sep=",")
				elif(colType[condition_col].lower() == "float" ):
					if(float(columns[index]) > value):
						print(proj_column,columns[index_p],sep=",")
				else:
					if(columns[index] == value):
						print(proj_column,columns[index_p],sep=",")

			elif(condition == "l"):
				if(colType[condition_col].lower() == "int" ):
					if(int(columns[index]) < value):
						print(proj_column,columns[index_p],sep=",")
				elif(colType[condition_col].lower() == "float" ):
					if(float(columns[index]) < value):
						print(proj_column,columns[index_p],sep=",")
				else:
					if(columns[index] == value):
						print(proj_column,columns[index_p],sep=",")

			elif(condition == "ge"):
				if(colType[condition_col].lower() == "int" ):
					if(int(columns[index]) >= value):
						print(proj_column,columns[index_p],sep=",")
				elif(colType[condition_col].lower() == "float" ):
					if(float(columns[index]) >= value):
						print(proj_column,columns[index_p],sep=",")
				else:
					if(columns[index] == value):
						print(proj_column,columns[index_p],sep=",")

			elif(condition == "le"):
				if(colType[condition_col].lower() == "int" ):
					if(int(columns[index]) <= value):
						print(proj_column,columns[index_p],sep=",")
				elif(colType[condition_col].lower() == "float" ):
					if(float(columns[index]) <= value):
						print(proj_column,columns[index_p],sep=",") #PRINT THE COLUMN IN THE SELECT STATEMENT NOT THE CONDITION COLUMN
				else:
					if(columns[index] == value):
						print(proj_column,columns[index_p],sep=",")

			else:
				print(-3,"Error",sep=",")
				break

	elif(int(sys.argv[1]) == 2): 	#SIMPLE SELECT STATEMENT
		columns = lines.strip("\n").split(",")
		t = ""
		for c in columns:
			t = t + c + " "
		print(t,t,sep=",")







				


	
	
