import csv
import random
import os

# with open("titan_section_train.csv","r") as f:
# 	reader = f.readlines()
# 	random.shuffle(reader)
# 	train_index = int(len(reader) * 0.8)
# 	train_csv = reader[:train_index]

# 	eval_csv = reader[train_index:]

# 	with open("train/train.csv","w") as f1:
# 		f1.writelines(train_csv)

# 	with open("eval/eval.csv","w") as f2:
# 		f2.writelines(eval_csv)


cmd = "cp titan_section_test.csv test/test.csv"
n = os.system(cmd)
if n != 0:
	print("make_test_data error.")