# -*- coding: utf-8 -*- 

import sys
from operator import add
from pyspark import SparkConf
from pyspark import SparkContext
from sklearn.svm import SVC
from sklearn.svm import LinearSVC
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier

####################################################################################
############################           用户特征           ############################
####################################################################################
def extract1(line):
	import time
	(uid, iid, ict) = line.strip().split("\t")[0].split(" ")
	items = filter(lambda x:x[0]>0, [(int(time.mktime(time.strptime('2014-'+etime,'%Y-%m-%d-%H'))-time.mktime(time.strptime('2014-'+i.split(",")[0],'%Y-%m-%d-%H')))/(24*3600)+1, int(i.split(",")[1])) for i in line.strip().split("\t")[1].split(" ")])
	return (uid,items)

def extract2(items_list):
	import itertools
	items, items_buy, items_buy_3, f, inf = [], [], [], [0]*39, 100
	f[32] = len(items_list) # 交互商品数
	for i in items_list:
		if len(filter(lambda x:x[1]==4,i))>0:
			items_buy.append(i)
		if len(filter(lambda x:x[1]==4 and x[0]<=3,i))>0:
			items_buy_3.append(i)	
		items.extend(i)
	f[33] = len(items_buy) # 购买商品数
	f[34] = len(items_buy_3) # 三天内购买商品数
	f[35] = len(filter(lambda x:len(x)==1,items_list)) # 只有过一次交互的商品数
	f[36] = len(filter(lambda x:len(x)==2,items_list)) # 有过两次交互的商品数
	f[37] = len(filter(lambda x:len(x)==3,items_list)) # 有过三次交互的商品数
	items = sorted(items, key=lambda x:x[0], reverse=True)
	buy = filter(lambda x:x[1]==4, items)
	last = buy[-1][0] if len(buy)!=0 else inf
	f[24] = len(filter(lambda x:x[0]<=1 and x[1]==1, items)) # 最后1天点击次数
	f[25] = len(filter(lambda x:x[0]<=1 and x[1]==2, items)) # 最后1天加收次数
	f[26] = len(filter(lambda x:x[0]<=1 and x[1]==3, items)) # 最后1天加购次数
	f[27] = len(filter(lambda x:x[0]<=1 and x[1]==4, items)) # 最后1天购买次数
	f[28] = len(filter(lambda x:x[0]<=3 and x[1]==1, items)) # 最后3天点击次数
	f[29] = len(filter(lambda x:x[0]<=3 and x[1]==2, items)) # 最后3天加收次数
	f[30] = len(filter(lambda x:x[0]<=3 and x[1]==3, items)) # 最后3天加购次数
	f[31] = len(filter(lambda x:x[0]<=3 and x[1]==4, items)) # 最后3天购买次数
	f[0] = len(filter(lambda x:x[0]<=7 and x[1]==1, items)) # 最后1周点击次数
	f[1] = len(filter(lambda x:x[0]<=7 and x[1]==2, items)) # 最后1周加收次数
	f[2] = len(filter(lambda x:x[0]<=7 and x[1]==3, items)) # 最后1周加购次数
	f[3] = len(filter(lambda x:x[0]<=7 and x[1]==4, items)) # 最后1周购买次数
	f[4] = len(filter(lambda x:x[0]<=21 and x[1]==1, items)) # 最后3周点击次数
	f[5] = len(filter(lambda x:x[0]<=21 and x[1]==2, items)) # 最后3周加收次数
	f[6] = len(filter(lambda x:x[0]<=21 and x[1]==3, items)) # 最后3周加购次数
	f[7] = len(filter(lambda x:x[0]<=21 and x[1]==4, items)) # 最后3周购买次数
	f[8] = min(1.0,round(1.0*f[3]/f[0],4)) if f[0]!=0 else 0.0 # 最后1周点击转化率
	f[9] = min(1.0,round(1.0*f[3]/f[1],4)) if f[1]!=0 else 0.0 # 最后1周加收转化率
	f[10] = min(1.0,round(1.0*f[3]/f[2],4)) if f[2]!=0 else 0.0 # 最后1周加购转化率
	f[11] = min(1.0,round(1.0*f[7]/f[4],4)) if f[4]!=0 else 0.0 # 最后3周点击转化率
	f[12] = min(1.0,round(1.0*f[7]/f[5],4)) if f[5]!=0 else 0.0 # 最后3周加收转化率
	f[13] = min(1.0,round(1.0*f[7]/f[6],4)) if f[6]!=0 else 0.0 # 最后3周加购转化率
	f[14] = last # 最后一次购买距离天数
	f[15] = len(set([item[0] for item in items if item[0]<=3])) # 最后3天内交互天数
	f[16] = len(set([item[0] for item in items if item[0]<=7])) # 最后1周内交互天数
	f[17] = len(set([item[0] for item in items if item[0]<=21])) # 最后3周内交互天数
	f[18] = items[-1][0] if len(items)!=0 else inf # 最后1次交互距离天数
	inter = [len(list(i)) for _,i in itertools.groupby(items, lambda x: x[0])]
	f[19] = len(inter) #交互天数
	f[20] = max(inter) if len(inter)!=0 else 0 #交互最多的一天交互次数
	f[21] = len(filter(lambda x:x[0]<=1 and x[1]==4, items)) # 最后1天购买次数
	f[22] = len(filter(lambda x:x[0]<=3 and x[1]==4, items)) # 最后3天购买次数
	f[23] = len(filter(lambda x:x[0]<=7 and x[1]==4, items)) # 最后7天购买次数
	f[38] = round(1.0*len(items)/f[32],4) if f[32]!=0 else 0.0 # 用户对每件商品的平均交互次数
	return "\t".join([str(i) for i in f])
	
global etime
global subset

if __name__ == "__main__":
	import fileinput

	conf = (SparkConf()
			.setMaster("Local")
			.setAppName("MyAp")
	sc = SparkContext(conf = conf)
	lines = sc.textFile('competition_tianchi/uid_iid')
	target, etime, subset = "12-19-0", "12-18-23", {}
	# target, etime, subset = "12-18-0", "12-17-23", {}
	# target, etime, subset = "12-17-0", "12-16-23", {}
	# target, etime, subset = "12-16-0", "12-15-23", {}
	# target, etime, subset = "12-15-0", "12-14-23", {}
	# target, etime, subset = "12-14-0", "12-13-23", {}
	# target, etime, subset = "12-13-0", "12-12-23", {}
	# target, etime, subset = "12-12-0", "12-11-23", {}
	# target, etime, subset = "12-11-0", "12-10-23", {}
	# target, etime, subset = "12-10-0", "12-09-23", {}
	# target, etime, subset = "12-09-0", "12-08-23", {}
	# target, etime, subset = "12-08-0", "12-07-23", {}
	# target, etime, subset = "12-07-0", "12-06-23", {}
	# target, etime, subset = "12-06-0", "12-05-23", {}
	# target, etime, subset = "12-05-0", "12-04-23", {}
	# target, etime, subset = "12-04-0", "12-03-23", {}
	# target, etime, subset = "12-03-0", "12-04-23", {}
	# target, etime, subset = "12-02-0", "12-01-23", {}
	# target, etime, subset = "12-01-0", "11-30-23", {}
	for line in fileinput.input("tianchi_mobile_recommend_train_item.csv"):
		subset[line.split(",")[0]] = True
	counts = lines.map(lambda x : extract1(x))\
				  .groupByKey()\
				  .map(lambda x : x[0]+"\t"+extract2(x[1]))
	output = counts.saveAsTextFile("competition_tianchi/feature/"+target+"/user/")
