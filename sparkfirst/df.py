from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc=spark.sparkContext
data="C:\\Users\\Dell\\Desktop\\datasets-df-series\\nba.csv"
drdd=sc.textFile(data)
res=drdd.filter(lambda x: "Age" not in x).map(lambda x:x.split(",")).map(lambda x:(x[0],x[1])).reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[1])
for i in res.collect():
    print(i)