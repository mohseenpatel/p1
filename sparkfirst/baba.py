from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc=spark.sparkContext

data="C:\\Users\\Dell\\Desktop\\Python\\Pandas\\datasets\\testing.csv"
drdd=sc.textFile(data)
res=drdd.map(lambda x:x.split(",")).toDF(["PassengerId","Survived","Pclass","Name","Sex","Age","SibSp","Parch","Ticket","Fare","Cabin","Embarked"])
#res.show()
res.createOrReplaceTempView("titu")
result=spark.sql("select * from titu where age = 22 and sex='male'")
#result=res.where(col("age")==30)
result.show()


