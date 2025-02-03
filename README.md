# CourseraDataBricksCourseFixes

## For the module 7 (3.Exercise-Deduplication-of-Data)
The code is dirty, but I don't have an aim to make it brilliant perfect. Just make it work, that's it.

1. Upload the file from the repo into your databricks workspace - [customer_data.csv](https://github.com/Antonio-Ingegnere/CourseraDataBricksCourseFixes/blob/main/customer_data.csv)
2. Use the code below. It perfectly passes the final test.

![image](https://github.com/user-attachments/assets/c6f393bd-df28-448a-80cb-8cb8875d3e91)


```python
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import regexp_replace

(source, sasEntity, sasToken) = getAzureDataSource()
spark.conf.set(sasEntity, sasToken)

source = "wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net"

#sourceFile = userhome + "/customer_data.csv"
#PUT HERE YOUR USER INSTEAD OF PUT_YOUR_USERNAME_HERE
sourceFile = "/Workspace/Users/PUT_YOUR_USERNAME_HERE/customer_data.csv"
destFile = userhome + "/people.parquet"

# In case it already exists
dbutils.fs.rm(destFile, True)

#print(userhome)

# Move file from workspace to DBFS
dbutils.fs.cp("file:/Workspace/Users/PUT_YOUR_USERNAME_HERE/customer_data.csv", "dbfs:/FileStore/customer_data.csv")

df = spark.read.option("header", "true").csv("dbfs:/FileStore/customer_data.csv")

# df = ( 
#       df.withColumn("First Name", lower(col("First Name"))) 
#        .withColumn("Middle Name", lower(col("Middle Name"))) 
#        .withColumn("Last Name", lower(col("Last Name"))) 
#        .withColumn("SSN", regexp_replace(col("SSN"), "-", "")) 
#       )
tmpDF = ( 
         df.withColumnRenamed("First Name", "First Name").select(col("*"), lower(col("First Name")).alias("LFirstName"))
          .withColumnRenamed("Middle Name", "Middle Name").select(col("*"), lower(col("Middle Name")).alias("LMiddleName"))
        #   .withColumn("MiddleName", lower(col("Middle Name"))).select(col("*"), col("MiddleName").alias("LMiddleName"))
          .withColumnRenamed("Last Name", "Last Name").select(col("*"), lower(col("Last Name")).alias("LLastName"))
          #.withColumnRenamed("SSN", "SSN").select(col("*"), lower(col("SSN")).alias("SSN"))")
          .withColumnRenamed("SSN", "SSN").select(col("*"), regexp_replace(col("SSN"), "-", "").alias("UpdatedSSN"))
          .dropDuplicates(["LFirstName", "LMiddleName", "LLastName", "UpdatedSSN", "Gender", "Birth Date", "Salary"])
         )
# Show first few rows
df.printSchema()

# tmpDF.show(5)

print(df.count())
print(tmpDF.count())

tmpDF = (tmpDF.select("First Name","Middle Name", "Last Name", "Gender", "Birth Date", "SSN", "Salary"))

tmpDF.repartition(8).write.mode("overwrite").parquet(destFile)
```
