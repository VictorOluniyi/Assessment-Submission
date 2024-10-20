### Assessment-Submission

### Question 3
#Import Spark session
from pyspark.sql import SparkSession

#Initialize a Spark session
spark = SparkSession.builder \
    .appName("TaskLogProcessing") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "4") \
    .getOrCreate()

#Read the dataset
df = spark.read.csv("record.csv", header=True, inferSchema=True)

#Show schema to verify the structure of the dataset
df.printSchema()

#Aggregating the total hours logged by project
#Group by 'project_id' and sum up the 'hours_logged'
project_hours = df.groupBy("project_id").sum("hours_logged")

#Rename the aggregated column for clarity
project_hours = project_hours.withColumnRenamed("sum(hours_logged)", "total_hours_logged")

#Sorting the result by total hours logged in descending order
project_hours = project_hours.orderBy("total_hours_logged", ascending=False)

#Show the top 20 projects by hours logged
project_hours.show(20)

# Save the result to a new CSV file
project_hours.write.csv("project_hours_aggregated.csv", header=True)

# Stop the Spark session after processing
spark.stop()
