## Assessment-Submission
### Question 1
#### Float Data
<img width="752" alt="Screenshot 2024-10-20 at 23 21 50" src="https://github.com/user-attachments/assets/3936326c-24b8-42ca-80c6-5f76c697b221">

### ETL process
![carbon (3)](https://github.com/user-attachments/assets/edf5e22e-b576-474c-9009-ec7b6f30eb2f)

#### Clickup Data
<img width="791" alt="Screenshot 2024-10-20 at 23 22 58" src="https://github.com/user-attachments/assets/35ef7bce-8624-4dfe-961d-624a848da975">

### Question 2
![Optimized Query](https://github.com/user-attachments/assets/12091d26-df60-43f9-b056-e8115bfc6a14)


#Create Indexes to improve JOIN and filtering performance
 CREATE INDEX idx_clickup_name_hours ON ClickUp(Name, hours);
 CREATE INDEX idx_float_name_estimed_hours ON Float(Name, Estimed_Hours);

#Optimized Query
 SELECT
    c.Name,
    f.Role,
    SUM(c.hours) AS Total_Tracked_Hours,
    SUM(f.Estimed_Hours) AS Total_Allocated_Hours
FROM ClickUp c
JOIN Float f ON c.Name = f.Name
GROUP BY c.Name, f.Role
HAVING SUM(c.hours) > 100
ORDER BY Total_Allocated_Hours DESC;


### Question 3
![carbon (2)](https://github.com/user-attachments/assets/a477338e-d48e-4044-ab64-572b3d383b47)

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

#Save the result to a new CSV file
project_hours.write.csv("project_hours_aggregated.csv", header=True)

#Stop the Spark session after processing
spark.stop()

### Question 4
#### Float Data
<img width="829" alt="Screenshot 2024-10-20 at 23 24 16" src="https://github.com/user-attachments/assets/95ec6984-c715-4a32-ac06-48d94d629792">

#### Clickup Data
<img width="964" alt="Screenshot 2024-10-20 at 23 24 47" src="https://github.com/user-attachments/assets/aa2c874a-f567-4bb8-93c2-48ba67152599">
