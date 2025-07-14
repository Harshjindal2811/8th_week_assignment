
# 🚖 Project: NYC Taxi Data Analysis Using Azure Databricks

This project demonstrates how to load NYC Yellow Taxi trip data from Azure Blob Storage into Azure Databricks and run analytical queries using PySpark and SQL. The results are then saved in an external Parquet format for further use.

---

## 🔧 Technologies Used

- **Azure Blob Storage** – For storing raw CSV data  
- **Azure Databricks** – For data processing and analytics  
- **Apache Spark (PySpark)** – For distributed computation  
- **Parquet** – For saving optimized output  

---

## 📥 Input Dataset

- **File Name**: `nyc-taxi-data.csv`  
- **Storage Account**: `assgn`  
- **Container**: `nyc-taxi-data`  
- **Blob URL**:  
  ```
  wasbs://nyc-taxi-data@assgn.blob.core.windows.net/nyc-taxi-data.csv
  ```

---

## 🛠️ Setup Instructions

### 1️⃣ Connect Azure Blob Storage

```python
spark.conf.set(
  "fs.azure.account.key.assgn.blob.core.windows.net",
  "my-storage-account-access-key"
)
```

---

### 2️⃣ Load Dataset into DataFrame

```python
file_path = "wasbs://nyc-taxi-data@assgn.blob.core.windows.net/nyc-taxi-data.csv"

df = spark.read.csv(file_path, header=True, inferSchema=True)
```

---

### 3️⃣ Run Analytical Queries

- ✅ **Query 1**: Add a column named `Revenue` into the DataFrame which is the sum of the following columns: `Fare_amount`, `Extra`, `MTA_tax`, `Improvement_surcharge`, `Tip_amount`, `Tolls_amount`, and `Total_amount`.

- ✅ **Query 2**: Analyze the increasing count of total passengers in New York City by area.

- ✅ **Query 3**: Get real-time average fare and total earning amount earned by the two vendors.

- ✅ **Query 4**: Calculate the moving count of payments made by each payment mode.

- ✅ **Query 5**: Identify the top two highest-gaining vendors on a particular date, along with the number of passengers and total cab distance.

- ✅ **Query 6**: Find the route (between two locations) that had the most passengers.

- ✅ **Query 7**: Get top pickup locations with the most passengers in the last 5 to 10 seconds.

---

### 4️⃣ Save DataFrame as External Parquet File

```python
output_path = "wasbs://nyc-taxi-data@assgn.blob.core.windows.net/"
 
df.write.mode("overwrite").parquet(output_path)
```
