from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, expr # Import expr here
import os
import glob

# Tạo SparkSession
spark = SparkSession.builder \
    .appName("Extract_Car_Data") \
    .getOrCreate()

# Đường dẫn đến thư mục chứa các file txt trên Hadoop
data_lake_path = "hdfs://localhost:9000/data_lake/data_crawled/"

# Đọc tất cả các file txt đệ quy
df_raw = spark.read.option("recursiveFileLookup", "true").text(data_lake_path)

# Định nghĩa các biểu thức chính quy để trích xuất các trường từ văn bản
patterns = {
    'Title': r'Title: (.*)',
    'Cash Price': r'Cash Price: \$(\d+,\d+)',
    'Finance Price': r'Finance Price: \$(\d+)/moEstimate',
    'Finance Details': r'Finance Details: (.*)',
    'Exterior': r'Exterior: (.*)',
    'Interior': r'Interior: (.*)',
    'Mileage': r'Mileage: ([\d,]+) miles',
    'Fuel Type': r'Fuel Type: (.*)',
    'MPG': r'MPG: (\d+ city / \d+ highway)',
    'Transmission': r'Transmission: (.*)',
    'Drivetrain': r'Drivetrain: (.*)',
    'Engine': r'Engine: (.*)',
    'Location': r'Location: (.*)',
    'Listed Since': r'Listed Since: (.*)',
    'VIN': r'VIN: (.*)',
    'Stock Number': r'Stock Number: (.*)',
    # 'Features': r'Features: (.*)',  # Loại bỏ dòng này
}

# Tạo DataFrame mới bằng cách trích xuất các trường dựa trên biểu thức chính quy
df = df_raw
for column, pattern in patterns.items():
    df = df.withColumn(column, regexp_extract(col("value"), pattern, 1))

# Trích xuất trường "Features" bằng cách sử dụng `substring_index`
df = df.withColumn("Features",
                   expr("substring_index(substring_index(value, 'Features: ', -1), 'See less', 1)"))

# Loại bỏ cột "value" gốc
df = df.drop("value")

# Đường dẫn để lưu file CSV đầu ra
output_folder = "C:/Users/kkagi/Downloads/test"

# Ghi DataFrame thành CSV
df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_folder)

# Đợi quá trình ghi hoàn tất trước khi tiếp tục
spark.sparkContext.stop()

# Tiếp tục với việc đổi tên file part
csv_files = glob.glob(f"{output_folder}/part-*.csv")
if csv_files:
    csv_file = csv_files[0]  # Tìm file part CSV

    # Đổi tên thành một tên dễ đọc hơn
    new_csv_file = f"{output_folder}/car_data.csv"
    os.rename(csv_file, new_csv_file)

    print(f"File saved as: {new_csv_file}")
else:
    print("No CSV file found to rename.")