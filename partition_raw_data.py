import os
from datetime import datetime, timedelta
import pandas as pd
from pyhive import hive

# Đường dẫn đến thư mục chính chứa các folder được đánh số tăng dần
input_base_dir = "C:/Users/kkagi/Downloads/ProjectSummer2024/data_crawled"
output_dir_base = "/data_lake_partition"

# Kết nối đến hive
conn = hive.Connection(host="localhost", port=10002, username="kkagi")  # Thay đổi thông tin kết nối nếu cần
cursor = conn.cursor()

# Tạo bảng phân vùng trên Hive nếu chưa tồn tại
table_name = "car_data"
cursor.execute(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {table_name} (
        title STRING,
        cash_price STRING,
        finance_price STRING,
        finance_details STRING,
        exterior STRING,
        interior STRING,
        mileage STRING,
        fuel_type STRING,
        mpg STRING,
        transmission STRING,
        drivetrain STRING,
        engine STRING,
        location STRING,
        vin STRING,
        stock_number STRING,
        features STRING
    )
    PARTITIONED BY (posted_date STRING)
    STORED AS TEXTFILE
    LOCATION '{output_dir_base}/{table_name}'
""")

# Duyệt qua các folder trong thư mục chính
for folder in os.listdir(input_base_dir):
    folder_path = os.path.join(input_base_dir, folder)

    if os.path.isdir(folder_path):
        # Duyệt qua các file TXT trong mỗi folder
        for filename in os.listdir(folder_path):
            if filename.endswith(".txt"):
                with open(os.path.join(folder_path, filename), 'r') as f:
                    data = {}
                    for line in f:
                        if ": " in line:
                            key, value = line.strip().split(": ", 1)
                            data[key] = value

                    # Tính toán ngày đăng bài
                    listed_days_ago = int(data.get('Listed Since', 'Listed 0 days ago').split()[1])
                    posted_date = (datetime.now() - timedelta(days=listed_days_ago)).strftime('%Y-%m-%d')

                    # Tạo DataFrame từ dữ liệu
                    df = pd.DataFrame([{
                        'title': data.get('Title', ''),
                        'cash_price': data.get('Cash Price', ''),
                        'finance_price': data.get('Finance Price', ''),
                        'finance_details': data.get('Finance Details', ''),
                        'exterior': data.get('Exterior', ''),
                        'interior': data.get('Interior', ''),
                        'mileage': data.get('Mileage', ''),
                        'fuel_type': data.get('Fuel Type', ''),
                        'mpg': data.get('MPG', ''),
                        'transmission': data.get('Transmission', ''),
                        'drivetrain': data.get('Drivetrain', ''),
                        'engine': data.get('Engine', ''),
                        'location': data.get('Location', ''),
                        'vin': data.get('VIN', ''),
                        'stock_number': data.get('Stock Number', ''),
                        'features': data.get('Features', ''),
                        'posted_date': posted_date
                    }])

                    # Lưu DataFrame vào bảng Hive, tự động phân vùng theo ngày đăng bài
                    temp_csv_path = f"C:/temp/{filename}.csv"
                    df.to_csv(temp_csv_path, index=False, header=False)  # Lưu tạm vào file CSV

                    cursor.execute(
                        f"LOAD DATA LOCAL INPATH '{temp_csv_path}' INTO TABLE {table_name} PARTITION (posted_date='{posted_date}')")
                    os.remove(temp_csv_path)  # Xóa file tạm sau khi tải lên HDFS

# Đóng kết nối Hive
cursor.close()
conn.close()
