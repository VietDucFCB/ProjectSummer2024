import os
from datetime import datetime, timedelta
import pandas as pd
from pyhive import hive  # Cần cài đặt thư viện pyhive

# Đường dẫn đến thư mục chứa các file TXT
input_dir = "C:/Users/kkagi/Downloads/ProjectSummer2024/data_crawled/1"

# Đường dẫn đến thư mục trên HDFS để lưu trữ dữ liệu đã xử lý
output_dir_base = "/data_lake"

# Kết nối đến Hive
conn = hive.Connection(host="localhost", port=10000, username="kkagi") # Thay đổi thông tin kết nối nếu cần
cursor = conn.cursor()

# Duyệt qua các file TXT trong thư mục
for filename in os.listdir(input_dir):
    if filename.endswith(".txt"):
        with open(os.path.join(input_dir, filename), 'r') as f:
            data = {}
            for line in f:
                key, value = line.strip().split(": ", 1)
                data[key] = value

            # Tính toán ngày đăng bài
            listed_days_ago = int(data['Listed Since'].split()[1])
            posted_date = (datetime.now() - timedelta(days=listed_days_ago)).strftime('%Y-%m-%d')

            # Tạo DataFrame từ dữ liệu
            df = pd.DataFrame([{'Title': data['Title'], 'Cash Price': data['Cash Price'],
                                # ... các cột dữ liệu khác
                                'Posted Date': posted_date}])

            # Tạo bảng phân vùng trên HDFS nếu chưa tồn tại
            table_name = "car_data"
            cursor.execute(f"""
                CREATE EXTERNAL TABLE IF NOT EXISTS {table_name} (
                    title STRING,
                    cash_price STRING,
                    -- ... các cột dữ liệu khác
                    posted_date STRING
                )
                PARTITIONED BY (posted_date STRING)
                LOCATION '{output_dir_base}'
            """)

            # Lưu DataFrame vào bảng Hive, tự động phân vùng theo ngày đăng bài
            df.to_csv(f"C:/temp/{filename}.csv", index=False, header=False) # Lưu tạm vào file CSV
            cursor.execute(f"LOAD DATA LOCAL INPATH 'C:/temp/{filename}.csv' INTO TABLE {table_name} PARTITION (posted_date='{posted_date}')")
            os.remove(f"C:/temp/{filename}.csv") # Xóa file tạm sau khi tải lên HDFS

# Đóng kết nối Hive
cursor.close()
conn.close()