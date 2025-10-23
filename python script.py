!pip install snowflake-connector-python
#----------------------------------------------------
import json
import random
import datetime
import time
import kagglehub
import pandas as pd
import os
import snowflake.connector


#--------------------------------------------------------------------------------------------------------
# Download dataset
path = kagglehub.dataset_download("utkarshshrivastav07/product-sales-and-marketing-analytics-dataset")

# print("Path to dataset files:", path)

# List files in the dataset directory
files = os.listdir(path)
# print("Files:", files)

# Pick the correct CSV file (example: "shopify_products.csv" or similar)
csv_file = [f for f in files if f.endswith(".csv")][0]

# Read the CSV
p = pd.read_csv(os.path.join(path, csv_file))
p['ProductID'] = range(1,len(p)+1)
p = p[['ProductID','Product_Name','Category','Price']]

# p.head()
# p.info()
p =p.values.tolist()


#----------------------------------------------------
conn = snowflake.connector.connect(
    user="WARDOX",
    password="Company684421je0437",
    account="UDPDCIF-TJ97358",   # e.g. abcde-xy12345.snowflakecomputing.com
    warehouse="COMPUTE_WH",
    database="SALES_ANALYTICS_DB",
    schema="RAW"
)
cs = conn.cursor()

#-------------------------------------------------------
class SalesDataGenerator:
    def __init__(self):
        self.products = p
        self.regions = ["North America", "Europe", "Asia Pacific", "Africa","Australia","South America"]

    def generate_sale(self):
        product = random.choice(self.products)
        x = random.randint(1,3)
        return {
            "order_id": f"ORD-{random.randint(10000, 99999)}",
            "product_id": product[0],
            "product_name": product[1],
            "category": product[2],
            "quantity": x,
            "unit_price": product[3],
            "total_amount": product[3] * x,
            "region": random.choice(self.regions),
            "order_timestamp": datetime.datetime.utcnow().isoformat()
        }

    def run_simulation(self, duration_minutes=1):
        # Generate and upload sales data every 30-60 seconds
        end_time = datetime.datetime.utcnow() + datetime.timedelta(minutes=duration_minutes)

        while datetime.datetime.utcnow() < end_time:
            # Generate 1-5 sales records
            batch = [self.generate_sale() for _ in range(random.randint(50,100))]

            # Upload to Azure (implementation depends on your setup)
            timestamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            filename = f"sales_{timestamp}.json"
            with open(filename, "w") as f:
              json.dump(batch, f, indent=4)
            put_sql = f"PUT file://{filename} @INT_STAGE AUTO_COMPRESS=TRUE"
            cs.execute(put_sql)
            print(f"Generated {len(batch)} sales records in {filename}")
            print(batch)

            # # Wait 30-60 seconds before next batch
            time.sleep(random.randint(30,60))
#-----------------------------------------------------------------------------------------------------

def main():
    gen = SalesDataGenerator();
    x = gen.run_simulation(60)
    x
main()



