import schedule
import time
import subprocess

def run_data_lake_consumer():
    subprocess.Popen(['python', 'consumer_data-lake.py'])

def run_data_warehouse_consumer():
    subprocess.Popen(['python', 'consumer_data-warehouse.py'])

schedule.every(10).minutes.do(run_data_lake_consumer)
schedule.every(10).minutes.do(run_data_warehouse_consumer)

while True:
    schedule.run_pending()
    time.sleep(1)
