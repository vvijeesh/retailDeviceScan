# retailDeviceScan
Self Scan details from a retail store user gathered for analysis

Self-scan device is a handheld device which the customer can pick up while entering the retail store and it enables customers to scan the items and do a faster checkout without having to wait in the queue.

## Flow

Customer scans products at the retail store, which is captured in the self scan device and transmitted to a REST api endpoint (built using springboot) to send the JSON records in a serialised manner to Kafka.

Records produced to Kafka will be consumed in a Spark Streaming Kafka Consumer and processed to store the data as hive tables (via dataframe) for further data science analysis. 