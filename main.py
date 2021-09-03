from pyspark import SparkContext


spark = SparkContext("local", "My Application")
raw_rdd = spark.textFile("data.csv")


def extract_vin_key_value(x):
    x = x.strip()
    incident_id, incident_type, vin_number, make, model, year, incident_year, description = x.split(",")

    print('%s\t%s\t%s\t%s' %(vin_number, incident_type, make, year))



vin_kv = raw_rdd.map(lambda x: extract_vin_key_value(x))
print(vin_kv.take(16))
