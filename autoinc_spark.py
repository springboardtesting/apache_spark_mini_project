from pyspark import SparkContext


spark = SparkContext("local", "My Application")
raw_rdd = spark.textFile("data.csv")


def extract_vin_key_value(x):
    x = x.strip()
    incident_id, incident_type, vin_number, make, model, year, incident_year, description = x.split(",")

    return (vin_number,(make, year, incident_type))


make = None
year = None


def populate_make(kv):
    global make, year
    kv = list(kv)
    output = list()
    
    for i in kv:
        incident_type = i[2]

        if incident_type == 'I':
            make = i[0]
            year = i[1]
            output.append((incident_type, make, year))

        if incident_type != 'I':
            output.append((incident_type, make, year))

    return output


def extract_make_key_value(x):
    return (f"{x[1]}-{x[2]}", 1)


vin_kv = raw_rdd.map(lambda x: extract_vin_key_value(x))
enhance_make = vin_kv.groupByKey().flatMap(lambda kv: populate_make(kv[1]))
make_kv = enhance_make.map(lambda x: extract_make_key_value(x))
total_count = make_kv.reduceByKey(lambda a, b: a+b)
for i in total_count.collect():
    print(i)