from pyspark import SparkContext
import json
from datetime import datetime


def extraer(line):
    data = json.loads(line)
    age_range = data.get('ageRange')
    unplug_hour_time = data.get('unplug_hourTime', {}).get('$date')
    return [age_range, unplug_hour_time]

# IDEA: Distinguir casos, entresemana y fin de semana
# weekday() method to get the day of the week as a number, where Monday is 0 and Sunday is 6.

def get_weekday(date_string):
    weekday = datetime.strptime(date_string, "%Y-%m-%d").weekday()
    return weekday

def main(filename):
    with SparkContext() as sc:

        # leer JSON archivo como RDD
        lines_rdd = sc.textFile(filename)

        # extrae los datos ageRange, unplug_hour_time y filtra ageRange = 0
        data_rdd = lines_rdd.map(extraer).filter(lambda x: x[0] != 0) #narrow transformation

        # Ordena según unplug_hourTime
        #sorted_rdd = data_rdd.sortBy(lambda x: datetime.strptime(x[1], "%Y-%m-%dT%H:%M:%S.%f%z")) #wide transformation

        # IDEA: Distinguir casos, entresemana y fin de semana
        # narrow transformation
        # Los 10 primeros str representan la fecha, lo que quiero extraer
        weekdays_rdd = data_rdd.filter(lambda x: get_weekday(x[1][:10]) < 5)
        weekends_rdd = data_rdd.filter(lambda x: get_weekday(x[1][:10]) >= 5)

        # Ordena según unplug_hourTime
        # wide transformation
        sorted_weekdays_rdd = weekdays_rdd.sortBy(lambda x: datetime.strptime(x[1], "%Y-%m-%dT%H:%M:%S.%f%z").time())
        sorted_weekends_rdd = weekends_rdd.sortBy(lambda x: datetime.strptime(x[1], "%Y-%m-%dT%H:%M:%S.%f%z").time())


'''
#Añadir al main

# wide transformation collect()
# action operation that is used to retrieve all the elements of the dataset 
# (from all nodes) to the driver node.

# Guardar los resultados en un archivo txt

with open('results.txt', 'w') as output_file:

    output_file.write("Weekdays: \n")

    for data in sorted_weekdays_rdd.collect():
        age_range = data[0]
        unplug_hour_time = data[1]
        print("Age Range:", age_range)
        print("Unplug Hour Time:", unplug_hour_time)
        print("--------------------")

    output_file.write("Weekends:\n")

    for data in sorted_weekends_rdd.collect():
        age_range = data[0]
        unplug_hour_time = data[1]
        print("Age Range:", age_range)
        print("Unplug Hour Time:", unplug_hour_time)
        print("--------------------")
'''

