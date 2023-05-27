from pyspark import SparkContext
import matplotlib.pyplot as plt
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

        
        age_ranges = range(1, 7)
        # Creamos dos plots para cada franja de edad
        for age_range in age_ranges:
            
            # Filtra age_Range == 1
            # Extrae la componente de la hora
            # countByValue() :Return the count of each unique value in this RDD as a dictionary of (value, count) pairs
            age_range_1_weekdays = sorted_weekdays_rdd.filter(lambda x: x[0] == age_range).\
                                map(lambda x: datetime.strptime(x[1], "%Y-%m-%dT%H:%M:%S.%f%z").hour).\
                                countByValue()

            age_range_1_weekends = sorted_weekends_rdd.filter(lambda x: x[0] == age_range).\
                                map(lambda x: datetime.strptime(x[1], "%Y-%m-%dT%H:%M:%S.%f%z").hour).\
                                countByValue()


            # Para cada intervalo, extraemos "count" usando .get(), que devuelva el valor para cada "key"
            # Si no existe este "key", devuelve 0

            result_dict_weekdays = {}
            for hour in range(0, 24, 2):
                result_dict_weekdays[hour] = age_range_1_weekdays.get(hour, 0)

            fig, ax = plt.subplots()
            ax.bar(result_dict_weekdays.keys(),result_dict_weekdays.values())
            ax.set_xlabel("Time Intervals")
            ax.set_ylabel("Users")
            ax.set_title(f"Histogram: Weekdays & age_Range {age_range}")
            fig.savefig(f"weekdays_{age_range}.png")
            fig.show() 

            result_dict_weekends = {}
            for hour in range(0, 24, 2):
                result_dict_weekends[hour] = age_range_1_weekends.get(hour, 0)
            
            fig, ax = plt.subplots()
            ax.bar(result_dict_weekends.keys(),result_dict_weekends.values(), color= 'orange')
            ax.set_xlabel("Time Intervals")
            ax.set_ylabel("Users")
            ax.set_title(f"Histogram: Weekends & age_Range {age_range}")
            fig.savefig(f"weekends_{age_range}.png") 
            fig.show()



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

