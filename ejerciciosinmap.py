from pyspark import SparkContext
import json
from datetime import datetime
import sys


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

        # leer JSON archivo como RDD^M
        lines_rdd = sc.textFile(filename)

        # extrae los datos ageRange, unplug_hour_time y filtra ageRange = 0^M
        data_rdd = lines_rdd.map(extraer).filter(lambda x: x[0] != 0) #narrow transformation^M

        # Ordena según unplug_hourTime^M
        #sorted_rdd = data_rdd.sortBy(lambda x: datetime.strptime(x[1], "%Y-%m-%dT%H:%M:%S.%f%z")) #wide transformation^M

        # IDEA: Distinguir casos, entresemana y fin de semana^M
        # narrow transformation^M
        # Los 10 primeros str representan la fecha, lo que quiero extraer^M
        weekdays_rdd = data_rdd.filter(lambda x: get_weekday(x[1][:10]) < 5)
        weekends_rdd = data_rdd.filter(lambda x: get_weekday(x[1][:10]) >= 5)

        # Ordena según unplug_hourTime^M
        # wide transformation^M
        sorted_weekdays_rdd = weekdays_rdd.sortBy(lambda x: datetime.strptime(x[1], "%Y-%m-%dT%H:%M:%S.%f%z").time())
        sorted_weekends_rdd = weekends_rdd.sortBy(lambda x: datetime.strptime(x[1], "%Y-%m-%dT%H:%M:%S.%f%z").time())

        age_ranges = range(1, 7)
        with open ("results.txt", "w") as output_file:
            
            for age_range in age_ranges:
                output_file.write(f"Weekdays {age_range}:\n")
                # Filter data for the current age range^M
                age_range_weekdays_rdd = sorted_weekdays_rdd.filter(lambda x: x[0] == age_range).\
                                        map(lambda x: (x[1][:13], 1)).\
                                        countByKey()

                age_range_weekends_rdd = sorted_weekends_rdd.filter(lambda x: x[0] == age_range).\
                                        map(lambda x: (x[1][:13], 1)).\
                                        countByKey()
                
                for time_interval, user_count in age_range_weekdays_rdd.items():
                    output_file.write(f" {time_interval} Users: {user_count}\n")

                output_file.write(f"Weekends {age_range}:\n")
                for time_interval, user_count in age_range_weekends_rdd.items():
                    output_file.write(f" {time_interval} - Users: {user_count}\n")

if __name__=="__main__":
    file = ""
    if len(sys.argv)>1:
        file = sys.argv[1]
    main(file)



