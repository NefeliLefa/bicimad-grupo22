import json
from datetime import datetime

#tiene como argumento una linea del archivo json, y extrae los datos ageRange, unplug_hour_time en una lista
def extraer(line):
    data = json.loads(line)
    age_range = data.get('ageRange')
    unplug_hour_time = data.get('unplug_hourTime', {}).get('$date')
    return [age_range, unplug_hour_time]

# cree una lista de listas para cada linea del archivo json
data_list = []
with open('sample_10e4.json', 'r') as file:
    for line in file:
        extracted_data = extraer(line)
        data_list.append(extracted_data)

# filtra listas con ageRange = 0
filtered_list = list(filter(lambda x: x[0] != 0, data_list))

# Ordena según unplug_hourTime
#sorted_list = sorted(filtered_list, key=lambda x: datetime.strptime(x[1], "%Y-%m-%dT%H:%M:%S.%f%z"))

# IDEA: Distinguir casos, entresemana y fin de semana

weekdays_list = []
weekends_list = []

#para cada sub-lista en la lista filtrada
for data in filtered_list:
    date_string = data[1][:10]  # Los 10 primeros str representan la fecha, lo que quiero extraer
    weekday = datetime.strptime(date_string, "%Y-%m-%d").weekday() #weekday() method to get the day of the week as a number, where Monday is 0 and Sunday is 6.
    
    if weekday < 5:  # Entresemana, lunes a viernes
        weekdays_list.append(data)
    else:  # fin de semana, sabado o domingo
        weekends_list.append(data)

# Ordena según unplug_hourTime
sorted_weekdays = sorted(weekdays_list, key=lambda x: datetime.strptime(x[1], "%Y-%m-%dT%H:%M:%S.%f%z").time())
sorted_weekends = sorted(weekends_list, key=lambda x: datetime.strptime(x[1], "%Y-%m-%dT%H:%M:%S.%f%z").time())
# Creamos dos plots para cada franja de edad

age_ranges = range(1,7)

for age_range in age_ranges:
    filtered_age_weekdays = list(filter(lambda x: x[0] == age_range, sorted_weekdays))
    filtered_age_weekends = list(filter(lambda x: x[0] == age_range, sorted_weekends))

    # Extraer horas
    hour_data_weekdays = [datetime.strptime(data[1], "%Y-%m-%dT%H:%M:%S.%f%z").hour for data in filtered_age_weekdays]
    hour_data_weekends = [datetime.strptime(data[1], "%Y-%m-%dT%H:%M:%S.%f%z").hour for data in filtered_age_weekends]

    # Histograma age_Range, weekdays

    plt.hist(hour_data_weekdays,bins=24, range=(0, 24), color='blue')
    plt.xlabel('Time Intervals')
    plt.ylabel('Users')
    plt.title(f'Histogram: Age Range & Weekdays {age_range} ')
    plt.savefig(f'histo_weekdays_{age_range}.png')
    plt.show()

    # Histigrama age_Range, weekends

    plt.hist(hour_data_weekends,bins=24, range=(0, 24), color='green')
    plt.xlabel('Time Intervals')
    plt.ylabel('Users')
    plt.title(f'Histogram: Age Range{age_range} & Weekends')
    plt.savefig(f"histo_weekends_{age_range}.png")
    plt.show()
'''
# Guardar los resultados en un archivo txt
with open('results1.txt', 'w') as output_file:
    output_file.write("Weekdays:\n")
    for data in sorted_weekdays:
        age_range = data[0]
        unplug_hour_time = data[1]
        output_file.write("Age Range: {}\n".format(age_range))
        output_file.write("Unplug Hour Time: {}\n".format(unplug_hour_time))
        output_file.write("--------------------\n")

    output_file.write("Weekends:\n")
    for data in sorted_weekends:
        age_range = data[0]
        unplug_hour_time = data[1]
        output_file.write("Age Range: {}\n".format(age_range))
        output_file.write("Unplug Hour Time: {}\n".format(unplug_hour_time))
        output_file.write("--------------------\n")
'''
'''
#Print para comprobar  

print("Weekdays:")
for data in sorted_weekdays:
    age_range = data[0]
    unplug_hour_time = data[1]
    print("Age Range:", age_range)
    print("Unplug Hour Time:", unplug_hour_time)
    print("--------------------")


print("Weekends:")
for data in sorted_weekends:
    age_range = data[0]
    unplug_hour_time = data[1]
    print("Age Range:", age_range)
    print("Unplug Hour Time:", unplug_hour_time)
    print("--------------------")
'''
