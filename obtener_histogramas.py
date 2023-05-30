from datetime import datetime
import matplotlib.pyplot as plt

'''

    TIPO DE TEXTO:

    Weekdays 1:
    ('2017-12-01T06', 1) Users: 1
    ('2017-12-01T07', 1) Users: 21

'''

data_list_weekdays = []
data_list_weekends = []

histogramas = []
with open("sample_spark.txt", 'r') as file:
    histograma = []
    for line in file:
        line = line.strip() # suprimir \n
        if line.startswith('Week'):
            if histograma != []:
                histogramas.append(histograma)
            histograma = [line,[]]
        #if not (line.startswith('Week')): # pasar los titulos
        else:    
            #encuentro los indices de los str para extraer los datos que quiero

            time_start_index = line.find('T') + 1 
            time_end_index = line.find("'", time_start_index)
        
            time = line[time_start_index:time_end_index]

            user_type_start_index = line.find(", ") + 1
            user_type_end_index = line.find(")", user_type_start_index)

            user_type = line[user_type_start_index:user_type_end_index]

            users_start_index = line.find('U') + 6

            users = line[users_start_index:]
            histograma[1].append([int(time), int(users)])
            #separar entresemana y fin de semana
            #if datetime.strptime(line[2:12], "%Y-%m-%d").weekday() < 5:
            #hago una lista con los datos
            #    data_list_weekdays.append([int(time), int(users)])
            #else:
            #    data_list_weekends.append([int(time), int(users)])
        

#print(data_list_weekdays)
#print(data_list_weekends)

for histograma in histogramas:
        #Hago lista con sublistas[tiempo, usuarios] para las 24 horas
        suma_week = 24*[0]

        for sublist in histograma[1]:
            index = sublist[0]  
            value = sublist[1]  
            suma_week[index] += value  #sumo los usuarios en las mismas horas

        suma_list_week = [[i,suma_week[i]] for i in range(24)]
        #print(suma_weekdays)
        #print(f"SUMA {suma_list_weekdays}")

        x = [sublist[0] for sublist in suma_list_week] #TIEMPO  X - AXIS
        y = [sublist[1] for sublist in suma_list_week] #USUARIOS Y - AXIS

        if 'day' in histograma[0]:
            plt.bar(x, y, color = "green")
            plt.xlabel('Time')
            plt.ylabel('Usuarios')
            plt.title(f'Histograma {histograma[0]}')
            plt.savefig(f'Histograma {histograma[0]}')
            plt.show()
        else:
            plt.bar(x, y, color = "purple")
            plt.xlabel('Time')
            plt.ylabel('Usuarios')
            plt.title(f'Histograma {histograma[0]}')
            plt.savefig(f'Histograma {histograma[0]}')
            plt.show()





