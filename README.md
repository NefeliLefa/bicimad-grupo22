# bicimad-grupo22
PRPA: Práctica 4, Spark

El archivo que presenta el problema, cómo se ha resuelto, los resultados y las conclusiones de manera extensa es 'Resultados_práctica_paralela.pdf'. Aquí se muestra un breve resumen.

## Definición del problema a resolver
Hemos utilizado los datos de Bicimad para encontrar cuántos viajes se realizan por intervalos de tiempo entresemana y los fines de semana por cada rango de edad. Pensamos que sería interesante conocer esta información para intentar ver reflejados algunos posibles usos de las bicicletas: si las personas jóvenes lo usan los fines de semana de madrugada es posiblemente para ir o volver de fiesta (esto puede verse en el ejemplo puesto abajo, los findes las personas de 0-16 años usan más las bicicletas por la noche en comparación con los días de diario), si los adultos las usan entresemana a primera hora de la mañana para ir al trabajo, etc.

## Diseño e implementación en Spark
Dado el tamaño de los datos hemos utilizado Spark en python, pyspark. 

unplug_hourTime: Franja horaria en la que se realiza el desenganche de la bicicleta. 

ageRange: Número que indica el rango de edad del usuario que ha realizado el movimiento. Sus posibles valores son:
- 0: No se ha podido determinar el rango de edad del usuario
- 1: El usuario tiene entre 0 y 16 años
- 2: El usuario tiene entre 17 y 18 años
- 3: El usuario tiene entre 19 y 26 años
- 4: El usuario tiene entre 27 y 40 años
- 5: El usuario tiene entre 41 y 65 años
- 6: El usuario tiene 66 años o más 

Hemos eliminado los datos con ageRange = 0.
## Ejemplo
![Histograma](histo_weekdays_1.png)
![Histograma](histo_weekends_1.png)


## Inclusiones
```python
from pyspark import SparkContext
import matplotlib.pyplot as plt
import json
from datetime import datetime
```

