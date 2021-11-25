# Trabajo Final - Seminario Intensivo de Tópicos Avanzados

Alumnos: 
- Lucas Serrenho Pfefferkorn                                                                                                                                                     
- Patricio Garbino


## Introducción

En el presente trabajo creamos una base de datos recolectando información de todos los departamentos en venta en la Ciudad de Buenos Aires publicados en el sitio argenprop.com .

## Desarrollo

### Setup del ambiente

Se realizaron algunos cambios al ambiente base visto en clase los cuales detallamos a continuación:

Imagenes: Se modificaron dos imagenes de Docker para agregar algunas librerías de python requeridas por nuestros scripts en Airflow y Jupyter. Estas imagen fueron subidas a Docker Hub por lo que deberían ser descargadas automáticamente al correr el Docker compose.

Dockercompose: Eliminamos el servicio de Kafka ya que no lo utilizamos y modificamos las imagenes de Airflow y Jupyter.

Index: Modificamos el archivo Index.html para cambiar un poco la pagina de inicio.

### Web scraping

La recolección de datos la realizamos creando un Dag en Airflow el cual esta programado para correr todos los días lunes a las 7 am. El mismo consta de cuatro etapas. 

1. La etapa uno busca el número de páginas total de las que descargaremos información.
2. La etapa dos realiza la descarga de información de cada una de las páginas, la parsea y la guarda en dataframe de pandas.
3. La etapa tres crea la tabla viviendas en postgres en caso de que esta no exista.
4. La etapa cuatro guarda la información en la base de datos.

Para esto fue necesario conectar Airflow con nuesta base Postgres. Es necesario levantar esta conexión para que todo funcione correctamente.

Dado que utilizamos una API de pago en modo free para evitar ser bloqueados por el servidor de argenprop, es probable que al momento de querer correr el dag esta no funcione. Se puede crear una cuenta para recibir una nueva key en www.webscrapingapi.com y reemplazarla en el Dag, de esta forma funcionará sin problemas.

Existe un archivo en Jupyter llamado scraping el cual realiza el mismo trabajo que nuestro Dag, está seteado para realizar el scraping de sólo 8 páginas y utilizando la ip local para demostrar que funciona.

### Ejercicio de predicción de precios

Realizción de un modelo de regresión para predecir el precio de las viviendas el cual se encuentra en Jupyter. Consta de 5 etapas:

1- Recuperación de la informacion de la base de datos previamente guardada por el scraping. 

2- Inicio de rutina de limpieza del dataframe:
	a-Eliminación de columnas con un 50% o más de valores nulos.
	b-Eliminación de columnas con poca varianza.
	c-Eliminación de todas las rows cuyo target value (Precio) fuese nulo.
	d-Reemplazo de valores nulos existentes por las medias de cada una de las columnas.
	
3- Enriquecimiento del dataframe con el campo comuna que lo obtenemos como un join entre los barrios y un dataset público de la ciudad de buenos aires.

4- Validacíon de datos: se procede a verificar los valores minimos y máximos de cada columna eliminando outliers. 	
	Ejemplo: Precio del M2 mayor a 10.000 USD o casos que tienen menos de 10 M2 totales.
	
5- El campo años en algunos casos contenía la fecha de construcción del edificio la cual se corrigió para homogeneizar el campo. 

6- Preprocessing de los datos para transformarlo en un dataframe compatible con las librerías de ML de Spark. Utilizando One Hot encoder, string indexer y Vector assembler.

7- Se probaron distintos modelos de regresión (lineal, decision trees, random forest, XGBoost) analizando las métricas r2, rmse y mae.

8- Se eligió el modelo más prometedor: XGBoost y se realizó un gridsearch con cross validation para el tunning de algunos parametros: maxDepth, maxBins y maxIter)

### 

### Superset

Finalmente realizamos un dashborad con tres visualizaciones en Superset. Se conectó la base Postgres con toda la información de nuestra tabla viviendas, se realizaron consultas a esa base de datos por medio de SQL lab y finalmente utilizamos estas consultas para generar los gráficos.

Exportamos el dashboard y la imagen obtenida en la carpeta superset.







