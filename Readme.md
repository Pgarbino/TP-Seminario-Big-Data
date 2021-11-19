# Trabajo Final - Seminario Intensivo de Tópicos Avanzados

Alumnos: Lucas Serrenho Pfefferkorn
                Patricio Garbino


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

1- La etapa uno búsca el número de páginas total de las que descargaremos información.
2- La etapa dos realiza la descarga de información de cada una de las páginas, la parsea y la guarda en dataframe de pandas.
3- La etapa tres crea la tabla viviendas en postgres en caso de que esta no exista.
4- La etapa cuatro guarda la información en la base de datos.

Para esto fue necesario conectar Airflow con nuesta base Postgres.

Dado que utilizamos una API de pago para evitar ser bloqueados por el servidor de Argenprop, el Dag se encuentra apagado, pero se puede correr sin problemas.

Existe un archivo en Jupyter llamado scraping el cual realiza el mismo trabajo que nuestro Dag, está seteado para realizar el scraping de sólo 10 páginas, pero modificando el bucle for que posee un comentario, bajaría la totalidad de las publicaciones.

### Ejercicio de predicción de precios

Realizamos un modelo de regresión para predecir el precio de las viviendas el cual se encuentra en Jupyter.

### 

### Superset

Finalmente realizamos un dashborad con cuatro visualizaciones en Superset. Se conectó la base Postgres con toda la información de nuestra tabla viviendas, se realizaron consultas a esa base de datos por medio de SQL lab y finalmente utilizamos estas consultas para generar los gráficos.

Para el acceso a Superset:

Usuario: itba
Clave: Itba2021








