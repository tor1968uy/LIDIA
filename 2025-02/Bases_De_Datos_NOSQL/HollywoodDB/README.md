** Proyecto para la facultad basado en Cine

El proyecto carga en una BD MongoDB peliculas, directores, actores, premios, etc y los datos serian visualizados usando Kafka y Elasticsearch 
En el proyecto esta tanto el codigo de los cdc (para leer los cambios en MongoDB y pasarlos a Kafka) y tambien los de los cosumidores para pasar la informacion desde Kafka a Elasticsearch

