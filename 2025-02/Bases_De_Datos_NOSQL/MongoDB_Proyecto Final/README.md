**Universo DB**

El proyecto incluyo:

1) Generacion de un cluster MongoDB con redundancia (3 bloques de redundancia, cada uno con 3 servidores Mondo para Sharding, 3 Routers redundantes, 3 config server y un Nginx)

2) Creacion de datos sinteticos para poblar la base de datos (Agencias espaciales, astronautas, galaxias, estrelas, planetas, satelites, cometas, etc)

3) Interconexion de todo eso y la creacion de un pipe que lea los camboios en mongo y usanda kafka los refleje en Zookeeper para hacer graficos y consultas en ellos.

Los archivos generados automaticamente (para poblar la BD) y todo el codigo usado esta en el repositorio
