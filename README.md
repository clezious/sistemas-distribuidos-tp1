![](https://i.imgur.com/P0aqOMI.jpg)

# sistemas-distribuidos-tp1

TP1 Sistemas Distribuídos FIUBA 2024 1c

| Nombre           | Padrón | Email                |
| ---------------- | ------ | -------------------- |
| Manuel Reberendo | 100930 | mreberendo@fi.uba.ar |
| Manuel Sanchez   | 107951 | msanchezf@fi.uba.ar  |

## Scope

El objetivo de este sistema es procesar 5 queries sobre un dataset de libros y reviews. Las queries son las siguientes:

- Título, autores y editoriales de los libros de categoría "Computers" entre
  2000 y 2023 que contengan 'distributed' en su título.
- Autores con títulos publicados en al menos 10 décadas distintas
- Títulos y autores de libros publicados en los 90' con al menos 500 reseñas.
- 10 libros con mejor rating promedio entre aquellos publicados en los 90’
  con al menos 500 reseñas.
- Títulos en categoría "Fiction" cuyo sentimiento de reseña promedio esté en
  el percentil 90 más alto

El sistema debe soportar el incremento de los volumenes de computo, con tal de poder escalar el poder de procesamiento.

## Vista Fisica

### Servicios

- Book_filter
  - Toma `books` de una cola de entrada, los filtra leyendo el campo y valor (o rango)
    configurados, y los deposita en una cola de salida.
- Review_filter
  - Toma `books` de una cola de entrada, luego toma `reviews` de otra cola de entrada
    y las filtra dependiendo de si corresponden a un `book` recibido anteriormente.
    Por último deposita las `reviews` filtradas en una cola de salida.
- Router
  - Recibe elementos (`books` o `reviews`) de una cola de entrada,
    les aplica un hash sobre un campo configurable y los deposita en
    una cola de salida que depende del hash obtenido.
- Input boundary
  - Configurable para procesar batches de `books` o `reviews` de una cola de entrada,
    eliminar campos que no sean necesarios (configurable), y depositar cada item en una cola de salida.
- Books_by_author_decades_counter
  - Recibe `books` de una cola de entrada y almacena por cada autor las decadas en las que publicó un libro.
    Al llegar a 10 decadas distintas se envia a una cola de salida el nombre del autor.
- Book_review_sentiment_analyzer
  - Recibe `reviews` de una cola de entrada y calcula su `score` producto de un "análisis de sentimiento"
    realizado sobre el texto de la `review`. Deposita en una cola de salida el título del `book` al
    que pertenece la `review` y su score calculado.
- Book_review_sentiment_aggregator
  - Recibe de una cola de entrada titulos y scores de sentimiento y almacena la suma y cantidad de scores recibidos por cada titulo.
  - Recibe por otra cola de entrada un mensaje indicando que debe depositar en una
    cola de salida el percentil 90 más alto del promedio de score de los titulos almacenados
- Book_review_stats_service
  - Recibe `reviews` de una cola de entrada y almacena la cantidad y suma (de puntajes) de las reviews por cada `book`.
  - Recibe por otra cola de entrada un mensaje indicando que debe depositar en una cola de salida el top 10 (hasta el momento)
    de los `books` almacenados con mayor promedio de puntaje
- Book_reviews_mean_rating_aggregator
  - Recibe el top 10 de `books` con mejor promedio de reseñas de cada instancia de `Book_review_stats_service` en una cola de entrada.
    - Al recibir una cantidad de mensajes igual a la cantidad de instancias de ese servicio, calcula el top 10 global y lo deposita en una cola de salida.

### Diagrama de Robustez

![Diagrama de robustez general](./images/diagrama_robustez_general.png)

[Link al diagrama](https://viewer.diagrams.net/?page-id=pvgHEc-C2KMQIe-LH_zm&highlight=0000ff&edit=_blank&layers=1&nav=1#G1wfcmCg63otTVOHnEUja5Xv4oczVIh9BT)

En este diagrama se pueden ver los distintos servicios mencionados en la seccion anterior, pero en forma mas concreta. Un detalle importante a notar son la existencia de servicios de routeo, los cuales hashean una clave de los mensajes recibidos para poder distribuirlos en distintas colas de salida. Esto permite que los servicios puedan escalar horizontalmente, ya que se pueden agregar mas instancias de un servicio sin que esto afecte el funcionamiento del sistema.

Imponemos una precondicion de que el cliente debe enviar primero todos los libros, de forma que el sistema los procese y guarde en sus filtros. Luego de esto, el cliente puede enviar las reviews.

### Diagrama de Despliegue

![Diagrama de despliegue](./images/diagrama_despliegue.png)

[Link al diagrama](https://viewer.diagrams.net/?page-id=8r_P_Zw1fpBbOP0c5zG2&highlight=0000ff&edit=_blank&layers=1&nav=1&page-id=8r_P_Zw1fpBbOP0c5zG2#G1wfcmCg63otTVOHnEUja5Xv4oczVIh9BT)

La topologia de despliegue del sistema es en forma de estrella. El nodo que aloja el servicio de RabbitMQ es quien se encuentra en el centro. Los demas nodos se comunican a traves de colas de mensajes provistas por dicho servicio. De esta forma, nunca se encuentran comunicados directamente entre si, sino que siempre lo hacen a traves de RabbitMQ.

## Vista de Procesos

### Diagrama de Actividades

![Diagrama de actividades](./images/diagrama_actividades.png)

[Link al diagrama](https://viewer.diagrams.net/?page-id=YzJNP-XYQnWjph2-Tj47&highlight=0000ff&edit=_blank&layers=1&nav=1&page-id=YzJNP-XYQnWjph2-Tj47#G1wfcmCg63otTVOHnEUja5Xv4oczVIh9BT)

En este diagrama se pueden ver las distintas actividades que realiza el sistema para procesar las queries. Las distintas queries son independientes entre si, por lo que el procesamiento puede ser realizado en paralelo y puede ser visto como 5 actividades totalmente distintas.


### Diagrama de Secuencia

![Diagrama de secuencia](./images/diagrama_secuencia.png)
[Link al diagrama](https://viewer.diagrams.net/?page-id=78cztUaxTj71S7lKw_Nt&highlight=0000ff&edit=_blank&layers=1&nav=1&page-id=78cztUaxTj71S7lKw_Nt#G1wfcmCg63otTVOHnEUja5Xv4oczVIh9BT)

En este diagrama de secuencia se puede visualizar el flujo de mensajes entre los distintos servicios con el fin de procesar, en este caso, la query 4. Notese un detalle importante, que es que el servicio de reviews filter debe esperar a que todos los libros hayan sido procesados para poder empezar a filtrar las reviews. Esto se debe a que, para saber que reviews corresponden a que libros, primero se llegan los libros, se guardan en el filtro, y luego las reviews son filtradas.

Otro aspecto intersante es que el servicio de Reviews_mean_rating_aggregator debe esperar a que todos los servicios de Review_stats_service hayan terminado y enviado su top 10 local, antes de proceder a calcular el top 10 global.

## Vista de Desarrollo

### Diagrama de Paquetes
![Diagrama de paquetes](./images/diagrama_paquetes.png)

[Link al diagrama](https://viewer.diagrams.net/?page-id=hVfBI8x4F1AI7FTGm5cZ&highlight=0000ff&edit=_blank&layers=1&nav=1&page-id=hVfBI8x4F1AI7FTGm5cZ#G1wfcmCg63otTVOHnEUja5Xv4oczVIh9BT)


En este diagrama se refleja la estructura de paquetes del sistema. El input boundary es el servicio que se encarga de recibir los mensajes del cliente y enviarlos a las colas correspondientes. Dentro del mismo y al igual que en todos los servicios, se hace uso del middleware, una capa de abstraccion para trabajar sobre RabbitMQ con mayor facilidad. 

Asimismo, dicho servicio hace uso de Book, EOFPacket y Review, subclases de Packet, que representan los distintos tipos de mensajes que se pueden recibir. Por otra parte, esta el PacketDispatcher, que se encarga de parsear los mensajes recibidos y convertilos en objetos de las clases mencionadas anteriormente.

Ademas, un boundary puede ser de tipo BookBoundary o ReviewBoundary, segun del tipo de mensaje que recibe. De esta manera, al recibir un mensaje, el boundary lo parsea y lo envia a la cola correspondiente, con el formato correspondiente.

Finalmente, el input boundary utiliza funciones auxiliares de recepcion de mensajes por sockets, con el fin de evitar problemas de short-read. Dichas funciones se encuentran en un modulo llamado receive_utils


## DAG
![DAG](./images/dag.png)

[Link al diagrama](https://viewer.diagrams.net/?page-id=9488BZJgpK-lBa-DFY4Z&highlight=0000ff&edit=_blank&layers=1&nav=1&page-id=9488BZJgpK-lBa-DFY4Z#G1wfcmCg63otTVOHnEUja5Xv4oczVIh9BT)

En el DAG se pueden observar aquellos datos que son necesarios para procesar cada una de las queries. Las queries se pueden interpretar como los distintos caminos desde el input boundary hasta el output boundary. Algunas queries, como la 3 y la 4 comparten parte de su camino, hasta bifurcarse. De esta forma, se optimiza el flujo de informacion, y no se repiten calculos innecesarios.