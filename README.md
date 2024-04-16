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
- batch_processor
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

Imponemos una precondicion de que el cliente debe enviar primero todos los libros, de forma que el sistema los procese y guarde en sus filtros. Luego de esto, el cliente puede enviar las reviews.

### Diagrama de Despliegue

![Diagrama de despliegue](./images/diagrama_despliegue.png)

[Link al diagrama](https://viewer.diagrams.net/?page-id=8r_P_Zw1fpBbOP0c5zG2&highlight=0000ff&edit=_blank&layers=1&nav=1&page-id=8r_P_Zw1fpBbOP0c5zG2#G1wfcmCg63otTVOHnEUja5Xv4oczVIh9BT)

## Vista de Procesos

### Diagrama de Actividades

![Diagrama de actividades](./images/diagrama_actividades.png)

[Link al diagrama](https://viewer.diagrams.net/?page-id=YzJNP-XYQnWjph2-Tj47&highlight=0000ff&edit=_blank&layers=1&nav=1&page-id=YzJNP-XYQnWjph2-Tj47#G1wfcmCg63otTVOHnEUja5Xv4oczVIh9BT)
