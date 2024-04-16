# sistemas-distribuidos-tp1
TP1 Sistemas Distribuídos FIUBA 2024 1c


## Servicios
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

