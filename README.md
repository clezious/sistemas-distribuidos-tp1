![](https://i.imgur.com/P0aqOMI.jpg)

TP 1 y 2 - Sistemas Distribuídos FIUBA 2024 1c

| Nombre           | Padrón | Email                |
| ---------------- | ------ | -------------------- |
| Manuel Reberendo | 100930 | mreberendo@fi.uba.ar |
| Manuel Sanchez   | 107951 | msanchezf@fi.uba.ar  |

# Parte 1

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

El sistema debe soportar el incremento de los volumenes de computo, con tal de poder escalar el poder de procesamiento. Ademas, debe ser tolerante a fallas, de forma que si un servicio falla, el sistema pueda seguir funcionando sin problemas.

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
- Client
  - Lee csv con books y reviews y los envia al Boundary. Al finalizar, recibe del boundary los resultados y los almacena en archivos csv (1 por cada query)
- Input_gateway
  - Recibe tanto `books` como `reviews` de un cliente en formato csv, los parsea, eliminar campos que no sean necesarios, y deposita cada item en exchange de salida (separado por tipo de item).
  - Soporta hasta N clientes conectados en simultaneo, haciendo uso de un ThreadPool.
- Output_gateway
  - Espera los resultados de las queries y los envia al cliente.
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
- Review_mean_rating_aggregator
  - Recibe el top 10 de `books` con mejor promedio de reseñas de cada instancia de `Book_review_stats_service` en una cola de entrada.
    - Al recibir una cantidad de mensajes igual a la cantidad de instancias de ese servicio, calcula el top 10 global y lo deposita en una cola de salida.
- Docktor
  - Monitorea el estado de los servicios y los reinicia en caso de que fallen.
  - Puede haber multiples instancias de Docktor, y cada una se encarga de monitorear una parte de los servicios. Ademas, los docktors forman un anillo, de forma que si uno falla, otro pueda reiniciarlo.
  - El sistema puede volverse a levantar siempre y cuando haya al menos un Docktor activo.

### Diagrama de Robustez

![Diagrama de robustez general](./images/diagrama_robustez_general.png)

[Link al diagrama](https://viewer.diagrams.net/?page-id=pvgHEc-C2KMQIe-LH_zm&highlight=0000ff&edit=_blank&layers=1&nav=1#G1wfcmCg63otTVOHnEUja5Xv4oczVIh9BT)

En este diagrama se pueden ver los distintos servicios mencionados en la seccion anterior, pero en forma mas concreta. Un detalle importante a notar son la existencia de servicios de routeo, los cuales hashean una clave de los mensajes recibidos para poder distribuirlos en distintas colas de salida. Esto permite que los servicios puedan escalar horizontalmente, ya que se pueden agregar mas instancias de un servicio sin que esto afecte el funcionamiento del sistema.

Imponemos una precondicion de que el cliente debe enviar primero todos los libros, de forma que el sistema los procese y guarde en sus filtros. Luego de esto, el cliente puede enviar las reviews.

Asimismo, decidimos no incluir en el diagrama a los Docktors, ya que estos no son parte del flujo de procesamiento de las queries, sino que son servicios que se encargan de monitorear el estado de los demas servicios y reiniciarlos en caso de que fallen. Ademas, haria que el diagrama sea mas complejo y dificil de entender ya que los docktors se comunican con todos los servicios.

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

![DAG](./images/DAG.png)

[Link al diagrama](https://viewer.diagrams.net/?page-id=9488BZJgpK-lBa-DFY4Z&highlight=0000ff&edit=_blank&layers=1&nav=1&page-id=9488BZJgpK-lBa-DFY4Z#G1wfcmCg63otTVOHnEUja5Xv4oczVIh9BT)

En el DAG se pueden observar aquellos datos que son necesarios para procesar cada una de las queries. Las queries se pueden interpretar como los distintos caminos desde el input boundary hasta el output boundary. Algunas queries, como la 3 y la 4 comparten parte de su camino, hasta bifurcarse. De esta forma, se optimiza el flujo de informacion, y no se repiten calculos innecesarios.

# Parte 2:

## Multiples Clientes

Para soportar que múltiples clientes se conecten en simultaneo al sistema (Sin esperar a que cada uno termine de enviar sus datos y recibir sus resultados para que se conecte el próximo) se realizaron las siguientes modificaciones:

### Client id y Packet id

Se utiliza un `ThreadPool` en el servicio de `input_gateway` para manejar la conexión de múltiples clientes en simultaneo. Cada cliente se conecta a un thread del `ThreadPool` que se encarga de recibir los datos del cliente y enviarlos a un thread de conexion con RabbitMQ. De esta forma, se puede manejar la conexión de múltiples clientes en simultaneo sin que uno tenga que esperar a que el otro termine de enviar sus datos y recibir sus resultados.

El servicio de `input_gateway` ahora asigna un `client_id` a cada cliente al momento de conectarse. Este `client_id` es un `id` único e incremental, no hay 2 clientes con el mismo `client_id` en el sistema. El `client_id` se comunica al cliente para que lo pueda usar para obtener los resultados del `output_gateway`. Este `client_id` se agrega a todos los paquetes que se generen a partir de datos enviados por ese cliente, junto a un `packet_id` que es un `id` único e incremental para cada paquete enviado por ese cliente. De esta forma, se puede identificar a que cliente pertenece cada paquete e identificar si un paquete es duplicado o no.

En un principio, habiamos planteado tener un `uuid`. No obstante, terminamos optando por un `id` incremental ya que nos asegura que no haya colisiones y es mas facil de realizar un seguimiento de los paquetes.

El `client_id` es persistido en disco por el `input_gateway` al igual que el estado actual de los clientes (si estan enviando libros o reviews). Esto permite poder recuperar el estado e invalidar las queries interrumpidas por el fallo. De esta forma, se puede garantizar que el `client_id` sea único y no se repita incluso frente a fallas del sistema.

En cuanto al estado de cada servicio, tuvimos que modificar todos los nodos que guardaban informacion para que soporte mulitples cliente. Todos los servicios con estado ahora guardan el estado de cada cliente por separado, usando el `client_id` para diferenciarlos. Esta independencia de estados permite que cada cliente pueda enviar sus datos y recibir sus resultados sin interferir con los otros clientes. Ademas, facilita la limpieza de los estados de los clientes una vez que estos terminan de enviar sus datos y recibir sus resultados, o en caso de error.

Al conectarse con el servicio del `output_gateway`, el cliente debe identificarse enviando su `client_id` para poder recibir los resultados correspondientes. El `output_gateway` se encarga de enviar los resultados correspondientes a cada cliente, identificando a que cliente pertenece cada resultado gracias al `client_id` que se encuentra en cada paquete. Los resultados se envian en forma de _streaming_, es decir, a medida que se van generando, se envian al cliente.

### Reencolado

Por otra parte, en el `review_filter` se implementó el uso de Threads para poder soportar la nueva simultaneidad de clientes. Ahora hay un thread recibiendo libros y otro recibiendo reviews en simultaneo. Esto trae consigo un problema, ya que los reviews pueden llegar antes que terminen de llegar los libros. Para solucionar esto, se implementó un mecanismo de reencolado de reviews. Dicho mecanismo funciona de la siguiente forma:

- Si llega una review y se encontró el libro al que pertenece, se filtra normalmente, por mas que no se haya terminado de recibir todos los libros.
- Si llega una review y no se encontró el libro al que pertenece pero se recibió un EOF, se descarta la review.
- Si llega una review y no se encontró el libro al que pertenece, se reencola la review y se espera a que llegue el libro o un EOF en su defecto.

Sin embargo, esto conlleva un problema, ya que podriamos estar reencolando una review atras de un EOF de reviews. Esto resultaria en que se procese el EOF primero, lo cual limpia el estado para el cliente que le pertenece, produciendo que la review que se reencolo pueda llegar a ser descartada de forma erronea. Es por eso que si se reencola una review, se guarda tanto en disco como en memoria que se debe reencolar el EOF tambien.

Decidimos implementar este mecanismo de reencolado ya que partimos de la suposicion de que la cantidad de libros es menor a la cantidad de reviews, y que ademas los clientes primero envian los libros y luego las reviews. Esto lleva a que sea probable que para el momento en el que se empiecen a recibir las reviews en el `review_filter`, ya haya una gran cantidad de libros guardados en el filtro. Por lo tanto, es probable que se encuentre el libro al que pertenece la review y no sea necesario reencolarla.

### Modificacion del protocolo Cliente - Gateways

Se modificó el protocolo de comunicación entre el cliente y los gateways. Previamente, se tenia que solo un cliente se conectaba a la vez, enviaba todos los datos y recibia los resultados. Ahora, multiples clientes pueden conectarse a la vez por lo que fue necesario modificar el protocolo de comunicación para que soporte esta nueva funcionalidad.

El cambio principal es que el `input_gateway`, al recibir una conexion de un cliente, le envia un `client_id`. El cliente guarda dicho `client_id` y lo envia al conectarse con el `output_gateway` para poder recibir los resultados correspondientes.

![Diagrama de secuencia nuevo protocolo](./images/protocolo-cliente-gateway.png)

(Los resultados son enviados en forma de _streaming_)

## Tolerancia a fallas

Para soportar la tolerancia a fallas se implementaron las siguientes modificaciones:

### Uso de ACKs en rabbitMQ

- Se implementó el uso de ACKs en RabbitMQ para garantizar que los mensajes no se pierdan en caso de que un servicio falle mientras los está procesando.
- Rabbit garantiza que los mensajes se volverán a enviar si no se recibe un ACK en un tiempo determinado, y permite además controlar la cantidad de mensajes máxima que puede tener para cada cola esperando sus ACKs.

### Persistencia y recuperación de estado

- Se implementó la persistencia de estado en los servicios que lo requieren, guardando el estado en archivos que se leerán al iniciar el servicio.
  - Se intenta siempre mantener el estado también en memoria para agilizar el procesamiento, de modo que la lectura del archivo solo se haga al iniciar el servicio y tener que recuperar el estado en memoria.
- Para esto, se agregó la clase `PersistenceManager` que sirve como una interfaz de almacenamiento clave - valor, que pueden usar los servicios para guardar y recuperar su estado.
  - Los métodos soportados son:
    - `get`, que recupera el valor asociado a una clave, leyendo del archivo correspondiente.
    - `put`, que internamente guarda en un archivo el valor asociado a una clave, sobreescribiendo el valor anterior si ya existía.
    - `append`, que internamente agrega el valor asociado a una clave en una nueva linea al final del archivo correspondiente de existir, o como primer valor si no existe.
    - `get_keys`, que dado un prefijo, devuelve todas las claves que empiezan con ese prefijo.
    - `delete_keys`, que dado un prefijo, borra todas las claves que empiezan con ese prefijo (Borrando también los archivos asociados).
  - El persistence manager no puede utilizar directamente la clave como nombre del archivo asociado, ya que las claves pueden contener caracteres no permitidos en nombres de archivos o pueden exceder el limite permitido de caracteres. Por lo tanto, se genera un `uuid` asociado a esa clave que será el nombre del archivo.
  - Hacer esto requiere que además se guarde un índice de claves -> nombres de archivos, que a su vez debe ser persistido.
    - Estos índices pueden crecer mucho en tamaño si un servicio maneja muchas claves, por eso se soporta un parámetro adicional `secondary_key` que permite agrupar todas las claves relacionadas en un solo archivo índice.
      - Por defecto, `secondary_key="default"`, por lo que todas las claves se guardan en el mismo archivo índice `keys_index_default`.
- El gran problema de esta solución es que no se puede garantizar que el estado guardado sea consistente, ya que si un servicio falla mientras está guardando el estado, el archivo puede quedar corrupto.
  - Para resolver esto, se implementaron los siguientes mecanismos:
    - Al hacer una escritura con `put`, en realidad primero se escribe sobre un archivo temporal, y luego se renombra a su nombre final. De esta forma, si la escritura falla, el archivo final no se sobreescribe y se mantiene el estado anterior.
    - Al hacer tanto un `put` como un `append`, se guarda al comienzo de cada linea la longitud de los datos contenidos en esa linea. De esta forma, si la escritura falla, se puede detectar al momento de leer que la linea está corrupta y se descarta.
- ### Ejemplo:

  Supongamos que una instancia de `review_filter` está recibiendo libros del cliente con `client_id=1` que luego utilizará para filtrar reviews.

  - Cuando recibe un libro, con `"title": "book1", "authors": "author1"` tiene que guardarlo en su estado interno, y por lo tanto debe persistirse.
  - Para esto realiza un `append` sobre la clave `books_{client_id}` (En este caso, `books_1`) con el valor `[{book.title}, {book.authors}]` en su instancia de `PersistenceManager`.
  - A medida que recibe libros los agrega a la misma clave, por eso se hace un `append` y no un `put` (Que implicaría sobreescribir todo el archivo de libros por cada nuevo libro una y otra vez).
  - Internamente, el `PersistenceManager` tendrá un archivo con todos los libros recibidos por ese cliente, uno por linea.
  - La primera vez que el `PersistenceManager` reciba un `append` con esa clave, no lo encontrará en su índice, por lo que debe generar un nuevo `uuid` para esa clave y guardarlo en el índice. Por ejemplo: `28bfc74d-5f75-4ecf-8e2d-77e58bc210cc`.
  - Al mismo tiempo, la nueva entrada en el índice se debe persistir a disco, utilizando el mismo mecanismo (`PersistenceManager` hace un append en su clave `keys_index` con el valor `["books_1","28bfc74d-5f75-4ecf-8e2d-77e58bc210cc"]`).
  - Antes de escribirse los datos en los archivos, se debe calcular la longitud de los datos a escribir y guardarse al comienzo de la linea.

  Entonces, el archivo `keys_index_default` a esta altura tendrá el siguiente contenido: (Nota: la longitud de la linea se guarda como bytes, pero lo representamos en decimal para simplificar)

  ```
  50["books_1","28bfc74d-5f75-4ecf-8e2d-77e58bc210cc"]

  ```

  Y el archivo con los libros recibidos por el cliente 1, asociado a la clave `books_1`, llamado `28bfc74d-5f75-4ecf-8e2d-77e58bc210cc` tendrá el siguiente contenido:

  ```
  19["book1","author1"]

  ```

  Si ahora llega un nuevo libro `"title": "book2", "authors": "author2, author3"`, se hará un nuevo `append` sobre la clave `books_1` (El persistence manager ya conoce el archivo asociado a esa clave, por lo que no se agregan entradas al índice), y el archivo `28bfc74d-5f75-4ecf-8e2d-77e58bc210cc` se verá así:

  ```
  19["book1","author1"]
  28["book2","author2, author3"]

  ```

  Pero si al llegar un tercer libro (Por ejemplo, `"title": "book3", "authors": "author4"`) el servicio falla en el momento en el que se está realizando la escritura, y solo se llega a escribir una parte de la linea, el archivo podría verse así:

  ```
  19["book1","author1"]
  28["book2","author2, author3"]
  19["book3","au

  ```

  Entonces, al volver a levantarse el servicio, se ejecutará un `get` sobre la clave `books_1`. Al leer las dos primeras lineas no habrá problemas, ya que el valor de la longitud coincide con el contenido, pero al leer la tercera se detectará que la longitud de la linea no coincide con la cantidad de bytes leidos, por lo que se descartará, y el estado interno del servicio será algo como:

  ```
  {
    "book1": "author1",
    "book2": "author2, author3"
  }

  ```

  Eventualmente el servicio volverá a recibir el paquete que no pudo terminar de procesar que contenía al `book3´, dado que nunca envió el ACK correspondiente a Rabbit, por lo que lo agregará a su estado interno y lo persistirá (esperemos que esta vez si) correctamente.  
  De esta manera el servicio puede volver a levantarse y recuperar su estado interno, aunque haya fallado en medio de una escritura, generando una linea corrupta.

### Recuperación de servicios caídos: _Docktor_ y _Health Checks_

- `Docktor`
  - Se implementó un servicio llamado `Docktor` que se encarga de monitorear el estado de los servicios y reiniciarlos en caso de que fallen.
    - Es necesario que haya multiples instancias de `Docktor`para que si uno falla pueda ser también reiniciado (Siempre debe haber al menos 1 `Docktor` activo).
      - Para evitar solapamientos entre `Docktors`, se garantiza que cada se encarga de monitorear una parte del total de servicios. Esto se hace con un mecanismo similar al de los `Routers`, es decir, se hashea el nombre del servicio y se realiza módulo con la cantidad de instancias de `Docktor`, y si el resultado coincide con el id de la instancia, entonces se encarga de monitorear ese servicio.
        - Para garantizar que siempre se puedan recuperar todos los `Docktor`entre si, se organizan en forma de anillo, donde cada instancia se encarga de monitorerar a la siguiente en el anillo en vez de realizar el hash.
    - La interfaz con el servicio de Docker, siendo que los `Docktor` corren también en contenedores se hace gracias a _Docker in Docker_, que consiste en montar el socket de Docker del host en cada contenedor de `Docktor`.
- `Health Checks`
  - Todos los servicios tienen ahora un Thread con una instancia de una nueva clase `HealthCheck` que se encarga de recibir conexiones TCP en un puerto determinado para comprobar que el servicio está funcionando.
  - Los `Docktor` se conectan a este puerto para comprobar que el servicio está funcionando, y si no lo logran (con un timeout determinado), reinician el servicio.

### Evitar procesamiento de mensajes duplicados

- Por el diseño del sistema, puede suceder que se generen mensajes duplicados en las colas de RabbitMQ, ya que los servicios pueden fallar en medio del procesamiento de un mensaje, habiendo ya encolado emensajes en colas de salida, pero no haber enviado el ACK correspondiente a la cola de entrada.
- En principio, esto no nos importa en los servicios que no tienen estado, pero si en los que sí lo tienen, ya que si un mensaje duplicado llega a un servicio con estado, puede alterar el estado del servicio y producir resultados incorrectos.
- La solución para este problema involucra dos partes:
  - Que el `Middleware` de los servicios interesados en evitar mensajes duplicados guarde el `id` de los paquetes que procesa, y que solo lo procese si no fue marcado como procesado antes.
    - Esto debe además ser persistido en disco, ya que si un servicio falla y se reinicia, no debe volver a procesar mensajes que ya procesó antes, por eso el middleware ahora recibe una instancia opcional de `PersistenceManager`.
  - Hay servicios que actualizan su estado como parte del procesamiento de un paquete y para los que procesar dos veces el mismo paquete no da igual que procesarlo solo una vez, y además persisten a disco este cambio.
    - Pero el `Middleware` solo lo marca como procesado al finalizar el procesamiento, por lo tanto una falla justo antes de marcarse como procesado el paquete pero después de actualizar el estado del servicio provocaría que el paquete vuelva a ser procesado, y que a raíz de esto el servicio vuelva a cambiar su estado incorrectamente.
    - Es por eso que estos servicios deben persistír el id del paquete que generó el último cambio en ese estado, y si vuelven a recibirlo pueden detectarlo y simplemente evitar actualizar el estado otra vez.
- Los ids de los paquetes son generados por el `input gateway` a medida que los recibe por cada cliente.
  - En algunos servicios tenemos que generar nuevos paquetes que son el resultado del procesamiento de múltiples paquetes previos. Ene stos casos, simplemente decidimos que el id del neuvo paquete sea el id del último paquete procesado que influyó en el estado del nuevo paquete.
