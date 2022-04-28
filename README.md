# Trabajo Práctico 1 "Concurrencia y Comunicaciones"

## Sistemas Distribuidos I



#### Servidor

El comando para correr el servidor es el siguiente:


```bash
make server-up
```



Para finalizar su ejecución, el comando es:


```bash
make server-down
```



El servidor cuenta con un archivo de configuraciones `/server/config.yaml` que puede ser modificado para alterar ciertos parámetros de su funcionalidad.



```yaml
address:
  ip: "server"
  port: "12345"
client:
  timeout: 5s //Timeout for receive syscall on client socket
log:
  level: 'debug'
workers: //Number of available workers
  database:
    writers: 4
    readers: 4
    mergers: 4
  connection: 10
queues: //Size of the message queues
  database:
    readers: 64
    writers: 256
    merger-admin: 256
    mergers: 256
  alarm-manager: 64
  connection: 64
  dispatcher: 256
database:
  epoch-duration: 2s	//Duration of a database epoch
  files-path: ./files   //File path where database will store metrics
alarm:
  period: 60s					//Alarm checking period
  config-path: ./alarms.json	//Alarm configuration file path
```



Adicionalmente se incluye un archivo `/server/alarms.json` en el cual deberán ser configuradas las alarmas que se deseen ejecutar periódicamente.



```json
[
    {
        "id": "ALARM_1",
        "metric_id": "CLI_METRIC_1",
        "aggregation": "COUNT",
        "aggregation_window_secs": 60,
        "limit": 800
    },
    {
        "id": "ALARM_2",
        "metric_id": "CLI_METRIC_1",
        "aggregation": "COUNT",
        "aggregation_window_secs": 15.0,
        "limit": 75
    }
]
```





### Clientes

Se incluyen dos aplicaciones cliente, una orientada al envío de métricas y la otra orientada a la realización de consultas.



#### Generación de Métricas

Esta aplicación cliente realiza un envío de métricas en base a las configuraciones provistas en el archivo  `/client_app/config.yaml`



*Archivo de Configuraciones*

```yaml
server:
  address: "server:12345"
loop:
  number: "100" //Number of metrics to send
  period: "1s" //Time to wait between metrics
log:
  level: "info"
```



El comando para ejecutar los clientes es el siguiente:


```bash
make client-app-up
```

Para su finalización el comando es:


```bash
make client-app-down
```



Cada cliente enviará una métrica `CLI_METRIC_{ID}` donde el campo ID corresponde al ID asignado al cliente al momento de iniciar el contenedor de docker. De esta forma, es posible asignar a más de un cliente generando la misma métrica simplemente cambiando la asignación de la ID para que coincida con cuantos clientes se desee.

Este tipo de modificaciones deberá realizarse en el archivo `docker-compose-clients.yaml`. Notar que también es posible modificar la cantidad de clientes generados agregando nuevos servicios que utilicen el mismo contenedor. 



#### Generación de Queries

Esta aplicación cliente realiza un envío de las queries configuradas en el archivo `/client_query/queries.json` en base a las configuraciones provistas en el archivo  `/client_query/config.yaml`





*Archivo de Configuraciones*

```yaml
server:
  address: "server:12345"
loop:
  period: "100ms" //Time to wait between queries
log:
  level: "info"
queries:
  file-path: ./queries.json  //File where queries are configured
```



*Archivo de Queries*

```json
[
    {
        "metric_id": "TEST_QUERY_1",
        "from": "2022-04-27T22:12:25.012Z",
        "to": "2022-04-27T22:12:34.022Z",
        "aggregation": "COUNT",
        "aggregation_window_secs": 10
    },
    {
        "metric_id": "TEST_QUERY_1",
        "from": "2022-04-27T22:12:25.012Z",
        "to": "2022-04-27T22:12:34.022Z",
        "aggregation": "MAX",
        "aggregation_window_secs": 60
    }
]
```





El comando para ejecutar el cliente es el siguiente:


```bash
make client-query-up
```

Para su finalización el comando es:


```bash
make client-query-down
```



Al iniciar, el cliente establecerá una conexión con el servidor y comenzará a enviar las queries al servidor, de forma secuencial, esperando la respuesta por cada una de ellas y respetando el período de envío configurado.



**Nota**: Todas las aplicaciones quedarán a la escucha del correspondiente comando de finalización para terminar su ejecución.

