# Ingestion Monitoring Dashboard & Alerts
The reference application provides this ARM template to setup a Azure Monitor dashboard to monitor the key metrics of all the resources setup using the template [SAP IoT Integration Reference Template](../arm-template/SAPIoTIntegrationMonitorTemplate.json). This template also sets up all the alerts based on the metrics of various resources and log based alerts for the Azure Function Apps deployed. 

> Note: The monitoring & alerting capabilities provided as part of this sample application is not a mandatory service to be subscribed for enabling integration with the SAP IoT Abstraction Layer. The provided dashboard can serve as a starting point and/or example to monitor Azure resources that are deployed as part of the Azure Resource Manager template.
## Parameters
* **IoT Hub Name**
	* IoT Hub resource name.
* **App Insights Name**
	* Application Insights connected to all Function App in the resource group.
* **Event Hub Name**
	* Event Hub resource name.
* **Azure Data Explorer Name**
	* Azure Data Explorer resource name.
* **Redis Cache Name**
	* Redis Cache resource name.
* **Key Vault Name**
	* Key Vault resource name.
* **Action Group Name**
	* Action Group name to send notifications or invoke actions when an alert is created.						
## Metrics
After the deployment of ARM template for ingestion monitoring dashboard & alerts, the following metrics are captured in the Azure dashboard for each deployed resource.

| Resource Name                                                | Metric Name                         | Description                                                  |
| ------------------------------------------------------------ | ----------------------------------- | ------------------------------------------------------------ |
| IoT Hub                                                      | Telemetry Message Sent              | Number of device-to-cloud telemetry messages sent successfully to your IoT Hub |
| [Metrics Documentation](https://docs.microsoft.com/en-us/azure/iot-hub/iot-hub-metrics#iot-hub-metrics-and-how-to-use-them) | Number of throttling errors         | Number of throttling errors due to device throughput throttles |
| Ingestion Functions                                          | Ingestion C_MessagesProcessed       | Number of time series measures processed                     |
|                                                              | Ingestion Count                     | Number of invocations (batched IoT Hub message)              |
|                                                              | Ingestion AvgDurationMs             | Function invocation average processing time                  |
|                                                              | Ingestion Failures                  | Ingestion Function Failures. If the value is greater than 0, indicates error in processing IoT Hub messages |
| Avro Parser Function                                         | Incoming Message                    | Incoming messages to EventHub Processed Time Series In       |
|                                                              | AvroParser C_MessagesProcessed      | Number of time series measures processed                     |
|                                                              | AvroParser Count                    | Number of invocations (batched Avro messages)                |
|                                                              | AvroParser AvgDurationMs            | Function invocation average processing time                  |
|                                                              | AvroParser Failures                 | Avro Parser Function Failures. If this value is greater then 0, indicates error in processing measures ingested using SAP IoT Time Series APIs |
|                                                              | AvroParser Success Rate             | Success rate parsing Avro message from time series data ingested from SAP IoT Time Series APIs |
| Notification Processor Function                              | NotificationProcessor AvgDurationMs | Function invocation average processing time                  |
|                                                              | NotificationProcessor Failures      | Number of failures in processing model change notifications. If the value is greater than 0, indicates error in processing model change notification generated from SAP IoT |
|                                                              | NotificationProcessor Success Rate  | Success rate of processing model change notification from SAP IoT |
| Device Management Function                                   | DeviceManagement AvgDurationMs      | Function invocation average processing time                  |
|                                                              | NotificationProcessor Failures      | Number of failures in processing Device Management request   |
|                                                              | DeviceManagement Success Rate       | Success rate of processing Device Management request. If the value is greater than 0, indicates error in processing onboarding of device in Azure IoT hub |
| Event Hub                                                    | Incoming Messages                   | Messages ingested per Event Hub                              |
| [Metrics Documentation](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-metrics-azure-monitor) | Outgoing Messages                   | Messages consumed from Event Hubs Namespace split by Entity (Egress) |
|                                                              | Incoming Request                    | Number of requests made to the Azure Event Hubs service      |
|                                                              | Successful Requests                 | Number of successful requests made to the Azure Event Hubs service |
|                                                              | Server Errors                       | Number of requests not processed due to an error in the Azure Event Hubs service |
|                                                              | Throttled Requests                  | The number of requests that were throttled because the throughput unit usage was exceeded |
|                                                              | Incoming Bytes                      | Number of bytes sent to the Azure Event Hubs service         |
|                                                              | Outgoing Bytes                      | Number of bytes consumed from the Azure Event Hubs service   |
| Azure Data Explorer                                          | Cache Utilization                   | Utilization level in the cluster scope                       |
| [Metrics Documentation](https://docs.microsoft.com/en-us/azure/data-explorer/using-metrics) | CPU Utilization                     | CPU utilization level                                        |
|                                                              | Ingestion Utilization               | Ratio of used ingestion slots in the cluster                 |
|                                                              | KeepAlive                           | Sanity check indicates the cluster responds to queries       |
|                                                              | Ingestion Result                    | Number of ingestion operations                               |
|                                                              | Total number of throttled commands  | Total number of throttled commands                           |
|                                                              | Events processed                    | Number of events processed by the cluster when ingesting from Event/IoT Hub |
|                                                              | Ingestion latency                   | Ingestion time from the source (e.g. message is in EventHub) to the cluster in seconds |
|                                                              | Query Duration                      | Queries duration in seconds                                  |
|                                                              | Total number of throttled queries   | Total number of throttled queries                            |
| Azure Cache                                                  | Cache Hits                          | Number of successful key lookups                             |
| [Metrics Documentation](https://docs.microsoft.com/en-us/azure/azure-cache-for-redis/cache-how-to-monitor) | Cache Misses                        | Number of failed key lookups                                 |
|                                                              | Cache Latency                       | Average cache latency is captured                            |
|                                                              | Key Count                           | Number o keys in the cache                                   |
|                                                              | Errors                              | Number of failures in Azure Cache for Redis                  |
|                                                              | Redis Server Load                   | The percentage of cycles in which the Redis server is busy processing and not waiting idle |
| Key Vault                                                    | Overall Vault Availability          | Vault requests availability                                  |
| [Metrics Documentation](https://docs.microsoft.com/en-us/azure/key-vault/general/alert) | Overall Service Api Latency         | Overall latency of service api requests                      |

## Alerts

The alerts and their condition are configured based on the metrics, for various azure resources. They are captured in the table below. 

| Resource Name                  | Alert Name                               | Condition                                                    |
| ------------------------------ | ---------------------------------------- | ------------------------------------------------------------ |
| AvroParser Function            | Avro Parser Function Failures            | Whenever the total avroparser failures is greater than 1     |
| DeviceManagement Function      | Device Management Function Failures      | Whenever the total devicemanagement failures is greater than 1 |
| Ingestion Function             | Ingestion Function Failures              | Whenever the total ingestion failures is greater than 1      |
| NotificationProcessor Function | Notification Processor Function Failures | Whenever the total notificationprocessor failures is greater than 1 |
| Application Insights Logs      | Non Transient Errors                     | exceptions / project timestamp, problemId, outerMessage, severityLevel, itemType, operation_Name, appName, itemCount / where outerMessage like "\"Transient\":false" |
| ADX                            | ADX Ingestion Failures                   | Whenever the count ingestionresult is greater than 1         |
|                                | ADX Throttled Queries                    | Whenever the total totalnumberofthrottledqueries is greater than 5 |
|                                | ADX Cluster Health Status                | Whenever the average keepalive is less than 1                |
| EventHub                       | EventHub User Errors                     | Whenever the count usererrors is greater than 5              |
|                                | EventHub Server Errors                   | Whenever the total servererrors is greater than 5            |
|                                | EventHub Throttling Errors               | Whenever the total throttledrequests is greater than 5       |
| IoT Hub                        | IoTHub Throttling Errors                 | Whenever the total d2c.telemetry.ingress.sendthrottle is greater than 5 |
| Key Vault                      | Vault Availability                       | Whenever the average availability is less than 100           |
| Redis Cache                    | Redis Cache Errors                       | Whenever the total errors is greater than 5                  |
|                                | Redis Cache Server Load                  | Whenever the average serverload is greater than or equal to 99 |

