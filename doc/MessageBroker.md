# Message Broker used in Azure Reference Implementation

EventHub Namespace groups all the EventHubs used in the Azure reference implementation. In the reference implementation, EventHub acts as the source or sink
 for Azure Functions.  

### Event Hubs & Message Format

Below are the details of all the Event Hub Topics used in the Ingestion Flow, Meta Change Notification Processing and Device Management:

| Topic / Purpose&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; | EventHubName           | Message Key           | Message Format           |
| ------------- |:-------------|:-------------|:-------------|
| Processed time series (outbound): Device telemetry data ingested via customer-managed Device Management is transformed to SAP-defined schema and ingested into this event hub. This allows IoT Applications in SAP to consume streaming data      | sap.iot.abstract.processed\_timeseries\_out.v1      |   {sourceId}/{structureId}     | Avro with embedded schema in message - See Avro schema definition below   |
| Processed time series (inbound): Time series data written via Create Time Series Data API is sent to the inbound processed time series topic following the SAP-defined Avro schema. | sap.iot.abstract.processed\_timeseries\_in.v1     |    {sourceId}/{structureId} | Avro with embedded schema in message - See Avro schema definition below      |
| ADX Source Topic: Ingestion source for ADX. Consumer Group used in ADX: sap-iot-ingestion-adx-cg |  sap.iot.abstract.adx\_timeseries.v1      |   sourceId | See below for format   |
| Meta Change: Model change notifications which carry information about changes within the model metadata. The information provided in these notification is used to update the according Azure Cache entries and ADX tables. These change notifications can be of different types (Structure, Mapping and Assignment). Depending on the type, the message key and payload structure differs. | sap.iot.modelabstraction.meta.change.v1 | **Structure Notification:** {tenant\_name}/com.sap.iot.i4c.Structure/{IndicatorGroupId} <br> **Mapping Notification:** {tenant\_name}/com.sap.iot.i4c.Mappings/{mappingId}/{structure id}/{capability id} <br> **Assignment Notification:** {tenant\_name}/com.sap.iot.i4c.Assignments/{sensorId} | Refer to [Notification Processor Documentation](./NotificationProcessor.md) |
| Device Management Request: Onboarding or offboarding of devices via the model onboarding API creates messages in this topic. These are processed and devices are either created or deleted in the Azure IoT Hub. | sap.iot.abstraction.device.management.request | {deviceId} | Refer to [Device Management Documentation](./DeviceManagement.md) |
| Device Management Status: The status of the device request processing is made available via this topic. | sap.iot.abstraction.device.management.status | {deviceId} | Refer to [Device Management Documentation](./DeviceManagement.md) |
| Delete Timeseries Request: Delete time series requests creates messages in this topic. | sap.iot.abstraction.timeseries.delete.request | {structureId} | Refer to [Delete Timeseries Documentation](./DeleteTimeSeries.md) |
| Delete Timeseries Status: The status of the delete time series processing is made available via this topic. | sap.iot.abstraction.timeseries.delete.status | {structureId} | Refer to [Delete Timeseries Documentation](./DeleteTimeSeries.md) |

#### ADX Source Topic Message format
```json
    {
      "appliationProperties": {
        "Table": "SAP__{StructureId}",
        "Format": "MULTIJSON",
        "IngestionMappingReference": "<<StructureId>>" 
      },
      "body": {
        "sourceId":"{sourceId}",
        "_time":"{eventTime}",
        "_isDeleted": false,
        "measurement": {
          "<<prop1>>":"<<val1>>",
          "<<prop2>>":"<<val2>>",
          "<<tagKey1>>":"<<tagVal1>>",
          "<<tagKey2>>":"<<tagVal2>>"
        }
      }
    }
```
### Processed Time Series Message Format 

Messages written into processed time series topic should conform to the Avro schema for the given structure id. Schema lookup API should return the Avro Schema (see API documentation). Please refer to SAP documentation on [Lookup Services Reference]((https://help.sap.com/viewer/DRAFT/224d189da0314339a1dd99489de10e48/2008a/en-US/68edba88c4e34e8390ef2fb8d50e98fb.html)) for more info.  

Below is a sample Avro schema:  
```json
    {
      "type": "record",
      "name": "E10100304AEFE7A616005E02C64AZ111",
      "fields": [
        {
          "name": "messageId",
          "type": "string"
        },
        {
          "name": "identifier",
          "type": "string"
        },
        {
          "name": "structureId",
          "type": "string"
        },
        {
          "name": "tenant",
          "type": "string"
        },
        {
          "name": "tags",
          "type": {
            "type": "array",
            "items": {
              "type": "record",
              "name": "queryParams",
              "fields": [
                {
                  "name": "modelId",
                  "type": [
                    "null",
                    "string"
                  ],
                  "default": null
                },
                {
                  "name": "equipmentId",
                  "type": [
                    "null",
                    "string"
                  ],
                  "default": null
                },
                {
                  "name": "indicatorGroupId",
                  "type": [
                    "null",
                    "string"
                  ],
                  "default": null
                },
                {
                  "name": "templateId",
                  "type": [
                    "null",
                    "string"
                  ],
                  "default": null
                }
              ]
            }
          }
        },
        {
          "name": "measurements",
          "type": {
            "type": "array",
            "items": {
              "type": "record",
              "name": "timeseriesRecord",
              "fields": [
                {
                  "name": "_time",
                  "type": {
                    "type": "long",
                    "logicalType": "nTimestamp"
                  }
                },
                {
                  "name": "Bearing_Temperature",
                  "type": [
                    "null",
                    "int"
                  ]
                },
                {
                  "name": "Pressure",
                  "type": [
                    "null",
                    "int"
                  ]
                },
                {
                  "name": "Status",
                  "type": [
                    "null",
                    "int"
                  ]
                }
              ]
            }
          }
        }
      ]
    }
 ```
