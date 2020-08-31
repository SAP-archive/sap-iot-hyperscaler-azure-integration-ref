# Security

This section provides an overview of security-relevant information that applies this reference template. In addition, you must follow the security policies and best practices recommended by [Microsoft Azure](https://azure.microsoft.com/en-us/overview/security/).

## Secure Storage of Sensitive Data
The reference implementation uses Azure Keyvault to securely store sensitive information such as Azure connection strings and the
 credentials for authenticating against the SAP IoT tenant. 
 The Azure Function resources reference these secrets through keyvault references. Since these references include the version of the secret, the respective
  environment variables have to be adjusted in case the secret is updated in the Keyvault.
 For more information on the Azure Keyvault, please refer to the official [documentation](https://docs.microsoft.com/en-us/azure/key-vault/).

## Data Protection and Privacy

Data protection is associated with numerous legal requirements and privacy concerns. In addition to compliance with general data protection and privacy acts, it is necessary to consider compliance with industry-specific legislation in different countries. 

For more information about data protection and privacy, and access logging in Microsoft Azure, see the following topics:
- [Data Protection and Privacy](http://help.sap.com/disclaimer?site=https://www.microsoft.com/en-us/TrustCenter/CloudServices/Azure/GDPR)
- [Access Logging	](http://help.sap.com/disclaimer?site=https://docs.microsoft.com/en-us/azure/security/azure-log-audit)

**Note –** The Timeseries Abstraction customer-managed solution is not designed to store or process personal and sensitive information. Therefore, it is the
 responsibility of the customer to ensure that such data is not ingested into or stored in the customer-managed data lake that is integrated with this solution.

**Note –** The Timeseries Abstraction customer-managed solution does not support the deletion of telemtry data stored in the customer-managed data lake.
