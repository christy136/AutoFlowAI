# AutoFlowAI – ADF Pipeline Generator & Deployer

LLM ➜ JSON config (schema-validated) ➜ Precheck (auto-fix) ➜ Generate ADF pipeline JSON ➜ Validate ➜ Save ➜ Deploy (+ optional schedule trigger)

## Prerequisites

- Python 3.10+
- Azure CLI logged in (`az login`)
- Azure subscription with:
  - Resource Group
  - Azure Data Factory (V2)
  - Storage account (for Blob source)
- Snowflake account (if you’ll deploy the Snowflake linked service)
- Environment:
  ```bash
  pip install -r requirements.txt
