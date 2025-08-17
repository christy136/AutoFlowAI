# utils/settings.py
import os

# Canonical Linked Service names
BLOB_LS_DEFAULT = "AzureBlobStorageLinkedService"
SNOWFLAKE_LS_DEFAULT = "Snowflake_LS"  # <-- set to your actual LS name in ADF

# Canonical dataset names
SRC_DS_DEFAULT = "SourceDataset"
SNK_DS_DEFAULT = "SinkDataset"

# CSV parsing defaults
CSV_DELIMITER_DEFAULT = ","
CSV_HEADER_DEFAULT = True

# LLM model control
OPENROUTER_MODEL_DEFAULT = "deepseek/deepseek-chat-v3-0324:free"

# Guidance region fallback (only for messages, not commands)
REGION_FALLBACK = "westeurope"
