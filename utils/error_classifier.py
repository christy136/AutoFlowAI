def classify_error(error_message: str) -> str:
    error_message = error_message.lower()
    if "json" in error_message and ("decode" in error_message or "parse" in error_message):
        return "json_format_error"
    elif "linked service" in error_message:
        return "missing_linked_service"
    elif "dataset" in error_message:
        return "missing_dataset"
    elif "validation" in error_message:
        return "validation_error"
    elif "reference" in error_message:
        return "invalid_reference"
    return "unknown"
