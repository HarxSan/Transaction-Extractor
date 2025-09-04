import re
import os
import logging
from typing import Optional, List, Tuple

logger = logging.getLogger(__name__)

OFFSET = 15

HEADER_KEYWORDS = [
    "date", "transaction", "txn", "description", "details", "narration", 
    "amount", "debit", "credit", "balance", "particulars", "withdrawal",
    "deposit", "transaction type", "type"
]

COLUMN_KEYWORDS = [
    "transaction", "txn", "description", "details", "narration", 
    "amount", "debit", "credit", "balance", "withdrawal", "deposit", "particulars"
]

SUMMARY_BLACKLIST = [
    "summary", "minimum amount", "payment due", "credit limit", 
    "available credit", "statement date", "account details", "transaction period"
]

DATE_REGEX = re.compile(
    r"""
    (
        \b(?:\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d{2,4}|\d{4}[\/\-.]\d{1,2}[\/\-.]\d{1,2})\b
        |
        \b\d{1,2}(st|nd|rd|th)?[\s\-]?(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*[\s\-]?\d{2,4}\b
        |
        \b\d{1,2}(st|nd|rd|th)?[\s\-]?(January|February|March|April|May|June|July|August|September|October|November|December)[\s\-]?\d{2,4}\b
    )
    """,
    re.VERBOSE | re.IGNORECASE,
)

AMOUNT_REGEX = re.compile(
    r"""
    (?:INR|Rs\.?|₹)?      
    \s*                  
    [-+]?                
    (?:\d{1,3}(?:,\d{2,3})*|\d+)  
    (?:\.\d+)?           
    \s*(Cr|Dr|CR|DR)?    
    """,
    re.VERBOSE,
)

GROQ_BASE_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL = "meta-llama/llama-4-scout-17b-16e-instruct"
GEMINI_MODEL = "gemini-2.0-flash-exp"
DEFAULT_BATCH_SIZE = 1
DEFAULT_DPI = 400
MAX_RETRIES = 3
TEMPERATURE = 0.01
MAX_COMPLETION_TOKENS = 8192

MAX_FILE_SIZE_MB = int(os.getenv("MAX_FILE_SIZE_MB", "50"))
MAX_IMAGE_SIZE_MB = int(os.getenv("MAX_IMAGE_SIZE_MB", "10"))
SUPPORTED_IMAGE_FORMATS = ('.png', '.jpg', '.jpeg', '.bmp', '.tiff')
SUPPORTED_PDF_FORMATS = ('.pdf',)

SESSION_TIMEOUT_MINUTES = int(os.getenv("SESSION_TIMEOUT_MINUTES", "60"))
TEMP_FILE_CLEANUP_ON_EXIT = os.getenv("TEMP_FILE_CLEANUP_ON_EXIT", "true").lower() == "true"
LOG_SENSITIVE_DATA = os.getenv("LOG_SENSITIVE_DATA", "false").lower() == "true"

RATE_LIMIT_RPM = int(os.getenv("RATE_LIMIT_RPM", "30"))
REQUEST_TIMEOUT_SECONDS = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "300"))

def validate_config() -> Tuple[bool, List[str]]:
    """Validate configuration values and return status with any errors."""
    errors = []
    
    if TEMPERATURE < 0 or TEMPERATURE > 2:
        errors.append("Temperature must be between 0 and 2")
    
    if MAX_COMPLETION_TOKENS < 100 or MAX_COMPLETION_TOKENS > 16384:
        errors.append("Max completion tokens must be between 100 and 16384")
    
    if MAX_RETRIES < 1 or MAX_RETRIES > 10:
        errors.append("Max retries must be between 1 and 10")
    
    if DEFAULT_DPI < 150 or DEFAULT_DPI > 600:
        errors.append("DPI must be between 150 and 600")
    
    if MAX_FILE_SIZE_MB < 1 or MAX_FILE_SIZE_MB > 100:
        errors.append("Max file size must be between 1MB and 100MB")
    
    if RATE_LIMIT_RPM < 1 or RATE_LIMIT_RPM > 1000:
        errors.append("Rate limit must be between 1 and 1000 requests per minute")
    
    return len(errors) == 0, errors

def get_api_keys() -> Tuple[Optional[str], Optional[str]]:
    """Get API keys from environment variables."""
    groq_key = os.getenv("GROQ_API_KEY")
    gemini_key = os.getenv("GEMINI_API_KEY")
    return groq_key, gemini_key

def log_config_validation():
    """Log configuration validation results."""
    is_valid, errors = validate_config()
    
    if is_valid:
        logger.info("Configuration validation passed")
    else:
        logger.error(f"Configuration validation failed: {', '.join(errors)}")
        for error in errors:
            logger.error(f"Config error: {error}")
    
    return is_valid

log_config_validation()