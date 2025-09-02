import re

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