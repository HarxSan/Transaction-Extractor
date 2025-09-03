import asyncio
import aiohttp
import json
import re
from config import *

def safe_json_loads(raw_text: str):
    cleaned = re.sub(r'[\x00-\x1F\x7F]', '', raw_text)
    cleaned = cleaned.strip()
    if cleaned.startswith("```json"):
        cleaned = cleaned[7:]
    if cleaned.endswith("```"):
        cleaned = cleaned[:-3]
    cleaned = re.sub(r',\s*([\]}])', r'\1', cleaned)
    return json.loads(cleaned)

class GeminiExtractor:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
        
    def get_extraction_prompt(self, extracted_text):
        return f"""Extract ALL financial transactions from this markdown content and return them as JSON.

    GROUND TRUTH TEXT FROM PDF:
    {extracted_text}

    Use this extracted text as the authoritative source for transaction details and amounts. 
    The markdown content should be used to understand structure, but all transaction data must match exactly with the ground truth text above.

    For each transaction found, provide:
    - date: Use DD/MM/YYYY format exactly
    - description: Merchant name or transaction description exactly as written in ground truth
    - amount: Numeric value only (no currency symbols) exactly matching ground truth
    - type: "Credit" for payments/refunds or "Debit" for purchases/charges

    CRITICAL JSON RULES:
    - Return output strictly inside a fenced JSON code block: ```json ... ```
    - Do not add any text outside the JSON block.
    - Every string field must be a single line. 
    - Do not include raw control characters (\\n, \\r, \\t, or any ASCII < 32) inside values.
    - If the ground truth text contains line breaks or unusual spacing, replace them with a single space in the JSON value.
    - Escape all double quotes inside strings as \\" 
    - Do not include trailing commas in arrays or objects.

    Transaction processing rules:
    - Cross-reference every transaction detail with the ground truth text
    - Ensure each date-description-amount triplet matches exactly
    - Only include the merchant or service name in the Description (exclude customer names, "MR", "MRS", HTML tags, <br>, or personal names)
    - Ensure Description is concise and only shows the merchant/service name
    - Process transactions in the EXACT ORDER they appear in the ground truth
    - Include ALL transactions without missing any
    - Only include actual transactions, exclude summary information
    - While marking transaction type, check for keywords like CR/Cr/cr for Credit; otherwise, default to Debit
    - Do not guess or fabricate data — if uncertain, omit the transaction

    Return JSON format:

    ```json
    {{
    "transactions": [
        {{
        "date": "15/06/2024",
        "description": "AMAZON INDIA",
        "amount": 1499.00,
        "type": "Debit"
        }}
    ]
    }}
    ```"""

    async def extract_transactions_from_markdown(self, markdown_content, extracted_text):
        print(f"DEBUG: Processing markdown content of length: {len(markdown_content)}")
        print(f"DEBUG: Processing extracted text of length: {len(extracted_text)}")
        
        headers = {
            "Content-Type": "application/json"
        }
        
        payload = {
            "contents": [{
                "parts": [{
                    "text": f"{self.get_extraction_prompt(extracted_text)}\n\nMarkdown content:\n{markdown_content}"
                }]
            }],
            "generationConfig": {
                "temperature": TEMPERATURE,
                "maxOutputTokens": MAX_COMPLETION_TOKENS,
                "responseMimeType": "application/json"
            }
        }
        
        url = f"{self.base_url}?key={self.api_key}"
        print("DEBUG: Sending request to Gemini API")
        
        for attempt in range(MAX_RETRIES):
            try:
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=300)) as session:
                    async with session.post(url, headers=headers, json=payload) as response:
                        print(f"DEBUG: Gemini API response status: {response.status}")
                        
                        if response.status == 200:
                            result = await response.json()
                            print("DEBUG: Successful Gemini response received")
                            
                            if 'candidates' in result and len(result['candidates']) > 0:
                                content = result['candidates'][0]['content']['parts'][0]['text']
                                print(f"DEBUG: Extracted content length: {len(content)}")
                                return content
                            else:
                                print(f"ERROR: No candidates in Gemini response: {result}")
                                return None
                        else:
                            error_text = await response.text()
                            print(f"ERROR: Gemini API call failed - Status {response.status}: {error_text}")
                            if attempt == MAX_RETRIES - 1:
                                raise Exception(f"Gemini API Error {response.status}: {error_text}")
                            
            except asyncio.TimeoutError:
                print(f"ERROR: Gemini request timeout on attempt {attempt + 1}")
                if attempt == MAX_RETRIES - 1:
                    raise Exception("Gemini request timeout after all retries")
                    
            except Exception as e:
                print(f"ERROR: Gemini request failed on attempt {attempt + 1}: {str(e)}")
                if attempt == MAX_RETRIES - 1:
                    raise e
                await asyncio.sleep(2 ** attempt)
        
        return None
    
    def process_gemini_result(self, result):
        try:
            data = safe_json_loads(result)
            print("DEBUG: Parsed Gemini JSON successfully")
            
            if 'transactions' in data and isinstance(data['transactions'], list):
                print(f"DEBUG: Found {len(data['transactions'])} transactions in Gemini response")
                
                processed_transactions = []
                for i, txn in enumerate(data['transactions']):
                    if all(key in txn for key in ['date', 'description', 'amount', 'type']):
                        try:
                            amount = float(str(txn['amount']).replace(',', '').replace('₹', '').replace('Rs', '').strip()) if isinstance(txn['amount'], (int, float, str)) else 0.0
                            
                            description = str(txn['description']).strip()
                            description = ' '.join(description.split())
                            
                            date_str = str(txn['date']).strip()
                            
                            if '/' in date_str:
                                parts = date_str.split('/')
                                if len(parts) == 3:
                                    day, month, year = parts[0].zfill(2), parts[1].zfill(2), parts[2]
                                    if len(year) == 2:
                                        year = '20' + year if int(year) < 50 else '19' + year
                                    date_str = f"{day}/{month}/{year}"
                            
                            txn_type = str(txn['type']).strip().title()
                            if txn_type not in ['Credit', 'Debit']:
                                if any(word in description.upper() for word in ['PAYMENT', 'REFUND', 'CREDIT', 'SALARY', 'DEPOSIT']):
                                    txn_type = 'Credit'
                                else:
                                    txn_type = 'Debit'
                            
                            if amount > 0 and description and date_str:
                                processed_txn = {
                                    'date': date_str,
                                    'description': description,
                                    'amount': amount,
                                    'type': txn_type
                                }
                                processed_transactions.append(processed_txn)
                                print(f"DEBUG: Processed transaction {i+1}: {processed_txn}")
                            else:
                                print(f"DEBUG: Skipped transaction {i+1} - invalid data: amount={amount}, desc='{description}', date='{date_str}'")
                        except (ValueError, TypeError) as e:
                            print(f"DEBUG: Skipped transaction {i+1} - processing error: {e}")
                    else:
                        missing_fields = [key for key in ['date', 'description', 'amount', 'type'] if key not in txn]
                        print(f"DEBUG: Skipped transaction {i+1} - missing fields: {missing_fields}")
                
                unique_transactions = self._remove_duplicates(processed_transactions)
                print(f"DEBUG: Final Gemini result: {len(unique_transactions)} unique transactions from {len(processed_transactions)} total")
                return unique_transactions
            else:
                print("DEBUG: No 'transactions' array found in Gemini response or invalid format")
                print(f"DEBUG: Response keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
                return []
                
        except json.JSONDecodeError as e:
            print(f"ERROR: JSON decode error from Gemini: {e}")
            print(f"ERROR: Raw Gemini response was: {result}")
            return []
        except Exception as e:
            print(f"ERROR: Unexpected error processing Gemini result: {e}")
            return []
    
    def _remove_duplicates(self, all_transactions):
        unique_transactions = []
        seen = set()
        
        for txn in all_transactions:
            key = (txn['date'], txn['description'], txn['amount'])
            if key not in seen:
                seen.add(key)
                unique_transactions.append(txn)
            else:
                print(f"DEBUG: Removed duplicate: {txn}")
        
        return unique_transactions
