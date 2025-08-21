import os
import sys
import json
import time
import csv
import re
from dotenv import load_dotenv
import google.generativeai as genai
from tqdm import tqdm
from io import StringIO

load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-2.0-flash')

REQUEST_DELAY = 1.0

def create_transaction_extraction_prompt():
    return """Extract ALL transaction data from LaTeX tables and convert to CSV with STRICT schema enforcement.

MANDATORY SCHEMA RULES:

BANK STATEMENTS (if you find 2 amount columns):
- EXACT 5 COLUMNS: Date,Description,First_Amount,Second_Amount,Balance
- First amount column = whatever comes first (credit/debit/deposit/withdrawal)
- Second amount column = whatever comes second
- Column names: use "First_Amount" and "Second_Amount" (not original names)
- Skip reward points, interest rate, or non-transaction columns

CREDIT CARD STATEMENTS (if you find 1 amount column):
- EXACT 4 COLUMNS: Date,Description,Amount,Transaction_Type
- Transaction_Type: ONLY "Credit" or "Debit" (nothing else)
- Determine type from context: payments/refunds=Credit, purchases/fees=Debit

COMPREHENSIVE TRANSACTION EXTRACTION:
- Extract EVERY transaction regardless of date format
- Include ALL: purchases, payments, transfers, deposits, withdrawals, fees, charges, refunds
- Include continuing descriptions that span multiple lines
- Include transactions with partial or missing amounts (use 0)
- Include ATM withdrawals, online transfers, card payments, checks, direct debits
- Include interest credits, service charges, overdraft fees
- Do NOT miss any financial activity

TRANSACTION FILTERING:
- Extract ALL rows containing transaction data (even if date format varies)
- Include: payments, purchases, transfers, fees, refunds, deposits, withdrawals
- Skip ONLY: page headers, account numbers, statement periods, summary totals
- Include rows with dates in ANY format: DD/MM/YYYY, MM/DD/YYYY, DD-MM-YYYY, YYYY-MM-DD
- Include continuing transaction descriptions on next line if they belong to same transaction

AMOUNT PROCESSING:
- Remove: ₹, Rs, INR, commas, spaces
- Keep only numbers and decimal point
- "1,234.56 Cr" → 1234.56
- Empty amounts → 0
- Include negative amounts if present

CSV FORMAT:
- Comma separated
- Wrap descriptions with commas in quotes
- No extra spaces or trailing commas
- Header row + data rows only
- Combine multi-line descriptions into single description field

VALIDATION REQUIREMENTS:
- Every row must have exact same number of columns as header
- All amount columns must contain only numbers (no text)
- Transaction_Type (if present) must be only "Credit" or "Debit"

Extract transactions from:

"""

def validate_csv_structure(csv_text):
    if not csv_text:
        return False, "Empty CSV"
    
    lines = [line.strip() for line in csv_text.split('\n') if line.strip()]
    if len(lines) < 2:
        return False, "Need header + data rows"
    
    try:
        reader = csv.reader(StringIO('\n'.join(lines)))
        rows = list(reader)
        
        if len(rows) < 2:
            return False, "Need header + data rows"
        
        header = rows[0]
        header_cols = len(header)
        
        if header_cols == 5:
            expected_columns = ['Date', 'Description', 'First_Amount', 'Second_Amount', 'Balance']
            if header != expected_columns:
                return False, f"Bank schema error. Expected: {expected_columns}, Got: {header}"
        elif header_cols == 4:
            expected_columns = ['Date', 'Description', 'Amount', 'Transaction_Type']
            if header != expected_columns:
                return False, f"Credit card schema error. Expected: {expected_columns}, Got: {header}"
        else:
            return False, f"Invalid schema. Expected 4 or 5 columns, got {header_cols}"
        
        for i, row in enumerate(rows[1:], 1):
            if len(row) != header_cols:
                return False, f"Row {i}: {len(row)} fields, expected {header_cols}"
            
            if header_cols == 5:
                try:
                    if row[2]:
                        float(row[2])
                    if row[3]:
                        float(row[3])
                    if row[4]:
                        float(row[4])
                except ValueError:
                    return False, f"Row {i}: Invalid amount values"
            elif header_cols == 4:
                try:
                    if row[2]:
                        float(row[2])
                except ValueError:
                    return False, f"Row {i}: Invalid amount value"
                if row[3] and row[3] not in ['Credit', 'Debit', '']:
                    return False, f"Row {i}: Transaction_Type must be 'Credit' or 'Debit', got '{row[3]}'"
        
        return True, f"Valid: {len(rows)-1} transactions, {header_cols} columns"
    
    except Exception as e:
        return False, f"CSV parse error: {str(e)}"

def clean_csv_response(csv_text):
    if not csv_text:
        return None
    
    csv_text = re.sub(r'```csv\n?', '', csv_text)
    csv_text = re.sub(r'```\n?', '', csv_text)
    
    lines = [line.strip() for line in csv_text.split('\n') if line.strip()]
    
    if len(lines) < 2:
        return None
    
    clean_lines = []
    for line in lines:
        line = line.rstrip(',')
        clean_lines.append(line)
    
    return '\n'.join(clean_lines)

def extract_transactions_with_gemini(txt_content, filename, max_retries=3):
    prompt = create_transaction_extraction_prompt() + txt_content
    
    for attempt in range(max_retries):
        try:
            print(f"Processing {filename} (attempt {attempt + 1}/{max_retries})")
            
            response = model.generate_content(
                prompt,
                generation_config={
                    "temperature": 0.05,
                }
            )
            
            if response.text:
                cleaned_csv = clean_csv_response(response.text.strip())
                
                if cleaned_csv:
                    is_valid, validation_msg = validate_csv_structure(cleaned_csv)
                    if is_valid:
                        print(f"✅ {filename}: {validation_msg}")
                        time.sleep(REQUEST_DELAY)
                        return cleaned_csv
                    else:
                        print(f"❌ {filename}: {validation_msg}")
                        if attempt == max_retries - 1:
                            return None
                
        except Exception as e:
            print(f"❌ Error processing {filename} (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                wait_time = 5 * (attempt + 1)
                time.sleep(wait_time)
    
    print(f"❌ Failed to process {filename} after {max_retries} attempts")
    return None

def save_csv(csv_content, output_path):
    try:
        with open(output_path, 'w', encoding='utf-8', newline='') as f:
            f.write(csv_content)
        return True
    except Exception as e:
        print(f"❌ Error saving CSV to {output_path}: {e}")
        return False

def save_failed_processing_log(filename, txt_content, output_dir):
    failed_dir = os.path.join(output_dir, "failed_processing")
    os.makedirs(failed_dir, exist_ok=True)
    
    failed_path = os.path.join(failed_dir, f"{filename}.txt")
    with open(failed_path, 'w', encoding='utf-8') as f:
        f.write(txt_content)
    
    print(f"💾 Saved failed file: {failed_path}")

def process_txt_folder(input_folder, output_folder):
    os.makedirs(output_folder, exist_ok=True)
    
    txt_files = [f for f in os.listdir(input_folder) if f.endswith('.txt')]
    
    if not txt_files:
        print(f"❌ No .txt files found in {input_folder}")
        return
    
    print(f"📁 Found {len(txt_files)} txt files")
    print(f"🎯 Output: {output_folder}")
    print(f"⏱️ Delay: {REQUEST_DELAY}s")
    
    successful = 0
    failed = 0
    
    with tqdm(total=len(txt_files), desc="Processing", unit="file") as pbar:
        for txt_file in txt_files:
            txt_path = os.path.join(input_folder, txt_file)
            base_name = os.path.splitext(txt_file)[0]
            csv_path = os.path.join(output_folder, f"{base_name}_transactions.csv")
            
            try:
                with open(txt_path, 'r', encoding='utf-8') as f:
                    txt_content = f.read()
            except Exception as e:
                print(f"❌ Error reading {txt_file}: {e}")
                failed += 1
                pbar.update(1)
                continue
            
            if len(txt_content.strip()) < 50:
                print(f"⚠️ Skipping {txt_file} - content too small")
                pbar.update(1)
                continue
            
            csv_response = extract_transactions_with_gemini(txt_content, txt_file)
            
            if csv_response and save_csv(csv_response, csv_path):
                successful += 1
            else:
                save_failed_processing_log(base_name, txt_content, output_folder)
                failed += 1
            
            pbar.update(1)
    
    print(f"\n📊 Results: ✅ {successful}, ❌ {failed}")
    if failed > 0:
        print("🔍 Check failed_processing/ folder")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python llm.py <txt_files_folder> <output_csv_folder>")
        sys.exit(1)
    
    input_folder = sys.argv[1]
    output_folder = sys.argv[2]
    
    if not os.path.exists(input_folder):
        print(f"❌ Input folder does not exist: {input_folder}")
        sys.exit(1)
    
    print("🚀 Starting transaction extraction")
    print(f"📂 Input: {input_folder}")
    print(f"📁 Output: {output_folder}")
    
    process_txt_folder(input_folder, output_folder)