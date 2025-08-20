import os
import sys
import json
import time
import csv
import re
from dotenv import load_dotenv
import google.generativeai as genai
from tqdm import tqdm

# Load environment variables
load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# Configure Gemini
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-2.5-flash')

# Request delay
REQUEST_DELAY = 1.0  # seconds between requests

def create_transaction_extraction_prompt():
    """Create comprehensive prompt for transaction extraction"""
    return """You are an expert at extracting transaction data from bank and credit card statements. 

TASK: Extract ONLY transaction data from the provided LaTeX table text and convert to CSV format.

IMPORTANT PARSING RULES:
1. **Table Structure**: Tables are in LaTeX format with & symbols separating columns
2. **Empty Cells**: If there is empty space between two & symbols (like "& &"), that cell is EMPTY - preserve this as empty in CSV
3. **Transaction Identification**: Only extract rows that START with a date pattern (various formats possible)
4. **Table Types**: 
   - Extract from tables with headers like "Date", "Transaction", "Description", "Amount", "Balance", "Credit", "Debit", "Points", "Reward"
   - SKIP tables like "Account Summary", "GST Summary", "Reward Points Summary", "Past Dues", etc.

TRANSACTION DATA EXTRACTION:
1. **Date Column**: Preserve original date format (don't convert)
2. **Description**: Keep transaction description as-is
3. **Amount Handling**: 
   - Remove "INR" prefixes and commas from amounts
   - If amount has "Cr", "CR", or "cr" suffix, create "transaction_type" column = "Credit", otherwise "Debit"
   - Clean amount: "30,840.00" → "30840.00", "363.62 Cr" → "363.62"
4. **Empty Columns**: If original table has empty cells (& &), keep them empty in CSV
5. **Column Headers**: Preserve original column names from the statement

CREDIT CARD SPECIFIC:
- For credit cards (typically 4-5 columns), always create "transaction_type" column
- Mark transactions as "Credit" or "Debit" based on amount suffix or context

OUTPUT FORMAT:
- Return ONLY clean CSV data (no explanations)
- First row should be column headers
- Each subsequent row should be one transaction
- Use comma separation
- If cells contain commas, wrap in quotes
- Empty cells should be truly empty (not "N/A" or "-")

EXAMPLE:
Input: 20/06/2025 & PHONEPE Bengaluru & & 30,840.00\\
Output: 20/06/2025,PHONEPE Bengaluru,,30840.00,Debit

Input: 28/05/2025 & AMAZON PAY INDIA & - 10 & 363.62 Cr\\
Output: 28/05/2025,AMAZON PAY INDIA,- 10,363.62,Credit

Now extract transaction data from this statement:

"""

def extract_transactions_with_gemini(txt_content, filename, max_retries=3):
    """Extract transactions using Gemini API"""
    
    prompt = create_transaction_extraction_prompt() + txt_content
    
    for attempt in range(max_retries):
        try:
            print(f"📤 Sending {filename} to Gemini (attempt {attempt + 1}/{max_retries})")
            
            response = model.generate_content(
                prompt,
                generation_config={
                    "temperature": 0.1,  # Low temperature for consistent parsing
                }
            )
            
            if response.text:
                print(f"✅ Gemini processed {filename} successfully")
                time.sleep(REQUEST_DELAY)
                return response.text.strip()
            else:
                print(f"⚠️ Empty response from Gemini for {filename}")
                
        except Exception as e:
            print(f"❌ Error processing {filename} with Gemini (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                wait_time = 5 * (attempt + 1)
                print(f"⏳ Retrying in {wait_time}s...")
                time.sleep(wait_time)
    
    print(f"❌ Failed to process {filename} after {max_retries} attempts")
    return None

def clean_csv_response(csv_text):
    """Clean and validate CSV response from Gemini"""
    if not csv_text:
        return None
    
    # Remove any markdown code blocks if present
    csv_text = re.sub(r'```csv\n?', '', csv_text)
    csv_text = re.sub(r'```\n?', '', csv_text)
    
    # Split into lines and remove empty lines
    lines = [line.strip() for line in csv_text.split('\n') if line.strip()]
    
    if len(lines) < 2:  # At least header + 1 data row
        return None
    
    return '\n'.join(lines)

def save_csv(csv_content, output_path):
    """Save cleaned CSV content to file"""
    try:
        with open(output_path, 'w', encoding='utf-8', newline='') as f:
            f.write(csv_content)
        return True
    except Exception as e:
        print(f"❌ Error saving CSV to {output_path}: {e}")
        return False

def save_failed_processing_log(filename, txt_content, output_dir):
    """Save original txt content for manual review when processing fails"""
    failed_dir = os.path.join(output_dir, "failed_processing")
    os.makedirs(failed_dir, exist_ok=True)
    
    failed_path = os.path.join(failed_dir, f"{filename}.txt")
    with open(failed_path, 'w', encoding='utf-8') as f:
        f.write(txt_content)
    
    print(f"💾 Saved failed file for manual review: {failed_path}")

def process_txt_folder(input_folder, output_folder):
    """Process all txt files in input folder and generate CSV files"""
    
    # Create output directory
    os.makedirs(output_folder, exist_ok=True)
    
    # Get all txt files
    txt_files = [f for f in os.listdir(input_folder) if f.endswith('.txt')]
    
    if not txt_files:
        print(f"❌ No .txt files found in {input_folder}")
        return
    
    print(f"📁 Found {len(txt_files)} txt files to process")
    print(f"🎯 Output directory: {output_folder}")
    print(f"⏱️ Request delay: {REQUEST_DELAY}s between API calls")
    
    successful = 0
    failed = 0
    
    with tqdm(total=len(txt_files), desc="🔄 Processing statements", unit="file") as pbar:
        for txt_file in txt_files:
            txt_path = os.path.join(input_folder, txt_file)
            base_name = os.path.splitext(txt_file)[0]
            csv_path = os.path.join(output_folder, f"{base_name}_transactions.csv")
            
            # Read txt content
            try:
                with open(txt_path, 'r', encoding='utf-8') as f:
                    txt_content = f.read()
            except Exception as e:
                print(f"❌ Error reading {txt_file}: {e}")
                failed += 1
                pbar.update(1)
                continue
            
            # Skip if txt file is empty or too small
            if len(txt_content.strip()) < 50:
                print(f"⚠️ Skipping {txt_file} - content too small")
                pbar.update(1)
                continue
            
            # Extract transactions with Gemini
            csv_response = extract_transactions_with_gemini(txt_content, txt_file)
            
            if csv_response:
                # Clean and validate CSV
                clean_csv = clean_csv_response(csv_response)
                
                if clean_csv and save_csv(clean_csv, csv_path):
                    print(f"💾 Saved: {csv_path}")
                    successful += 1
                else:
                    print(f"❌ Failed to save valid CSV for {txt_file}")
                    save_failed_processing_log(base_name, txt_content, output_folder)
                    failed += 1
            else:
                print(f"❌ Failed to extract transactions from {txt_file}")
                save_failed_processing_log(base_name, txt_content, output_folder)
                failed += 1
            
            pbar.update(1)
    
    print("\n📊 Processing Summary:")
    print(f"✅ Successful: {successful}")
    print(f"❌ Failed: {failed}")
    print(f"📁 Output directory: {output_folder}")
    if failed > 0:
        print("🔍 Check failed_processing/ folder for manual review")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python transaction_processor.py <txt_files_folder> <output_csv_folder>")
        print("Example: python transaction_processor.py ./extracted_tables ./transaction_csvs")
        sys.exit(1)
    
    input_folder = sys.argv[1]
    output_folder = sys.argv[2]
    
    if not os.path.exists(input_folder):
        print(f"❌ Input folder does not exist: {input_folder}")
        sys.exit(1)
    
    print("🚀 Starting transaction extraction pipeline")
    print(f"📂 Input: {input_folder}")
    print(f"📁 Output: {output_folder}")
    print("🤖 Using: Gemini 2.0 Flash")
    
    process_txt_folder(input_folder, output_folder)