import streamlit as st
import os
import tempfile
import shutil
import pandas as pd
import json
from datetime import datetime
import subprocess
import sys
from pathlib import Path
import time
import re

st.set_page_config(
    page_title="Bank Statement Processor",
    page_icon="🏦",
    layout="wide"
)

st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .stage-container {
        border: 1px solid #e0e0e0;
        border-radius: 8px;
        padding: 15px;
        margin: 8px 0;
        background-color: #ffffff;
    }
    .stage-complete {
        border-color: #28a745;
        background-color: #f8fff9;
    }
    .stage-processing {
        border-color: #ffc107;
        background-color: #fffbf0;
    }
    .metric-card {
        background-color: white;
        padding: 15px;
        border-radius: 8px;
        border: 1px solid #e0e0e0;
        text-align: center;
        margin: 5px;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 24px;
    }
    .main > div {
        padding-top: 1rem;
    }
</style>
""", unsafe_allow_html=True)

def run_script_with_progress(script_path, args, stage_name):
    try:
        cmd = [sys.executable, script_path] + args
        env = os.environ.copy()
        env['PYTHONIOENCODING'] = 'utf-8'
        
        process = subprocess.Popen(
            cmd, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.STDOUT, 
            universal_newlines=True,
            bufsize=1,
            env=env,
            encoding='utf-8',
            errors='replace'
        )
        
        key_messages = []
        for line in iter(process.stdout.readline, ''):
            clean_line = line.strip().encode('utf-8', errors='ignore').decode('utf-8', errors='ignore')
            if any(keyword in clean_line.lower() for keyword in ['processing', 'completed', 'error', 'failed', 'success']):
                key_messages.append(clean_line)
                st.text(clean_line)
        
        process.wait()
        return process.returncode == 0, key_messages
    except Exception as e:
        st.error(f"Error running {stage_name}: {str(e)}")
        return False, [str(e)]

def create_transaction_extraction_prompt():
    return """You are an expert at extracting transaction data from bank and credit card statements.

CRITICAL RULES - MUST FOLLOW EXACTLY:

1. ANALYZE THE TABLE STRUCTURE FIRST:
   - Count the number of columns in the header row
   - EVERY data row MUST have EXACTLY the same number of columns as the header
   - If header has 4 columns, EVERY row must have exactly 4 fields
   - If header has 5 columns, EVERY row must have exactly 5 fields

2. TRANSACTION TYPE DETECTION:

   FOR CREDIT CARD STATEMENTS:
   - Look for amounts with "Cr", "CR", "cr" suffix = CREDIT transaction
   - Amounts without "Cr/CR/cr" suffix = DEBIT transaction
   - If original table has separate Credit/Debit columns, preserve them exactly
   - Clean amounts: "1,234.56 Cr" → "1234.56" and note as Credit

   FOR BANK STATEMENTS:
   - Look for column headers like "Debit", "Withdrawal", "Dr", "Debited"
   - Look for column headers like "Credit", "Deposit", "Cr", "Credited" 
   - Preserve these column structures exactly as they appear
   - Do NOT add transaction_type column if separate Credit/Debit columns exist

3. COLUMN CONSISTENCY ENFORCEMENT:
   - Missing data = empty field (just comma, no space)
   - Extra data = merge into appropriate existing column
   - NEVER add or remove columns from what's in the original table header
   - Use EXACT original column names from the statement

4. TRANSACTION IDENTIFICATION:
   - Extract ONLY rows that start with a date pattern
   - SKIP summary rows, totals, headers, account info, balance forward

5. CSV FORMATTING - STRICT:
   - NO extra commas beyond column separators
   - If text contains commas, wrap entire field in double quotes: "AMAZON, INDIA"
   - Empty cells: just comma with nothing between
   - Remove currency symbols (₹, INR, Rs) but keep amounts
   - Clean amounts: remove commas from numbers "1,234.56" → "1234.56"

VALIDATION CHECKLIST:
- Count columns in header row = X
- Count fields in EVERY data row = X (same number)
- No row should have more or fewer fields than header
- All commas within text are properly quoted
- No trailing commas at end of rows
- Transaction types correctly identified based on statement type

EXAMPLES:

CREDIT CARD (with Cr suffix):
Header: Date & Description & Amount & Balance \\
Data: 20/06/2024 & PHONEPE & 1,000.00 Cr & 45,000.00 \\
OUTPUT:
Date,Description,Amount,Balance,Transaction_Type
20/06/2024,PHONEPE,1000.00,45000.00,Credit

BANK STATEMENT (separate columns):
Header: Date & Description & Debit & Credit & Balance \\
Data: 20/06/2024 & SALARY & & 50,000.00 & 75,000.00 \\
OUTPUT:
Date,Description,Debit,Credit,Balance
20/06/2024,SALARY,,50000.00,75000.00

PROCESS:
1. Identify if Credit Card (Cr suffix) or Bank Statement (separate columns)
2. Count exact column structure from headers
3. Extract only transaction rows (date-starting rows)
4. Clean amounts and preserve transaction type info
5. Ensure each row has exact same field count as header
6. Return ONLY the CSV with no explanations

Now extract transaction data from this statement:

"""

def analyze_csv_file(csv_path):
    try:
        df = pd.read_csv(csv_path)
        
        total_transactions = len(df)
        
        # Enhanced transaction type and amount detection
        amount_cols = []
        credit_cols = []
        debit_cols = []
        transaction_type_col = None
        
        for col in df.columns:
            col_lower = col.lower()
            
            # Look for amount columns
            if any(keyword in col_lower for keyword in ['amount', 'amt']):
                amount_cols.append(col)
            
            # Look for credit columns (bank statements)
            elif any(keyword in col_lower for keyword in ['credit', 'deposit', 'credited', 'cr']):
                credit_cols.append(col)
            
            # Look for debit columns (bank statements)
            elif any(keyword in col_lower for keyword in ['debit', 'withdrawal', 'debited', 'dr', 'withdraw']):
                debit_cols.append(col)
            
            # Look for transaction type column (credit cards)
            elif any(keyword in col_lower for keyword in ['transaction_type', 'type', 'trans_type']):
                transaction_type_col = col
        
        summary = {
            'total_transactions': total_transactions,
            'full_data': df
        }
        
        # Calculate totals based on statement type
        if credit_cols and debit_cols:
            # BANK STATEMENT: Separate Credit/Debit columns
            credit_total = 0
            debit_total = 0
            
            for credit_col in credit_cols:
                df[credit_col] = pd.to_numeric(df[credit_col].astype(str).str.replace(r'[^\d.-]', '', regex=True), errors='coerce')
                credit_total += df[credit_col].fillna(0).sum()
            
            for debit_col in debit_cols:
                df[debit_col] = pd.to_numeric(df[debit_col].astype(str).str.replace(r'[^\d.-]', '', regex=True), errors='coerce')
                debit_total += df[debit_col].fillna(0).sum()
            
            summary.update({
                'total_credit': credit_total,
                'total_debit': debit_total,
                'net_amount': credit_total - debit_total,
                'statement_type': 'Bank Statement'
            })
            
        elif transaction_type_col and amount_cols:
            # CREDIT CARD: Transaction type column with amount
            main_amount_col = amount_cols[0]
            df[main_amount_col] = pd.to_numeric(df[main_amount_col].astype(str).str.replace(r'[^\d.-]', '', regex=True), errors='coerce')
            
            credit_mask = df[transaction_type_col].astype(str).str.lower().str.contains('credit|cr', na=False)
            debit_mask = df[transaction_type_col].astype(str).str.lower().str.contains('debit|dr', na=False)
            
            credit_total = df[credit_mask][main_amount_col].sum() if credit_mask.any() else 0
            debit_total = df[debit_mask][main_amount_col].sum() if debit_mask.any() else 0
            
            summary.update({
                'total_credit': credit_total,
                'total_debit': debit_total,
                'net_amount': credit_total - debit_total,
                'statement_type': 'Credit Card'
            })
            
        elif amount_cols:
            # FALLBACK: Single amount column, guess based on positive/negative
            main_amount_col = amount_cols[0]
            df[main_amount_col] = pd.to_numeric(df[main_amount_col].astype(str).str.replace(r'[^\d.-]', '', regex=True), errors='coerce')
            
            credit_total = df[df[main_amount_col] > 0][main_amount_col].sum()
            debit_total = abs(df[df[main_amount_col] < 0][main_amount_col].sum())
            
            summary.update({
                'total_credit': credit_total,
                'total_debit': debit_total,
                'net_amount': credit_total - debit_total,
                'statement_type': 'Unknown'
            })
        
        return summary
    except Exception as e:
        return {'error': str(e)}

def main():
    st.markdown('<div class="main-header">🏦 Bank Statement Processor</div>', unsafe_allow_html=True)
    
    if 'processing_stage' not in st.session_state:
        st.session_state.processing_stage = 0
    if 'temp_dir' not in st.session_state:
        st.session_state.temp_dir = None
    if 'processing_complete' not in st.session_state:
        st.session_state.processing_complete = False
    if 'current_file' not in st.session_state:
        st.session_state.current_file = None
    
    st.header("📄 Upload Bank Statement")
    
    uploaded_file = st.file_uploader(
        "Choose a PDF file", 
        type=['pdf'],
        help="Upload a bank or credit card statement in PDF format"
    )
    
    if uploaded_file is not None:
        current_file_name = uploaded_file.name
        if 'current_file' not in st.session_state or st.session_state.current_file != current_file_name:
            if st.session_state.temp_dir and os.path.exists(st.session_state.temp_dir):
                shutil.rmtree(st.session_state.temp_dir)
            
            st.session_state.processing_stage = 0
            st.session_state.temp_dir = tempfile.mkdtemp()
            st.session_state.processing_complete = False
            st.session_state.current_file = current_file_name
        
        temp_dir = st.session_state.temp_dir
        pdf_path = os.path.join(temp_dir, uploaded_file.name)
        
        with open(pdf_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        
        st.success(f"✅ Uploaded: {uploaded_file.name}")
        
        st.header("🔄 Processing Pipeline")
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            stage1_class = "stage-complete" if st.session_state.processing_stage > 1 else ("stage-processing" if st.session_state.processing_stage == 1 else "")
            st.markdown(f'<div class="stage-container {stage1_class}">', unsafe_allow_html=True)
            st.subheader("📸 PDF → Images")
            
            if st.session_state.processing_stage == 0:
                if st.button("🚀 Start Processing", key="start_btn"):
                    st.session_state.processing_stage = 1
                    st.rerun()
            elif st.session_state.processing_stage == 1:
                progress_bar.progress(0.1)
                status_text.text("Converting PDF to images...")
                
                images_dir = os.path.join(temp_dir, "images")
                os.makedirs(images_dir, exist_ok=True)
                
                with st.spinner("Processing..."):
                    success, output = run_script_with_progress(
                        "img.py", 
                        [temp_dir, images_dir],
                        "PDF to Images"
                    )
                
                if success:
                    st.success("✅ Complete")
                    progress_bar.progress(0.33)
                    st.session_state.processing_stage = 2
                    time.sleep(0.5)
                    st.rerun()
                else:
                    st.error("❌ Failed")
                    st.session_state.processing_stage = 0
            else:
                st.success("✅ Complete")
            
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col2:
            stage2_class = "stage-complete" if st.session_state.processing_stage > 2 else ("stage-processing" if st.session_state.processing_stage == 2 else "")
            st.markdown(f'<div class="stage-container {stage2_class}">', unsafe_allow_html=True)
            st.subheader("🔍 Images → Tables")
            
            if st.session_state.processing_stage == 2:
                progress_bar.progress(0.4)
                status_text.text("Extracting tables from images...")
                
                results_dir = os.path.join(temp_dir, "results")
                images_dir = os.path.join(temp_dir, "images")
                os.makedirs(results_dir, exist_ok=True)
                
                with st.spinner("Processing..."):
                    success, output = run_script_with_progress(
                        "nvidia.py",
                        [images_dir, results_dir],
                        "Image to Table Extraction"
                    )
                
                if success:
                    st.success("✅ Complete")
                    progress_bar.progress(0.66)
                    st.session_state.processing_stage = 3
                    time.sleep(0.5)
                    st.rerun()
                else:
                    st.error("❌ Failed")
                    st.session_state.processing_stage = 0
            elif st.session_state.processing_stage > 2:
                st.success("✅ Complete")
            else:
                st.info("⏳ Waiting...")
            
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col3:
            stage3_class = "stage-complete" if st.session_state.processing_stage > 3 else ("stage-processing" if st.session_state.processing_stage == 3 else "")
            st.markdown(f'<div class="stage-container {stage3_class}">', unsafe_allow_html=True)
            st.subheader("📊 Tables → CSV")
            
            if st.session_state.processing_stage == 3:
                progress_bar.progress(0.7)
                status_text.text("Converting tables to CSV...")
                
                results_dir = os.path.join(temp_dir, "results")
                csv_dir = os.path.join(temp_dir, "csv_output")
                os.makedirs(csv_dir, exist_ok=True)
                
                with st.spinner("Processing..."):
                    success, output = run_script_with_progress(
                        "llm.py",
                        [results_dir, csv_dir],
                        "Table to CSV Conversion"
                    )
                
                if success:
                    st.success("✅ Complete")
                    progress_bar.progress(1.0)
                    status_text.text("Processing completed successfully!")
                    st.session_state.processing_stage = 4
                    st.session_state.processing_complete = True
                    time.sleep(0.5)
                    st.rerun()
                else:
                    st.error("❌ Failed")
                    st.session_state.processing_stage = 0
            elif st.session_state.processing_stage > 3:
                st.success("✅ Complete")
                progress_bar.progress(1.0)
                status_text.text("All stages completed!")
            else:
                st.info("⏳ Waiting...")
            
            st.markdown('</div>', unsafe_allow_html=True)
    
    if st.session_state.processing_complete and st.session_state.temp_dir:
        st.markdown("---")
        st.header("📈 Results & Analytics")
        
        csv_dir = os.path.join(st.session_state.temp_dir, "csv_output")
        csv_files = [f for f in os.listdir(csv_dir) if f.endswith('.csv')]
        
        if csv_files:
            csv_path = os.path.join(csv_dir, csv_files[0])
            analysis = analyze_csv_file(csv_path)
            
            if 'error' not in analysis:
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                    st.metric("Total Transactions", analysis['total_transactions'])
                    st.markdown('</div>', unsafe_allow_html=True)
                
                with col2:
                    if 'total_credit' in analysis:
                        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                        st.metric("Total Credits", f"₹{analysis['total_credit']:,.2f}")
                        st.markdown('</div>', unsafe_allow_html=True)
                
                with col3:
                    if 'total_debit' in analysis:
                        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                        st.metric("Total Debits", f"₹{analysis['total_debit']:,.2f}")
                        st.markdown('</div>', unsafe_allow_html=True)
                
                with col4:
                    if 'net_amount' in analysis:
                        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                        st.metric("Net Amount", f"₹{analysis['net_amount']:,.2f}")
                        st.markdown('</div>', unsafe_allow_html=True)
                
                st.subheader("📋 Transaction Preview")
                st.dataframe(analysis['full_data'], use_container_width=True, height=400)
                
                st.subheader("📥 Download")
                with open(csv_path, 'rb') as f:
                    st.download_button(
                        label="📊 Download CSV File",
                        data=f.read(),
                        file_name=f"{uploaded_file.name.replace('.pdf', '')}_transactions.csv",
                        mime="text/csv",
                        type="primary"
                    )
            else:
                st.error(f"Error analyzing CSV: {analysis['error']}")
        else:
            st.warning("No CSV files found")

if __name__ == "__main__":
    main()