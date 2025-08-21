import streamlit as st
import os
import tempfile
import shutil
import pandas as pd
import numpy as np
from datetime import datetime
import subprocess
import sys
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
    .info-card {
        background-color: #e8f4fd;
        padding: 12px;
        border-radius: 6px;
        border-left: 4px solid #1f77b4;
        margin: 10px 0;
    }
    .warning-card {
        background-color: #fff3cd;
        padding: 12px;
        border-radius: 6px;
        border-left: 4px solid #ffc107;
        margin: 10px 0;
    }
    .error-card {
        background-color: #f8d7da;
        padding: 12px;
        border-radius: 6px;
        border-left: 4px solid #dc3545;
        margin: 10px 0;
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
        output_container = st.empty()
        
        for line in iter(process.stdout.readline, ''):
            clean_line = line.strip().encode('utf-8', errors='ignore').decode('utf-8', errors='ignore')
            if any(keyword in clean_line.lower() for keyword in ['processing', 'completed', 'error', 'failed', 'success']):
                key_messages.append(clean_line)
                output_container.text(clean_line)
        
        process.wait()
        return process.returncode == 0, key_messages
    except Exception as e:
        st.error(f"Error running {stage_name}: {str(e)}")
        return False, [str(e)]

def clean_amount_series(series):
    if series is None or len(series) == 0:
        return pd.Series([0])
    
    cleaned = series.astype(str)
    cleaned = cleaned.str.replace(r'[₹,\s$€£¥INRRsrs]', '', regex=True)
    cleaned = cleaned.str.replace(r'[^\d.-]', '', regex=True)
    cleaned = cleaned.replace(['nan', 'NaN', '', 'None', 'null'], '0')
    
    numeric_series = pd.to_numeric(cleaned, errors='coerce')
    numeric_series = numeric_series.fillna(0)
    
    return numeric_series

def analyze_csv_file(csv_path):
    try:
        df = pd.read_csv(csv_path)
        total_transactions = len(df)
        
        if total_transactions == 0:
            return {
                'error': 'CSV file is empty',
                'total_transactions': 0,
                'detection_method': 'Empty File',
                'column_schema': []
            }
        
        analysis_result = {
            'total_transactions': total_transactions,
            'full_data': df,
            'column_schema': list(df.columns),
            'detection_method': None,
            'primary_amount_column': None,
            'total_credit': 0,
            'total_debit': 0,
            'net_amount': 0
        }
        
        columns = list(df.columns)
        
        if len(columns) == 5 and columns == ['Date', 'Description', 'Amount_Credit', 'Amount_Debit', 'Balance']:
            credit_amounts = clean_amount_series(df['Amount_Credit'])
            debit_amounts = clean_amount_series(df['Amount_Debit'])
            
            credit_total = float(credit_amounts.sum())
            debit_total = float(debit_amounts.sum())
            
            analysis_result.update({
                'detection_method': 'Bank Statement (5-Column Schema)',
                'primary_amount_column': 'Amount_Credit & Amount_Debit',
                'total_credit': credit_total,
                'total_debit': debit_total,
                'net_amount': credit_total - debit_total,
                'statement_type': 'Bank Statement'
            })
            
        elif len(columns) == 4 and columns == ['Date', 'Description', 'Amount', 'Transaction_Type']:
            amounts = clean_amount_series(df['Amount'])
            
            credit_mask = df['Transaction_Type'].fillna('').astype(str) == 'Credit'
            debit_mask = df['Transaction_Type'].fillna('').astype(str) == 'Debit'
            
            credit_total = float(amounts[credit_mask].sum())
            debit_total = float(amounts[debit_mask].sum())
            
            analysis_result.update({
                'detection_method': 'Credit Card Statement (4-Column Schema)',
                'primary_amount_column': 'Amount',
                'total_credit': credit_total,
                'total_debit': debit_total,
                'net_amount': credit_total - debit_total,
                'statement_type': 'Credit Card'
            })
        
        else:
            columns_lower = [col.lower().strip() for col in df.columns]
            
            amount_columns = []
            for i, col_lower in enumerate(columns_lower):
                original_col = df.columns[i]
                if any(keyword in col_lower for keyword in ['amount', 'credit', 'debit', 'withdrawal', 'deposit', 'dr', 'cr']) and 'balance' not in col_lower:
                    try:
                        test_series = clean_amount_series(df[original_col])
                        if test_series.sum() > 0:
                            amount_columns.append(original_col)
                    except:
                        continue
            
            if len(amount_columns) == 2:
                first_col = amount_columns[0]
                second_col = amount_columns[1]
                
                first_amounts = clean_amount_series(df[first_col])
                second_amounts = clean_amount_series(df[second_col])
                
                first_total = float(first_amounts.sum())
                second_total = float(second_amounts.sum())
                
                analysis_result.update({
                    'detection_method': 'Legacy Bank Statement (2 Amount Columns)',
                    'primary_amount_column': f'{first_col} & {second_col}',
                    'total_credit': first_total,
                    'total_debit': second_total,
                    'net_amount': first_total - second_total,
                    'statement_type': 'Bank Statement',
                    'warning': 'Non-standard schema detected, amounts assigned by column order'
                })
                
            elif len(amount_columns) == 1:
                amount_col = amount_columns[0]
                amounts = clean_amount_series(df[amount_col])
                
                type_col = None
                for col in df.columns:
                    if 'type' in col.lower() or 'transaction' in col.lower():
                        type_col = col
                        break
                
                if type_col:
                    credit_mask = df[type_col].astype(str).str.contains('credit', case=False, na=False)
                    debit_mask = df[type_col].astype(str).str.contains('debit', case=False, na=False)
                    
                    credit_total = float(amounts[credit_mask].sum())
                    debit_total = float(amounts[debit_mask].sum())
                    
                    unclassified_mask = ~(credit_mask | debit_mask)
                    unclassified_total = float(amounts[unclassified_mask].sum())
                    
                    analysis_result.update({
                        'detection_method': 'Legacy Credit Card (Type Detection)',
                        'primary_amount_column': amount_col,
                        'total_credit': credit_total,
                        'total_debit': debit_total + unclassified_total,
                        'net_amount': credit_total - (debit_total + unclassified_total),
                        'statement_type': 'Credit Card',
                        'warning': f'Non-standard schema, {len(df[unclassified_mask])} unclassified transactions treated as debits'
                    })
                else:
                    total_amount = float(amounts.sum())
                    
                    analysis_result.update({
                        'detection_method': 'Single Amount Column (All Debits)',
                        'primary_amount_column': amount_col,
                        'total_credit': 0,
                        'total_debit': total_amount,
                        'net_amount': -total_amount,
                        'statement_type': 'Unknown',
                        'warning': 'All transactions treated as debits due to lack of transaction type information'
                    })
            else:
                numeric_columns = df.select_dtypes(include=[np.number]).columns.tolist()
                fallback_columns = [col for col in numeric_columns 
                                   if not any(keyword in col.lower() 
                                            for keyword in ['balance', 'total', 'running', 'outstanding'])]
                
                if fallback_columns:
                    fallback_col = fallback_columns[0]
                    amounts = clean_amount_series(df[fallback_col])
                    total_amount = float(amounts.sum())
                    
                    analysis_result.update({
                        'detection_method': 'Fallback Numeric Column',
                        'primary_amount_column': fallback_col,
                        'total_credit': 0,
                        'total_debit': total_amount,
                        'net_amount': -total_amount,
                        'statement_type': 'Fallback',
                        'warning': f'Used fallback column: {fallback_col}, schema not recognized'
                    })
                else:
                    analysis_result.update({
                        'detection_method': 'No Amount Columns Found',
                        'error': 'No suitable amount columns detected in the CSV file'
                    })
        
        return analysis_result
        
    except Exception as e:
        return {
            'error': f'Analysis failed: {str(e)}',
            'total_transactions': 0,
            'detection_method': 'Error',
            'column_schema': []
        }

def display_analysis_debug_info(analysis):
    st.subheader("Debug Information")
    
    col1, col2 = st.columns(2)
    with col1:
        st.write("**Detection Details**")
        st.write(f"- Method: {analysis.get('detection_method', 'Unknown')}")
        st.write(f"- Primary Column(s): {analysis.get('primary_amount_column', 'None')}")
        st.write(f"- Statement Type: {analysis.get('statement_type', 'Unknown')}")
    
    with col2:
        st.write("**Schema Information**")
        st.write(f"- Total Columns: {len(analysis.get('column_schema', []))}")
        st.write(f"- Column Names: {', '.join(analysis.get('column_schema', []))}")
    
    if 'error' in analysis:
        st.markdown(f'<div class="error-card"><strong>Error:</strong> {analysis["error"]}</div>', unsafe_allow_html=True)
    
    if 'warning' in analysis:
        st.markdown(f'<div class="warning-card"><strong>Warning:</strong> {analysis["warning"]}</div>', unsafe_allow_html=True)

def main():
    st.markdown('<div class="main-header">🏦 Enhanced Bank Statement Processor</div>', unsafe_allow_html=True)
    
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
                if st.button("🚀 Start Processing", key="start_btn", type="primary"):
                    st.session_state.processing_stage = 1
                    st.rerun()
            elif st.session_state.processing_stage == 1:
                progress_bar.progress(0.1)
                status_text.text("Converting PDF to images...")
                
                images_dir = os.path.join(temp_dir, "images")
                os.makedirs(images_dir, exist_ok=True)
                
                with st.spinner("Converting PDF pages to images..."):
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
                    st.error("❌ Failed - Check PDF file format")
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
                
                with st.spinner("Using OCR to extract table data..."):
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
                    st.error("❌ Failed - Check image quality")
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
                status_text.text("Converting tables to structured CSV...")
                
                results_dir = os.path.join(temp_dir, "results")
                csv_dir = os.path.join(temp_dir, "csv_output")
                os.makedirs(csv_dir, exist_ok=True)
                
                with st.spinner("Using AI to structure transaction data..."):
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
                    st.error("❌ Failed - Check table structure")
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
        csv_files = [f for f in os.listdir(csv_dir) if f.endswith('.csv')] if os.path.exists(csv_dir) else []
        
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
                    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                    st.metric("Total Credits", f"₹{analysis['total_credit']:,.2f}")
                    st.markdown('</div>', unsafe_allow_html=True)
                
                with col3:
                    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                    st.metric("Total Debits", f"₹{analysis['total_debit']:,.2f}")
                    st.markdown('</div>', unsafe_allow_html=True)
                
                with col4:
                    net_amount = analysis['net_amount']
                    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                    st.metric("Net Amount", f"₹{net_amount:,.2f}")
                    st.markdown('</div>', unsafe_allow_html=True)
                
                st.markdown(f'<div class="info-card"><strong>Analysis Method:</strong> {analysis.get("detection_method", "Unknown")}<br><strong>Primary Amount Column(s):</strong> {analysis.get("primary_amount_column", "None")}</div>', unsafe_allow_html=True)
                
                if 'warning' in analysis:
                    st.markdown(f'<div class="warning-card"><strong>⚠️ Warning:</strong> {analysis["warning"]}</div>', unsafe_allow_html=True)
                
                tab1, tab2, tab3 = st.tabs(["📋 Transaction Data", "📊 Summary Stats", "🔍 Debug Info"])
                
                with tab1:
                    st.subheader("Transaction Preview")
                    st.dataframe(analysis['full_data'], use_container_width=True, height=400)
                    
                    col1, col2 = st.columns([2, 1])
                    with col1:
                        st.subheader("📥 Download Results")
                    with col2:
                        with open(csv_path, 'rb') as f:
                            st.download_button(
                                label="📊 Download CSV File",
                                data=f.read(),
                                file_name=f"{uploaded_file.name.replace('.pdf', '')}_transactions.csv",
                                mime="text/csv",
                                type="primary"
                            )
                
                with tab2:
                    st.subheader("Summary Statistics")
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write("**Statement Information**")
                        st.write(f"- Statement Type: {analysis.get('statement_type', 'Unknown')}")
                        st.write(f"- Total Rows: {len(analysis['full_data'])}")
                        st.write(f"- Columns: {len(analysis['column_schema'])}")
                    
                    with col2:
                        st.write("**Amount Analysis**")
                        if analysis['total_credit'] > 0 and analysis['total_debit'] > 0:
                            ratio = analysis['total_credit'] / analysis['total_debit']
                            st.write(f"- Credit/Debit Ratio: {ratio:.2f}")
                        avg_transaction = (analysis['total_credit'] + analysis['total_debit']) / max(analysis['total_transactions'], 1)
                        st.write(f"- Average Transaction: ₹{avg_transaction:,.2f}")
                
                with tab3:
                    display_analysis_debug_info(analysis)
                    
            else:
                st.markdown(f'<div class="error-card"><strong>Analysis Error:</strong> {analysis["error"]}</div>', unsafe_allow_html=True)
        else:
            st.warning("⚠️ No CSV files found. Processing may have failed.")
            
            if os.path.exists(csv_dir):
                all_files = os.listdir(csv_dir)
                if all_files:
                    st.write("Files found in output directory:")
                    for file in all_files:
                        st.write(f"- {file}")

    if st.session_state.processing_complete:
        st.markdown("---")
        if st.button("🔄 Process Another Statement", type="secondary"):
            if st.session_state.temp_dir and os.path.exists(st.session_state.temp_dir):
                shutil.rmtree(st.session_state.temp_dir)
            
            for key in ['processing_stage', 'temp_dir', 'processing_complete', 'current_file']:
                if key in st.session_state:
                    del st.session_state[key]
            
            st.rerun()

if __name__ == "__main__":
    main()