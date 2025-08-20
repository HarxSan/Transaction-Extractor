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

def analyze_csv_file(csv_path):
    try:
        df = pd.read_csv(csv_path)
        
        total_transactions = len(df)
        
        amount_cols = []
        transaction_type_col = None
        date_col = None
        
        for col in df.columns:
            if any(keyword in col.lower() for keyword in ['amount', 'amt']):
                amount_cols.append(col)
            elif any(keyword in col.lower() for keyword in ['type', 'credit', 'debit', 'cr', 'dr']):
                transaction_type_col = col
            elif any(keyword in col.lower() for keyword in ['date', 'dt']):
                date_col = col
        
        summary = {
            'total_transactions': total_transactions,
            'columns': list(df.columns),
            'sample_data': df.head(10) if len(df) > 0 else pd.DataFrame(),
            'full_data': df,
            'amount_columns': amount_cols,
            'date_column': date_col
        }
        
        if amount_cols:
            main_amount_col = amount_cols[0]
            df[main_amount_col] = pd.to_numeric(df[main_amount_col].astype(str).str.replace(r'[^\d.-]', '', regex=True), errors='coerce')
            
            if transaction_type_col:
                credit_mask = df[transaction_type_col].astype(str).str.lower().str.contains('credit|cr', na=False)
                debit_mask = df[transaction_type_col].astype(str).str.lower().str.contains('debit|dr', na=False)
                
                credit_total = df[credit_mask][main_amount_col].sum() if credit_mask.any() else 0
                debit_total = df[debit_mask][main_amount_col].sum() if debit_mask.any() else 0
            else:
                credit_total = df[df[main_amount_col] > 0][main_amount_col].sum()
                debit_total = abs(df[df[main_amount_col] < 0][main_amount_col].sum())
            
            summary.update({
                'main_amount_column': main_amount_col,
                'transaction_type_column': transaction_type_col,
                'total_credit': credit_total,
                'total_debit': debit_total,
                'net_amount': credit_total - debit_total
            })
            
            last_cols_analysis = {}
            numeric_cols = df.select_dtypes(include=[float, int]).columns.tolist()
            
            for i in range(1, min(4, len(numeric_cols) + 1)):
                if i <= len(numeric_cols):
                    col_name = numeric_cols[-i]
                    col_sum = df[col_name].sum()
                    col_count = df[col_name].count()
                    last_cols_analysis[f'last_{i}_col'] = {
                        'name': col_name,
                        'sum': col_sum,
                        'count': col_count
                    }
            
            summary['last_columns_analysis'] = last_cols_analysis
        
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
    
    st.header("📄 Upload Bank Statement")
    uploaded_file = st.file_uploader(
        "Choose a PDF file", 
        type=['pdf'],
        help="Upload a bank or credit card statement in PDF format"
    )
    
    if uploaded_file is not None:
        if st.session_state.temp_dir is None:
            st.session_state.temp_dir = tempfile.mkdtemp()
        
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
                
                if 'last_columns_analysis' in analysis and analysis['last_columns_analysis']:
                    st.subheader("📊 Last Column Analysis")
                    
                    cols = st.columns(len(analysis['last_columns_analysis']))
                    for i, (key, data) in enumerate(analysis['last_columns_analysis'].items()):
                        with cols[i]:
                            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                            st.write(f"**{data['name']}**")
                            st.metric("Sum", f"₹{data['sum']:,.2f}")
                            st.metric("Count", data['count'])
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
    
    if st.session_state.temp_dir:
        st.markdown("---")
        if st.button("🔄 Process New Statement", type="secondary"):
            if os.path.exists(st.session_state.temp_dir):
                shutil.rmtree(st.session_state.temp_dir)
            
            st.session_state.processing_stage = 0
            st.session_state.temp_dir = None
            st.session_state.processing_complete = False
            st.rerun()

if __name__ == "__main__":
    main()