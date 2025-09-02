import streamlit as st
import os
import tempfile
import shutil
import asyncio
import pandas as pd
from dotenv import load_dotenv

from pdf_processor import PDFProcessor
from image_converter import ImageConverter
from markdown_processor import MarkdownProcessor
from gemini_extractor import GeminiExtractor
from ui_components import UIComponents
from config import DEFAULT_BATCH_SIZE, DEFAULT_DPI

load_dotenv(override=True)

def cleanup_temp_files(temp_dirs):
    for temp_dir in temp_dirs:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

def main():
    st.set_page_config(
        page_title="Credit Card Transaction Extractor",
        page_icon="💳",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    if 'temp_dirs' not in st.session_state:
        st.session_state.temp_dirs = []
    
    st.markdown(UIComponents.load_css(), unsafe_allow_html=True)
    st.markdown(UIComponents.render_header(), unsafe_allow_html=True)
    
    with st.sidebar:
        st.markdown(UIComponents.render_sidebar_header(), unsafe_allow_html=True)
        
        groq_api_key = os.getenv("GROQ_API_KEY")
        if not groq_api_key:
            groq_api_key = st.text_input(
                "🔑 Groq API Key", 
                type="password", 
                help="Enter your Groq API key",
                placeholder="Enter your API key..."
            )
        
        gemini_api_key = os.getenv("GEMINI_API_KEY")
        if not gemini_api_key:
            gemini_api_key = st.text_input(
                "🔑 Gemini API Key", 
                type="password", 
                help="Enter your Gemini API key",
                placeholder="Enter your API key..."
            )
            
        if not groq_api_key or not gemini_api_key:
            st.error("⚠️ Both API keys required to proceed")
            st.markdown('<div class="feature-item"><strong>Setup:</strong><br>• Enter both API keys above<br>• Or set GROQ_API_KEY and GEMINI_API_KEY in .env file</div>', unsafe_allow_html=True)
            st.stop()
        else:
            st.success("✅ Both API keys loaded")
        
        st.markdown("---")
        st.markdown("**🚀 Features**")
        
        for feature in UIComponents.get_features():
            st.markdown(f'<div class="feature-item">{feature}</div>', unsafe_allow_html=True)
    
    st.markdown(UIComponents.render_security_note(), unsafe_allow_html=True)
    
    uploaded_file = st.file_uploader(
        "📄 Choose PDF file", 
        type="pdf",
        help="Upload your credit card statement PDF"
    )
    
    if uploaded_file:
        temp_dir = tempfile.mkdtemp()
        st.session_state.temp_dirs.append(temp_dir)
        
        try:
            pdf_path = os.path.join(temp_dir, "input.pdf")
            with open(pdf_path, "wb") as f:
                f.write(uploaded_file.read())
            
            password = None
            if not PDFProcessor.authenticate_pdf(pdf_path):
                st.markdown(UIComponents.render_status("🔒 PDF is password protected", "warning"), unsafe_allow_html=True)
                password = st.text_input("🔐 Enter PDF Password", type="password")
                if password:
                    if PDFProcessor.authenticate_pdf(pdf_path, password):
                        st.markdown(UIComponents.render_status("✅ PDF unlocked successfully", "success"), unsafe_allow_html=True)
                    else:
                        st.error("❌ Incorrect password")
                        return
                else:
                    st.info("Please enter the password to continue.")
                    return
            
            col1, col2 = st.columns([1.3, 0.7], gap="large")
            
            with col1:
                st.markdown(UIComponents.render_process_card_header(), unsafe_allow_html=True)
                
                if st.button("🚀 Extract Transactions", type="primary", use_container_width=True):
                    progress_container = st.container()
                    
                    with progress_container:
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        
                        status_text.markdown("🔧 **Redacting sensitive information...**")
                        progress_bar.progress(15)
                        
                        redacted_path = os.path.join(temp_dir, "redacted.pdf")
                        redacted = PDFProcessor.redact_pdf(pdf_path, redacted_path, password)
                        
                        if not redacted:
                            st.error("⚠️ No transaction table detected in PDF")
                            return
                        
                        status_text.markdown("🖼️ **Converting to high-resolution images...**")
                        progress_bar.progress(25)
                        
                        img_dir = os.path.join(temp_dir, "images")
                        os.makedirs(img_dir, exist_ok=True)
                        image_paths = ImageConverter.convert_pdf_to_images(redacted_path, img_dir, DEFAULT_DPI, password)
                        
                        status_text.markdown("📝 **Step 1: Converting images to markdown...**")
                        progress_bar.progress(40)
                        
                        markdown_processor = MarkdownProcessor(groq_api_key, DEFAULT_BATCH_SIZE)
                        
                        try:
                            markdown_content = asyncio.run(markdown_processor.process_all_images(image_paths))
                            
                            if not markdown_content or not markdown_content.strip():
                                st.error("❌ Step 1 failed: No markdown content generated")
                                return
                            
                            status_text.markdown("🤖 **Step 2: Extracting transactions with Gemini...**")
                            progress_bar.progress(65)
                            
                            gemini_extractor = GeminiExtractor(gemini_api_key)
                            
                            gemini_result = asyncio.run(gemini_extractor.extract_transactions_from_markdown(markdown_content))
                            
                            if not gemini_result:
                                st.error("❌ Step 2 failed: No response from Gemini")
                                return
                            
                            transactions = gemini_extractor.process_gemini_result(gemini_result)
                        
                        except Exception as e:
                            st.error(f"🚨 AI Processing Error: {str(e)}")
                            return
                        
                        progress_bar.progress(85)
                        status_text.markdown("📊 **Finalizing analysis...**")
                        
                        if transactions and len(transactions) > 0:
                            df = pd.DataFrame(transactions)
                            
                            df.rename(columns={
                                'date': 'Date',
                                'description': 'Description', 
                                'amount': 'Amount',
                                'type': 'Type'
                            }, inplace=True)
                            
                            st.session_state.df = df
                            st.session_state.uploaded_file_name = uploaded_file.name
                            
                            progress_bar.progress(100)
                            status_text.markdown("✅ **Processing complete!**")
                            
                            st.markdown(UIComponents.render_section_header("📊 Transaction Analysis"), unsafe_allow_html=True)
                            
                            debit_sum = df[df['Type'] == 'Debit']['Amount'].sum()
                            credit_sum = df[df['Type'] == 'Credit']['Amount'].sum()
                            total_transactions = len(df)
                            debit_count = len(df[df['Type'] == 'Debit'])
                            credit_count = len(df[df['Type'] == 'Credit'])
                            
                            col_m1, col_m2, col_m3 = st.columns(3)
                            
                            with col_m1:
                                st.markdown(UIComponents.render_metric_card("💸 Total Debits", debit_sum, debit_count, "#e74c3c"), unsafe_allow_html=True)
                            
                            with col_m2:
                                st.markdown(UIComponents.render_metric_card("💰 Total Credits", credit_sum, credit_count, "#27ae60"), unsafe_allow_html=True)
                            
                            with col_m3:
                                st.markdown(f"""
                                <div class="metric-card">
                                    <h4>📋 Total Transactions</h4>
                                    <h2 style="color: #667eea;">{total_transactions}</h2>
                                    <span class="transaction-count">All records</span>
                                </div>
                                """, unsafe_allow_html=True)
                            
                            st.markdown(UIComponents.render_section_header("📋 Full Transaction Preview"), unsafe_allow_html=True)
                            
                            try:
                                df['date_parsed'] = pd.to_datetime(df['Date'], dayfirst=True, errors='coerce')
                                df = df.sort_values(['date_parsed'], ascending=[True])
                                display_df = df.drop('date_parsed', axis=1).reset_index(drop=True)
                            except Exception as e:
                                display_df = df.reset_index(drop=True)
                            
                            st.dataframe(
                                display_df.style.format({'Amount': '₹{:,.2f}'}),
                                use_container_width=True,
                                height=450
                            )
                            
                            csv_data = display_df.to_csv(index=False)
                            
                            st.download_button(
                                "📥 Download CSV",
                                csv_data,
                                f"transactions_{uploaded_file.name.replace('.pdf', '')}.csv",
                                "text/csv",
                                use_container_width=True
                            )
                        else:
                            progress_bar.progress(100)
                            status_text.markdown("❌ **No transactions found**")
                            st.error("No valid transactions could be extracted from this PDF.")
                
                st.markdown("</div>", unsafe_allow_html=True)
            
            with col2:
                st.markdown(UIComponents.render_preview_header(), unsafe_allow_html=True)
                
                redacted_path = os.path.join(temp_dir, "redacted.pdf")
                if os.path.exists(redacted_path):
                    import fitz
                    doc = fitz.open(redacted_path)
                    if doc.needs_pass and password:
                        doc.authenticate(password)
                    
                    for page_num in range(len(doc)):
                        page = doc.load_page(page_num)
                        pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
                        img_data = pix.tobytes("png")
                        st.image(
                            img_data, 
                            caption=f"Page {page_num + 1} (processed)", 
                            use_container_width=True
                        )
                    doc.close()
                else:
                    st.markdown("""
                    <div style="text-align: center; padding: 2rem; color: #667eea;">
                        <h4>👆 Click 'Extract Transactions'</h4>
                        <p>Document preview will appear here</p>
                    </div>
                    """, unsafe_allow_html=True)
                
                st.markdown("</div>", unsafe_allow_html=True)
        
        except Exception as e:
            st.error(f"Error processing PDF: {str(e)}")
        
        finally:
            cleanup_temp_files(st.session_state.temp_dirs)
            st.session_state.temp_dirs.clear()

if __name__ == "__main__":
    main()