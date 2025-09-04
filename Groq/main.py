import streamlit as st
import os
import tempfile
import shutil
import asyncio
import pandas as pd
import logging
import hashlib
import time
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass
from contextlib import contextmanager
from pathlib import Path
from dotenv import load_dotenv

from pdf_processor import PDFProcessor
from image_converter import ImageConverter
from markdown_processor import MarkdownProcessor
from gemini_extractor import GeminiExtractor
from ui_components import UIComponents
from config import DEFAULT_BATCH_SIZE, DEFAULT_DPI

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv(override=True)

@dataclass
class ProcessingState:
    processing_complete: bool = False
    df: Optional[pd.DataFrame] = None
    uploaded_file_name: Optional[str] = None
    temp_dirs: List[str] = None
    redacted_images: List[bytes] = None
    current_file_id: Optional[str] = None
    processed_images: List[Dict[str, Any]] = None
    processing_stopped: bool = False
    processing_started: bool = False
    file_processed: bool = False
    pdf_password: Optional[str] = None
    password_verified: bool = False
    password_needed: bool = False
    password_input_key: int = 0
    password_input_value: str = ''
    
    def __post_init__(self):
        if self.temp_dirs is None:
            self.temp_dirs = []
        if self.redacted_images is None:
            self.redacted_images = []
        if self.processed_images is None:
            self.processed_images = []

@contextmanager
def managed_temp_dir():
    temp_dir = None
    try:
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
    finally:
        if temp_dir and os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
            except Exception as e:
                logger.error(f"Failed to cleanup temp directory: {e}")

def cleanup_temp_files(temp_dirs: List[str]) -> None:
    for temp_dir in temp_dirs:
        if temp_dir and os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
            except Exception as e:
                logger.error(f"Failed to cleanup {temp_dir}: {e}")

def initialize_session_state() -> None:
    if 'processing_state' not in st.session_state:
        st.session_state.processing_state = ProcessingState()

def reset_session() -> None:
    if hasattr(st.session_state, 'processing_state'):
        state = st.session_state.processing_state
        cleanup_temp_files(state.temp_dirs)
        st.session_state.processing_state = ProcessingState()

def get_state() -> ProcessingState:
    return st.session_state.processing_state

def validate_file(uploaded_file) -> Tuple[bool, str]:
    if not uploaded_file:
        return False, "No file uploaded"
    
    if uploaded_file.size > 50 * 1024 * 1024:
        return False, "File size exceeds 50MB limit"
    
    if not uploaded_file.name.lower().endswith('.pdf'):
        return False, "Only PDF files are allowed"
    
    return True, "File is valid"

def calculate_metrics(df: pd.DataFrame) -> Dict[str, Any]:
    if df is None or df.empty:
        return {}
    
    try:
        debit_sum = df[df['Type'] == 'Debit']['Amount'].sum()
        credit_sum = df[df['Type'] == 'Credit']['Amount'].sum()
        total_transactions = len(df)
        debit_count = len(df[df['Type'] == 'Debit'])
        credit_count = len(df[df['Type'] == 'Credit'])
        
        return {
            'debit_sum': debit_sum,
            'credit_sum': credit_sum,
            'total_transactions': total_transactions,
            'debit_count': debit_count,
            'credit_count': credit_count
        }
    except Exception as e:
        logger.error(f"Error calculating metrics: {e}")
        return {}

def sort_transactions(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    
    try:
        df_sorted = df.copy()
        df_sorted['date_parsed'] = pd.to_datetime(
            df_sorted['Date'], 
            dayfirst=True, 
            errors='coerce'
        )
        df_sorted = df_sorted.sort_values(['date_parsed'], ascending=[True])
        return df_sorted.drop('date_parsed', axis=1).reset_index(drop=True)
    except Exception:
        return df.reset_index(drop=True)

def display_results() -> None:
    state = get_state()
    if state.df is None:
        return
    
    metrics = calculate_metrics(state.df)
    if not metrics:
        st.error("Unable to calculate transaction metrics")
        return
    
    st.markdown(UIComponents.render_section_header("📊 Transaction Analysis"), unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown(
            UIComponents.render_metric_card(
                "💸 Total Debits", 
                metrics['debit_sum'], 
                metrics['debit_count'], 
                "#e74c3c"
            ), 
            unsafe_allow_html=True
        )
    
    with col2:
        st.markdown(
            UIComponents.render_metric_card(
                "💰 Total Credits", 
                metrics['credit_sum'], 
                metrics['credit_count'], 
                "#27ae60"
            ), 
            unsafe_allow_html=True
        )
    
    with col3:
        st.markdown(f"""
        <div class="metric-card">
            <h4>📋 Total Transactions</h4>
            <h2 style="color: #667eea;">{metrics['total_transactions']}</h2>
            <span class="transaction-count">All records</span>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown(UIComponents.render_section_header("📋 Full Transaction Preview"), unsafe_allow_html=True)
    
    display_df = sort_transactions(state.df)
    
    st.dataframe(
        display_df.style.format({'Amount': '₹{:,.2f}'}),
        use_container_width=True,
        height=450
    )
    
    csv_data = display_df.to_csv(index=False)
    filename = f"transactions_{Path(state.uploaded_file_name).stem}.csv"
    
    st.download_button(
        "📥 Download CSV",
        csv_data,
        filename,
        "text/csv",
        use_container_width=True,
        key="download_csv"
    )

class DynamicMarkdownProcessor(MarkdownProcessor):
    def __init__(self, api_key: str, batch_size: int, preview_container):
        super().__init__(api_key, batch_size)
        self.preview_container = preview_container
        self.processing_state = get_state()
        
    async def process_all_images_with_preview(self, image_paths: List[str]) -> Optional[str]:
        if not image_paths:
            return None
        
        all_markdown = []
        self.processing_state.processed_images = []
        self.processing_state.processing_stopped = False
        
        for i, image_path in enumerate(image_paths):
            if not os.path.exists(image_path):
                continue
            
            self.update_preview(image_paths, i, "processing")
            
            try:
                result, has_transactions = await self.convert_images_to_markdown([image_path])
                
                if result and result.strip():
                    all_markdown.append(result)
                    self.processing_state.processed_images.append({
                        'index': i,
                        'status': 'completed',
                        'has_transactions': has_transactions
                    })
                    
                    self.update_preview(image_paths, i, "completed")
                    
                    if not has_transactions:
                        self.processing_state.processing_stopped = True
                        self.update_preview(image_paths, i, "stopped")
                        break
                else:
                    self.processing_state.processing_stopped = True
                    self.update_preview(image_paths, i, "error")
                    break
                    
            except Exception as e:
                logger.error(f"Error processing page {i+1}: {e}")
                self.processing_state.processing_stopped = True
                self.update_preview(image_paths, i, "error")
                break
        
        return "\n\n---\n\n".join(all_markdown) if all_markdown else None
    
    def update_preview(self, image_paths: List[str], current_index: int, status: str) -> None:
        try:
            with self.preview_container.container():
                st.markdown(UIComponents.render_preview_header(), unsafe_allow_html=True)
                
                status_messages = {
                    "processing": "🔄 Processing...",
                    "completed": "✅ Processed",
                    "stopped": "⚠️ No transactions found - Stopping",
                    "error": "❌ Error"
                }
                
                for i, image_path in enumerate(image_paths):
                    if i > current_index:
                        break
                        
                    if os.path.exists(image_path):
                        if i == current_index:
                            caption = f"Page {i + 1} - {status_messages.get(status, 'Unknown')}"
                        else:
                            caption = f"Page {i + 1} - ✅ Processed"
                        
                        st.image(image_path, caption=caption, use_container_width=True)
                
                if self.processing_state.processing_stopped and current_index < len(image_paths) - 1:
                    remaining = len(image_paths) - current_index - 1
                    st.markdown(f"""
                    <div style="text-align: center; padding: 1rem; color: #f39c12; 
                                background-color: #fef9e7; border-radius: 8px; margin: 1rem 0;">
                        <strong>⚠️ Processing Stopped</strong><br>
                        {remaining} remaining pages skipped (no transactions detected)
                    </div>
                    """, unsafe_allow_html=True)
                    
        except Exception as e:
            logger.error(f"Error updating preview: {e}")

def check_pdf_password(uploaded_file) -> bool:
    try:
        with managed_temp_dir() as temp_dir:
            pdf_path = os.path.join(temp_dir, "temp_check.pdf")
            with open(pdf_path, "wb") as f:
                f.write(uploaded_file.getbuffer())
            
            return not PDFProcessor.authenticate_pdf(pdf_path)
    except Exception as e:
        logger.error(f"Error checking PDF password: {e}")
        return False

async def process_pdf_file(uploaded_file, groq_api_key: str, gemini_api_key: str, preview_container) -> bool:
    state = get_state()
    
    try:
        with managed_temp_dir() as temp_dir:
            state.temp_dirs.append(temp_dir)
            
            pdf_path = os.path.join(temp_dir, "input.pdf")
            with open(pdf_path, "wb") as f:
                f.write(uploaded_file.getbuffer())
            
            progress_container = st.container()
            
            with progress_container:
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                status_text.markdown("🔧 **Redacting sensitive information...**")
                progress_bar.progress(15)
                
                redacted_path = os.path.join(temp_dir, "redacted.pdf")
                redacted = PDFProcessor.redact_pdf(pdf_path, redacted_path, state.pdf_password)
                
                if not redacted:
                    st.error("⚠️ No transaction table detected in PDF")
                    state.processing_started = False
                    return False
                
                status_text.markdown("📄 **Extracting text from PDF...**")
                progress_bar.progress(20)
                
                extracted_text = PDFProcessor.extract_text_from_pdf(redacted_path, state.pdf_password)
                
                if not extracted_text or not extracted_text.strip():
                    st.error("❌ Failed to extract text from PDF")
                    state.processing_started = False
                    return False
                
                status_text.markdown("🖼️ **Converting to high-resolution images...**")
                progress_bar.progress(30)
                
                img_dir = os.path.join(temp_dir, "images")
                os.makedirs(img_dir, exist_ok=True)
                image_paths = ImageConverter.convert_pdf_to_images(
                    redacted_path, img_dir, DEFAULT_DPI, state.pdf_password
                )
                
                if not image_paths:
                    st.error("❌ Failed to convert PDF to images")
                    state.processing_started = False
                    return False
                
                status_text.markdown("🔍 **Converting images to markdown...**")
                progress_bar.progress(45)
                
                dynamic_processor = DynamicMarkdownProcessor(groq_api_key, DEFAULT_BATCH_SIZE, preview_container)
                
                markdown_content = await dynamic_processor.process_all_images_with_preview(image_paths)
                
                if not markdown_content or not markdown_content.strip():
                    st.error("❌ Failed to generate markdown content")
                    state.processing_started = False
                    return False
                
                status_text.markdown("🤖 **Extracting transactions with Gemini...**")
                progress_bar.progress(70)
                
                gemini_extractor = GeminiExtractor(gemini_api_key)
                
                gemini_result = await gemini_extractor.extract_transactions_from_markdown(
                    markdown_content, extracted_text
                )
                
                if not gemini_result:
                    st.error("❌ No response from Gemini")
                    state.processing_started = False
                    return False
                
                transactions = gemini_extractor.process_gemini_result(gemini_result)
                
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
                    
                    state.df = df
                    state.uploaded_file_name = uploaded_file.name
                    
                    try:
                        import fitz
                        doc = fitz.open(redacted_path)
                        if doc.needs_pass and state.pdf_password:
                            doc.authenticate(state.pdf_password)
                        
                        redacted_images = []
                        for page_num in range(len(doc)):
                            page = doc.load_page(page_num)
                            pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
                            img_data = pix.tobytes("png")
                            redacted_images.append(img_data)
                        doc.close()
                        
                        state.redacted_images = redacted_images
                        
                    except Exception as e:
                        logger.error(f"Error generating redacted images: {e}")
                    
                    state.processing_complete = True
                    state.processing_started = False
                    
                    progress_bar.progress(100)
                    status_text.markdown("✅ **Processing complete!**")
                    
                    st.success("🎉 Transactions extracted successfully!")
                    st.balloons()
                    
                    return True
                else:
                    progress_bar.progress(100)
                    status_text.markdown("❌ **No transactions found**")
                    st.error("No valid transactions could be extracted from this PDF.")
                    state.processing_started = False
                    return False
    
    except Exception as e:
        logger.error(f"Error processing PDF: {e}")
        st.error(f"Error processing PDF: {str(e)}")
        state.processing_started = False
        return False

def main():
    st.set_page_config(
        page_title="Credit Card Transaction Extractor",
        page_icon="💳",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    initialize_session_state()
    state = get_state()
    
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
            st.markdown(
                '<div class="feature-item"><strong>Setup:</strong><br>'
                '• Enter both API keys above<br>'
                '• Or set GROQ_API_KEY and GEMINI_API_KEY in .env file</div>', 
                unsafe_allow_html=True
            )
            st.stop()
        else:
            st.success("✅ Both API keys loaded")
        
        st.markdown("---")
        st.markdown("**🚀 Features**")
        
        for feature in UIComponents.get_features():
            st.markdown(f'<div class="feature-item">{feature}</div>', unsafe_allow_html=True)
        
        if state.processing_complete:
            if st.button("🔄 Process New Document", use_container_width=True, key="new_doc_btn"):
                reset_session()
                st.rerun()
    
    st.markdown(UIComponents.render_security_note(), unsafe_allow_html=True)
    
    uploaded_file = st.file_uploader(
        "📄 Choose PDF file", 
        type="pdf",
        help="Upload your credit card statement PDF",
        key="file_uploader"
    )
    
    if uploaded_file:
        is_valid, validation_message = validate_file(uploaded_file)
        if not is_valid:
            st.error(f"❌ {validation_message}")
            return
        
        current_file_id = f"{uploaded_file.name}_{uploaded_file.size}"
        
        if state.current_file_id != current_file_id:
            reset_session()
            state = get_state()
            state.current_file_id = current_file_id
            state.password_needed = check_pdf_password(uploaded_file)
        
        if state.password_needed and not state.password_verified:
            st.markdown(UIComponents.render_status("🔒 PDF is password protected", "warning"), unsafe_allow_html=True)
            
            password_form = st.form(key="password_form")
            with password_form:
                password_input = st.text_input(
                    "🔓 Enter PDF Password", 
                    type="password", 
                    key=f"pdf_password_{state.password_input_key}",
                    value=state.password_input_value
                )
                
                if st.form_submit_button("Verify Password", use_container_width=True):
                    with managed_temp_dir() as temp_dir:
                        try:
                            pdf_path = os.path.join(temp_dir, "temp_auth.pdf")
                            with open(pdf_path, "wb") as f:
                                f.write(uploaded_file.getbuffer())
                            
                            if PDFProcessor.authenticate_pdf(pdf_path, password_input):
                                state.pdf_password = password_input
                                state.password_verified = True
                                state.password_input_value = password_input
                                st.rerun()
                            else:
                                st.error("❌ Incorrect password")
                        except Exception as e:
                            st.error("❌ Error verifying password")
        
        if state.processing_complete:
            col1, col2 = st.columns([1.3, 0.7], gap="large")
            
            with col1:
                display_results()
            
            with col2:
                st.markdown(UIComponents.render_preview_header(), unsafe_allow_html=True)
                
                if state.redacted_images:
                    for i, img_data in enumerate(state.redacted_images):
                        st.image(
                            img_data,
                            caption=f"Page {i + 1} (processed)",
                            use_container_width=True
                        )
                else:
                    st.markdown("""
                    <div style="text-align: center; padding: 2rem; color: #667eea;">
                        <h4>No preview available</h4>
                    </div>
                    """, unsafe_allow_html=True)
        
        elif not state.processing_started and (not state.password_needed or state.password_verified):
            col1, col2 = st.columns([1.3, 0.7], gap="large")
            
            with col2:
                preview_container = st.empty()
                with preview_container.container():
                    st.markdown(UIComponents.render_preview_header(), unsafe_allow_html=True)
                    st.markdown("""
                    <div style="text-align: center; padding: 2rem; color: #667eea;">
                        <h4>👆 Click 'Extract Transactions'</h4>
                        <p>Document preview will appear here</p>
                    </div>
                    """, unsafe_allow_html=True)
            
            with col1:
                st.markdown(UIComponents.render_process_card_header(), unsafe_allow_html=True)
                
                if st.button("🚀 Extract Transactions", type="primary", use_container_width=True, key="extract_btn"):
                    state.processing_started = True
                    try:
                        success = asyncio.run(process_pdf_file(uploaded_file, groq_api_key, gemini_api_key, preview_container))
                        if success:
                            st.rerun()
                    except Exception as e:
                        logger.error(f"Error in main processing: {e}")
                        st.error("An error occurred during processing")
                        state.processing_started = False

if __name__ == "__main__":
    main()