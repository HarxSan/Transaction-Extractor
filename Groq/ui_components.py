class UIComponents:
    @staticmethod
    def load_css():
        return """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
        
        .main-header {
            text-align: center;
            padding: 2rem 0;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border-radius: 16px;
            margin-bottom: 2rem;
            box-shadow: 0 8px 32px rgba(102, 126, 234, 0.3);
        }
        .main-header h1 {
            font-family: 'Inter', sans-serif;
            font-size: 2.5rem;
            margin: 0;
            font-weight: 700;
        }
        .main-header p {
            font-family: 'Inter', sans-serif;
            font-size: 1.1rem;
            opacity: 0.9;
            margin: 0.5rem 0 0 0;
            font-weight: 400;
        }
        
        .security-note {
            background: rgba(40, 167, 69, 0.1);
            color: #155724;
            padding: 1rem;
            border-radius: 8px;
            border-left: 4px solid #28a745;
            margin: 1rem 0 2rem 0;
            text-align: center;
            font-weight: 500;
            font-family: 'Inter', sans-serif;
        }
        
        .metric-card {
            background: linear-gradient(135deg, rgba(102, 126, 234, 0.1) 0%, rgba(118, 75, 162, 0.1) 100%);
            padding: 1.5rem;
            border-radius: 12px;
            border: 1px solid rgba(102, 126, 234, 0.2);
            margin: 1rem 0;
            text-align: center;
            transition: transform 0.2s ease;
        }
        .metric-card:hover {
            transform: translateY(-2px);
        }
        .metric-card h4 {
            color: #667eea;
            font-weight: 600;
            margin-bottom: 0.5rem;
            font-size: 1rem;
            font-family: 'Inter', sans-serif;
        }
        .metric-card h2 {
            margin: 0;
            font-weight: 700;
            font-size: 1.8rem;
            font-family: 'Inter', sans-serif;
        }
        
        .process-card {
            background: rgba(255, 255, 255, 0.05);
            border: 1px solid rgba(102, 126, 234, 0.2);
            border-radius: 16px;
            padding: 2rem;
            margin: 1.5rem 0;
        }
        
        .section-header {
            background: linear-gradient(135deg, rgba(102, 126, 234, 0.1) 0%, rgba(118, 75, 162, 0.1) 100%);
            padding: 1rem;
            border-radius: 8px;
            border-left: 4px solid #667eea;
            margin: 1.5rem 0 1rem 0;
        }
        .section-header h3 {
            color: #667eea;
            margin: 0;
            font-weight: 600;
            font-family: 'Inter', sans-serif;
            font-size: 1.2rem;
        }
        
        .sidebar-header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 1rem;
            border-radius: 8px;
            margin-bottom: 1.5rem;
            text-align: center;
        }
        .sidebar-header h3 {
            margin: 0;
            font-family: 'Inter', sans-serif;
            font-weight: 600;
        }
        
        .feature-item {
            background: rgba(102, 126, 234, 0.05);
            padding: 0.6rem 0.8rem;
            border-radius: 6px;
            margin: 0.4rem 0;
            border-left: 3px solid #667eea;
            font-family: 'Inter', sans-serif;
            font-size: 0.9rem;
        }
        
        .preview-container {
            background: rgba(255, 255, 255, 0.02);
            border: 1px solid rgba(102, 126, 234, 0.2);
            border-radius: 12px;
            padding: 1.5rem;
        }
        
        .status-success {
            background: rgba(40, 167, 69, 0.1);
            color: #28a745;
            padding: 1rem;
            border-radius: 8px;
            border-left: 4px solid #28a745;
            margin: 1rem 0;
            font-weight: 500;
        }
        .status-warning {
            background: rgba(255, 193, 7, 0.1);
            color: #856404;
            padding: 1rem;
            border-radius: 8px;
            border-left: 4px solid #ffc107;
            margin: 1rem 0;
            font-weight: 500;
        }
        
        .stButton > button {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border: none;
            border-radius: 25px;
            padding: 0.8rem 2rem;
            font-weight: 600;
            font-size: 1rem;
            transition: all 0.3s ease;
            font-family: 'Inter', sans-serif;
        }
        .stButton > button:hover {
            transform: translateY(-2px);
            box-shadow: 0 8px 25px rgba(102, 126, 234, 0.3);
        }
        
        .transaction-count {
            display: inline-block;
            background: #667eea;
            color: white;
            padding: 0.2rem 0.6rem;
            border-radius: 15px;
            font-weight: 500;
            font-size: 0.8rem;
            margin-left: 0.5rem;
        }
        </style>
        """

    @staticmethod
    def render_header():
        return """
        <div class="main-header">
            <h1>Credit Card Transaction Extractor</h1>
            <p>AI-powered transaction extraction</p>
        </div>
        """

    @staticmethod
    def render_security_note():
        return """
        <div class="security-note">
            🔒 Your personal information is automatically masked and redacted for security
        </div>
        """

    @staticmethod
    def render_sidebar_header():
        return """
        <div class="sidebar-header">
            <h3>Configuration</h3>
        </div>
        """

    @staticmethod
    def render_process_card_header():
        return """
        <div class="process-card">
            <h3 style="color: #667eea; margin-bottom: 1rem; font-family: 'Inter', sans-serif;">Process Document</h3>
        """

    @staticmethod
    def render_preview_header():
        return """
        <div class="preview-container">
            <h3 style="color: #667eea; margin-bottom: 1rem; font-family: 'Inter', sans-serif; text-align: center;">Document Preview</h3>
        """

    @staticmethod
    def render_section_header(title):
        return f"""
        <div class="section-header">
            <h3>{title}</h3>
        </div>
        """

    @staticmethod
    def render_metric_card(title, value, count, color="#667eea"):
        return f"""
        <div class="metric-card">
            <h4>{title}</h4>
            <h2 style="color: {color};">₹{value:,.2f}</h2>
            <span class="transaction-count">{count} transactions</span>
        </div>
        """

    @staticmethod
    def render_status(message, status_type="success"):
        class_name = f"status-{status_type}"
        return f'<div class="{class_name}">{message}</div>'

    @staticmethod
    def get_features():
        return [
            "Universal bank statement support", 
            "Duplicate removal system",
            "Smart credit/debit classification",
            "Real-time document preview",
            "Enhanced CSV export",
            "Clean data processing"
        ]