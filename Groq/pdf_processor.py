import fitz
from collections import defaultdict
from config import *

class PDFProcessor:
    @staticmethod
    def authenticate_pdf(pdf_path, password=None):
        doc = fitz.open(pdf_path)
        if doc.needs_pass:
            if password and doc.authenticate(password):
                doc.close()
                return True
            doc.close()
            return False
        doc.close()
        return True

    @staticmethod
    def is_transaction_line(idx, merged_lines):
        text = merged_lines[idx][1]
        if DATE_REGEX.search(text) and AMOUNT_REGEX.search(text):
            return True
        if AMOUNT_REGEX.search(text):
            for back in range(1, 3):
                if idx-back >= 0 and DATE_REGEX.search(merged_lines[idx-back][1]):
                    return True
        return False

    @staticmethod
    def merge_blocks_by_line(blocks, tolerance=6):
        lines = defaultdict(list)
        for b in blocks:
            y0 = round(b[1]/tolerance)*tolerance
            lines[y0].append(b)
        merged = []
        for y, blks in sorted(lines.items()):
            blks_sorted = sorted(blks, key=lambda x: x[0])
            text = " ".join(b[4].strip() for b in blks_sorted if b[4].strip())
            merged.append((y, text))
        return merged

    @staticmethod
    def is_header_line(text: str) -> bool:
        text_low = text.lower()
        if any(b in text_low for b in SUMMARY_BLACKLIST):
            return False
        if "date" not in text_low:
            return False
        matches = sum(1 for k in COLUMN_KEYWORDS if k in text_low)
        return matches >= 1

    @staticmethod
    def detect_header_y(merged_lines):
        for i, (y, text) in enumerate(merged_lines):
            if PDFProcessor.is_header_line(text):
                tx_count = sum(PDFProcessor.is_transaction_line(j, merged_lines) 
                             for j in range(i+1, min(i+10, len(merged_lines))))
                if tx_count >= 2:
                    return y
            header_chunk = merged_lines[i:i+3]
            combined = " ".join(txt.lower() for _, txt in header_chunk)
            if ("date" in combined and "transaction" in combined and "amount" in combined):
                tx_count = sum(PDFProcessor.is_transaction_line(j, merged_lines) 
                             for j in range(i+3, min(i+13, len(merged_lines))))
                if tx_count >= 2:
                    return max(y for y, _ in header_chunk)
        return None

    @staticmethod
    def redact_pdf(pdf_path, output_path, password=None):
        doc = fitz.open(pdf_path)
        if doc.needs_pass and password:
            doc.authenticate(password)
        
        modified = False
        for page in doc:
            blocks = page.get_text("blocks")
            merged_lines = PDFProcessor.merge_blocks_by_line(blocks)
            header_y = PDFProcessor.detect_header_y(merged_lines)
            if header_y:
                rect = fitz.Rect(0, 0, page.rect.width, max(0, header_y - OFFSET))
                page.draw_rect(rect, color=(1, 1, 1), fill=(1, 1, 1))
                modified = True
        
        if modified:
            doc.save(output_path)
        doc.close()
        return modified