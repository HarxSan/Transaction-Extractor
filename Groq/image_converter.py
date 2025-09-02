import fitz
import os
from config import DEFAULT_DPI

class ImageConverter:
    @staticmethod
    def convert_pdf_to_images(pdf_path, output_dir, dpi=DEFAULT_DPI, password=None):
        doc = fitz.open(pdf_path)
        if doc.needs_pass and password:
            doc.authenticate(password)
        
        image_paths = []
        for page_num in range(len(doc)):
            page = doc.load_page(page_num)
            mat = fitz.Matrix(dpi/72, dpi/72)
            pix = page.get_pixmap(matrix=mat)
            img_path = os.path.join(output_dir, f"page_{page_num + 1}.png")
            pix.save(img_path)
            image_paths.append(img_path)
        
        doc.close()
        return image_paths

    @staticmethod
    def get_pdf_page_as_image(pdf_path, page_num=0, password=None):
        doc = fitz.open(pdf_path)
        if doc.needs_pass and password:
            doc.authenticate(password)
        
        page = doc.load_page(page_num)
        pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
        img_data = pix.tobytes("png")
        doc.close()
        return img_data