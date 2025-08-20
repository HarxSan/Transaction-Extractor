import fitz  # PyMuPDF
from PIL import Image
import os
import argparse

def pdf_to_images(pdf_path, output_dir, max_pixels=1_800_000, min_dpi=216):
    pdf_name = os.path.splitext(os.path.basename(pdf_path))[0]
    pdf_output_dir = os.path.join(output_dir, pdf_name)
    os.makedirs(pdf_output_dir, exist_ok=True)

    doc = fitz.open(pdf_path)
    for page_num in range(len(doc)):
        page = doc.load_page(page_num)

        # --- Step 1: Render at higher DPI if native resolution is low ---
        # First get size at 72 DPI
        pix72 = page.get_pixmap(alpha=False)
        width72, height72 = pix72.width, pix72.height
        dpi72 = (width72 / page.rect.width) * 72  # Approx horizontal DPI

        # Decide zoom factor
        if dpi72 < min_dpi:
            zoom = min_dpi / 72.0
        else:
            zoom = 1.0  # already high-res

        mat = fitz.Matrix(zoom, zoom)
        pix = page.get_pixmap(matrix=mat, alpha=False)

        # --- Step 2: Convert to PIL and optionally resize ---
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        width, height = img.size

        if width * height > max_pixels:
            scale = (max_pixels / (width * height)) ** 0.5
            new_size = (int(width * scale), int(height * scale))
            img = img.resize(new_size, Image.LANCZOS)

        # --- Step 3: Save ---
        filename = os.path.join(pdf_output_dir, f"page_{page_num + 1:03d}.png")
        img.save(filename, format="PNG")
        print(f"Saved {filename} ({img.size[0]}x{img.size[1]}) | zoom={zoom:.2f}")

    print(f"Finished converting {pdf_path}")

def process_folder(input_folder, output_folder, max_pixels=1_800_000, min_dpi=216):
    os.makedirs(output_folder, exist_ok=True)
    for file_name in os.listdir(input_folder):
        if file_name.lower().endswith(".pdf"):
            pdf_path = os.path.join(input_folder, file_name)
            pdf_to_images(pdf_path, output_folder, max_pixels, min_dpi)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert PDFs to high-quality images with auto-DPI boost.")
    parser.add_argument("input_folder", help="Path to folder containing PDF files")
    parser.add_argument("output_folder", help="Path to folder where images will be saved")
    parser.add_argument("--max_pixels", type=int, default=1_800_000, help="Max pixels per image (default 1.8M)")
    parser.add_argument("--min_dpi", type=int, default=216, help="Minimum DPI to ensure quality (default 216)")
    args = parser.parse_args()

    process_folder(args.input_folder, args.output_folder, args.max_pixels, args.min_dpi)
