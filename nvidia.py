import os
import sys
import requests
import json
import time
import base64
from dotenv import load_dotenv
from tqdm import tqdm

# Load .env
load_dotenv()
API_KEY = os.getenv("NVIDIA_API_KEY")

nvai_url = "https://integrate.api.nvidia.com/v1/chat/completions"

headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/json",
    "Content-Type": "application/json"
}

# Always use markdown_bbox
tools = ["markdown_bbox", "markdown_no_bbox", "detection_only"]
TOOL = tools[0]  # locked to markdown_bbox

# Add delay between requests
REQUEST_DELAY = 2.0  # seconds between requests


def encode_image_to_base64(image_path):
    """Encode image to base64 string"""
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode('utf-8')


def _generate_content_base64(image_path):
    """Generate content using base64 encoded image instead of asset upload"""
    tool = [{"type": "function", "function": {"name": TOOL}}]
    
    # Encode image to base64
    base64_image = encode_image_to_base64(image_path)
    
    # Use base64 format instead of asset_id
    content = [{
        "type": "image_url",
        "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}
    }]
    
    return content, tool


def extract_tables_from_bbox(bbox_list):
    """Extract only tables (Markdown) from bbox response list"""
    return [item["text"] for item in bbox_list if item.get("type") == "Table"]


def process_image_base64(img_path, folder, page_num, total_pages, max_retries=3):
    """Send request using base64 encoded image and return response + extracted tables"""
    for attempt in range(max_retries):
        try:
            print(f"[{folder}] [PROCESSING] Processing Page {page_num}/{total_pages} ({os.path.basename(img_path)})")

            content, tool = _generate_content_base64(img_path)
            inputs = {
                "tools": tool,
                "model": "nvidia/nemoretriever-parse",
                "messages": [{"role": "user", "content": content}]
            }

            response = requests.post(nvai_url, headers=headers, json=inputs, timeout=120)

            if response.status_code == 429:  # Rate limit
                wait = min(60, 2 ** attempt)
                print(f"[{folder}] Page {page_num}/{total_pages} → Rate limit. Retrying in {wait}s...")
                time.sleep(wait)
                continue
                
            if response.status_code == 500:  # Internal server error
                wait = min(30, 5 * (attempt + 1))
                print(f"[{folder}] Page {page_num}/{total_pages} → Server error. Retrying in {wait}s...")
                time.sleep(wait)
                continue

            response.raise_for_status()
            resp_json = response.json()

            # Extract tables safely
            tables = []
            try:
                tool_args = resp_json["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"]
                if isinstance(tool_args, str):
                    tool_args = json.loads(tool_args)
                
                # tool_args is now a list of lists - flatten it
                if isinstance(tool_args, list) and len(tool_args) > 0:
                    # If it's a nested list, flatten it
                    bbox_list = tool_args[0] if isinstance(tool_args[0], list) else tool_args
                    tables = extract_tables_from_bbox(bbox_list)
                else:
                    tables = []
            except Exception as e:
                tables = [f"[ERROR extracting table from {img_path}] {e}"]

            print(f"[{folder}] [OK] Page {page_num}/{total_pages} processed successfully")
            
            # Add delay between successful requests
            time.sleep(REQUEST_DELAY)
            
            return page_num, {"image": os.path.basename(img_path), "response": resp_json}, tables

        except Exception as e:
            print(f"[{folder}] [ERROR] Error processing Page {page_num}/{total_pages} ({img_path}): {e}")
            if attempt < max_retries - 1:
                wait = min(60, 5 * (attempt + 1))
                print(f"[{folder}] Retrying in {wait}s...")
                time.sleep(wait)

    print(f"[{folder}] [ERROR] Failed to process Page {page_num}/{total_pages} after {max_retries} attempts")
    return page_num, {"image": os.path.basename(img_path), "response": {"error": "Failed after retries"}}, []


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python nvidia.py <images_root_folder> <result_dir>")
        sys.exit(1)

    root_folder = sys.argv[1]
    result_dir = sys.argv[2]

    os.makedirs(result_dir, exist_ok=True)

    print("[OK] Using Base64 image encoding (no asset upload)")
    print(f"[OK] Tool locked: {TOOL}")
    print(f"[OK] Request delay: {REQUEST_DELAY}s between requests")

    # count total pages across all folders
    total_images = sum(
        len([f for f in os.listdir(os.path.join(root_folder, folder)) if f.lower().endswith((".jpg", ".jpeg", ".png"))])
        for folder in os.listdir(root_folder) if os.path.isdir(os.path.join(root_folder, folder))
    )

    with tqdm(total=total_images, desc="[STATS] Overall Progress", unit="page") as overall_pbar:
        for folder in os.listdir(root_folder):
            folder_path = os.path.join(root_folder, folder)
            if not os.path.isdir(folder_path):
                continue

            print(f"\n[FOLDER] Processing folder: {folder}")
            img_files = sorted(
                [f for f in os.listdir(folder_path) if f.lower().endswith((".jpg", ".jpeg", ".png"))]
            )

            results = []
            total_pages = len(img_files)
            
            # Sequential processing with base64 encoding
            for i, img in enumerate(img_files, start=1):
                result = process_image_base64(os.path.join(folder_path, img), folder, i, total_pages)
                results.append(result)
                overall_pbar.update(1)

            # sort results back to correct page order
            results.sort(key=lambda x: x[0])

            # split into responses and tables
            all_responses = [r[1] for r in results]
            all_tables = sum([r[2] for r in results], [])

            # Save JSON (full responses)
            out_json = os.path.join(result_dir, f"{folder}.json")
            with open(out_json, "w", encoding="utf-8") as f:
                json.dump(all_responses, f, indent=2)

            # Save TXT (tables only)
            out_txt = os.path.join(result_dir, f"{folder}.txt")
            with open(out_txt, "w", encoding="utf-8") as f:
                f.write("\n\n".join(all_tables))

            print(f"[COMPLETED] Completed folder {folder}: {total_pages} pages → {out_json}, {out_txt}")
            print(f"[STATS] Found {len(all_tables)} tables total")
            