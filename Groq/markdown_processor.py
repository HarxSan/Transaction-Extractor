import base64
import asyncio
import aiohttp
import json
import os
from config import *

class MarkdownProcessor:
    def __init__(self, api_key, batch_size=DEFAULT_BATCH_SIZE):
        self.api_key = api_key
        self.batch_size = batch_size
        self.base_url = GROQ_BASE_URL
        
    def encode_image(self, image_path):
        with open(image_path, "rb") as image_file:
            encoded = base64.b64encode(image_file.read()).decode('utf-8')
            print(f"DEBUG: Encoded image {image_path}, size: {len(encoded)} characters")
            return encoded
    
    def get_markdown_prompt(self):
        return """Convert this credit card statement image to structured markdown format.

Extract ALL information exactly as shown including:
- Account details and headers
- ALL transaction entries in exact order
- Amounts, dates, descriptions precisely as written
- Summary information if present
- Do not omit any sections or details

Requirements:
- Preserve the EXACT sequence of transactions as they appear
- Include every transaction without missing any
- Use markdown table format for transactions
- Keep original text formatting and spacing where possible
- Do not reorder, sort, or modify any data
- Include row numbers or sequence indicators to maintain order

Format as clean markdown with tables for transaction data."""

    async def convert_images_to_markdown(self, image_paths):
        print(f"DEBUG: Processing batch with {len(image_paths)} images for markdown conversion")
        
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        content = [{"type": "text", "text": self.get_markdown_prompt()}]
        
        for img_path in image_paths:
            if not os.path.exists(img_path):
                print(f"ERROR: Image file not found: {img_path}")
                continue
                
            try:
                base64_image = self.encode_image(img_path)
                file_size = os.path.getsize(img_path)
                print(f"DEBUG: Adding image {img_path} (size: {file_size} bytes)")
                
                content.append({
                    "type": "image_url",
                    "image_url": {
                        "url": f"data:image/png;base64,{base64_image}"
                    }
                })
            except Exception as e:
                print(f"ERROR: Failed to encode image {img_path}: {str(e)}")
                continue
        
        if len(content) == 1:
            print("ERROR: No images were successfully added to the request")
            return None
            
        payload = {
            "model": GROQ_MODEL,
            "messages": [{"role": "user", "content": content}],
            "temperature": TEMPERATURE,
            "max_completion_tokens": MAX_COMPLETION_TOKENS
        }
        
        print(f"DEBUG: Sending markdown conversion request with {len(content)-1} images to {self.base_url}")
        
        for attempt in range(MAX_RETRIES):
            try:
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=300)) as session:
                    async with session.post(self.base_url, headers=headers, json=payload) as response:
                        print(f"DEBUG: API response status: {response.status}")
                        
                        if response.status == 200:
                            result = await response.json()
                            print(f"DEBUG: Successful markdown conversion response received")
                            response_content = result['choices'][0]['message']['content']
                            print(f"DEBUG: Markdown content length: {len(response_content)} characters")
                            return response_content
                        else:
                            error_text = await response.text()
                            print(f"ERROR: API call failed - Status {response.status}: {error_text}")
                            if attempt == MAX_RETRIES - 1:
                                raise Exception(f"API Error {response.status}: {error_text}")
                            
            except asyncio.TimeoutError:
                print(f"ERROR: Request timeout on attempt {attempt + 1}")
                if attempt == MAX_RETRIES - 1:
                    raise Exception("Request timeout after all retries")
                    
            except Exception as e:
                print(f"ERROR: Request failed on attempt {attempt + 1}: {str(e)}")
                if attempt == MAX_RETRIES - 1:
                    raise e
                await asyncio.sleep(2 ** attempt)
        
        return None
    
    async def process_all_images(self, image_paths):
        print(f"DEBUG: Starting markdown conversion for {len(image_paths)} total images")
        all_markdown = []
        
        batches = [image_paths[i:i+self.batch_size] for i in range(0, len(image_paths), self.batch_size)]
        print(f"DEBUG: Created {len(batches)} batches with batch size {self.batch_size}")
        
        for i, batch in enumerate(batches):
            print(f"DEBUG: Processing markdown batch {i+1}/{len(batches)}")
            try:
                result = await self.convert_images_to_markdown(batch)
                if result:
                    print(f"DEBUG: Markdown batch {i+1} returned result length: {len(result)}")
                    all_markdown.append(result)
                else:
                    print(f"WARNING: Markdown batch {i+1} returned no result")
            except Exception as e:
                print(f"ERROR: Markdown batch {i+1} failed: {str(e)}")
                continue
        
        combined_markdown = "\n\n---\n\n".join(all_markdown)
        print(f"DEBUG: Combined markdown length: {len(combined_markdown)} characters")
        
        return combined_markdown if combined_markdown.strip() else None