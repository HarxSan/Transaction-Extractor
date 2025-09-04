import base64
import asyncio
import aiohttp
import os
import logging
import time
from typing import Optional, List, Tuple
from pathlib import Path
from config import *

logger = logging.getLogger(__name__)

class MarkdownProcessor:
    def __init__(self, api_key: str, batch_size: int = DEFAULT_BATCH_SIZE):
        if not api_key or not api_key.strip():
            raise ValueError("API key is required")
        
        self.api_key = api_key.strip()
        self.batch_size = max(1, min(batch_size, 10))
        self.base_url = GROQ_BASE_URL
        self.session = None
        self.retry_delays = [1, 2, 4]
    
    async def __aenter__(self):
        connector = aiohttp.TCPConnector(
            limit=10,
            limit_per_host=2,
            ttl_dns_cache=300,
            keepalive_timeout=30
        )
        
        timeout = aiohttp.ClientTimeout(
            total=300,
            connect=30,
            sock_read=60
        )
        
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
            self.session = None
    
    def validate_image(self, image_path: str) -> Tuple[bool, str]:
        if not image_path or not os.path.exists(image_path):
            return False, "Image file does not exist"
        
        try:
            file_size = os.path.getsize(image_path)
            if file_size == 0:
                return False, "Image file is empty"
            
            if file_size > 10 * 1024 * 1024:
                return False, "Image file too large (>10MB)"
                
        except Exception as e:
            return False, f"Error validating image: {str(e)}"
        
        return True, "Valid image"
    
    def encode_image(self, image_path: str) -> str:
        is_valid, validation_message = self.validate_image(image_path)
        if not is_valid:
            raise ValueError(f"Invalid image: {validation_message}")
        
        try:
            with open(image_path, "rb") as image_file:
                encoded = base64.b64encode(image_file.read()).decode('utf-8')
                if not encoded:
                    raise ValueError("Failed to encode image to base64")
                return encoded
        except Exception as e:
            raise ValueError(f"Error encoding image {image_path}: {str(e)}")
    
    def get_markdown_prompt(self) -> str:
        return """Convert this credit card statement image to structured markdown format.

CRITICAL: Start your response with exactly this format:
HAS_TRANSACTIONS: True

OR

HAS_TRANSACTIONS: False

Analyze if this page contains actual financial transactions with dates, descriptions, and amounts. 
Do not count headers, summaries, account information, advertisements, or promotional content as transactions.

After the HAS_TRANSACTIONS line, if there are transactions, extract ALL information exactly as shown:
- Account details and headers (preserve exactly)
- ALL transaction entries in exact order (do not skip any)
- Amounts, dates, descriptions precisely as written
- Summary information if present
- Preserve all sections and details

Requirements for transaction extraction:
- Extract transactions row by row in exact order
- For each transaction: Date | Description | Amount | Type (Debit/Credit)
- Do not split or merge rows
- If description spans multiple lines, join into one cell
- Ensure Amount belongs to same row as its Date
- Include every transaction without omissions
- Use markdown table format for transactions
- Keep original text formatting and spacing
- Do not reorder, sort, or modify data
- Include sequence indicators to maintain order

Format as clean markdown with tables for transaction data.
Preserve all numerical values exactly as shown."""

    async def convert_images_to_markdown(self, image_paths: List[str]) -> Tuple[Optional[str], bool]:
        if not image_paths:
            return None, False
        
        if not self.session:
            async with self:
                return await self._process_images(image_paths)
        else:
            return await self._process_images(image_paths)
    
    async def _process_images(self, image_paths: List[str]) -> Tuple[Optional[str], bool]:
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        content = [{"type": "text", "text": self.get_markdown_prompt()}]
        
        valid_images = 0
        for img_path in image_paths:
            try:
                is_valid, validation_message = self.validate_image(img_path)
                if not is_valid:
                    logger.warning(f"Skipping invalid image {img_path}: {validation_message}")
                    continue
                
                base64_image = self.encode_image(img_path)
                content.append({
                    "type": "image_url",
                    "image_url": {
                        "url": f"data:image/png;base64,{base64_image}"
                    }
                })
                valid_images += 1
                
            except Exception as e:
                logger.error(f"Error processing image {img_path}: {str(e)}")
                continue
        
        if valid_images == 0:
            return None, False
        
        payload = {
            "model": GROQ_MODEL,
            "messages": [{"role": "user", "content": content}],
            "temperature": TEMPERATURE,
            "max_completion_tokens": MAX_COMPLETION_TOKENS,
            "top_p": 0.9,
            "stream": False
        }
        
        response_content = await self._make_api_request(headers, payload)
        if not response_content:
            return None, False
        
        has_transactions = self._detect_transactions_in_markdown(response_content)
        return response_content, has_transactions
    
    async def _make_api_request(self, headers, payload) -> Optional[str]:
        for attempt in range(MAX_RETRIES):
            try:
                async with self.session.post(self.base_url, headers=headers, json=payload) as response:
                    if response.status == 200:
                        try:
                            result_data = await response.json()
                            if 'choices' not in result_data or not result_data['choices']:
                                logger.error("No choices in API response")
                                continue
                                
                            choice = result_data['choices'][0]
                            if 'message' not in choice or 'content' not in choice['message']:
                                logger.error("Invalid response structure")
                                continue
                            
                            response_content = choice['message']['content']
                            if not response_content or not response_content.strip():
                                logger.error("Empty response content")
                                continue
                            
                            return response_content
                            
                        except Exception as e:
                            logger.error(f"Error parsing JSON response: {e}")
                            continue
                            
                    elif response.status == 429:
                        retry_after = int(response.headers.get('Retry-After', self.retry_delays[min(attempt, len(self.retry_delays)-1)]))
                        logger.warning(f"Rate limited, retrying after {retry_after}s")
                        await asyncio.sleep(retry_after)
                        continue
                        
                    elif response.status == 401:
                        logger.error("Authentication failed. Check API key.")
                        break
                        
                    else:
                        try:
                            error_text = await response.text()
                            logger.error(f"API Error {response.status}: {error_text[:200]}")
                        except:
                            logger.error(f"API Error {response.status}")
                        
                        if response.status >= 500 and attempt < MAX_RETRIES - 1:
                            delay = self.retry_delays[min(attempt, len(self.retry_delays)-1)]
                            await asyncio.sleep(delay)
                            continue
                        else:
                            break
                            
            except asyncio.TimeoutError:
                logger.warning(f"Request timeout (attempt {attempt + 1})")
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(self.retry_delays[min(attempt, len(self.retry_delays)-1)])
                    continue
                    
            except Exception as e:
                logger.error(f"Request error: {e}")
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(self.retry_delays[min(attempt, len(self.retry_delays)-1)])
                    continue
                else:
                    break
        
        return None
    
    def _detect_transactions_in_markdown(self, markdown_content: str) -> bool:
        if not markdown_content or not markdown_content.strip():
            return False
        
        try:
            content_lines = markdown_content.strip().split('\n')
            
            for line in content_lines[:15]:
                line = line.strip()
                if line.startswith('HAS_TRANSACTIONS:'):
                    has_transactions_str = line.replace('HAS_TRANSACTIONS:', '').strip().lower()
                    result = has_transactions_str == 'true'
                    logger.info(f"Transaction detection result: {result}")
                    return result
            
            logger.warning("HAS_TRANSACTIONS marker not found in response")
            return False
            
        except Exception as e:
            logger.error(f"Error detecting transactions: {e}")
            return False
    
    async def process_all_images(self, image_paths: List[str]) -> Optional[str]:
        if not image_paths:
            return None
        
        all_markdown = []
        
        try:
            async with self:
                for i, image_path in enumerate(image_paths):
                    if not os.path.exists(image_path):
                        logger.warning(f"Image {i+1} does not exist: {image_path}")
                        continue
                    
                    logger.info(f"Processing image {i+1}/{len(image_paths)}: {Path(image_path).name}")
                    
                    try:
                        result, has_transactions = await self.convert_images_to_markdown([image_path])
                        
                        if result and result.strip():
                            all_markdown.append(result)
                            logger.info(f"Successfully processed image {i+1}, has_transactions: {has_transactions}")
                            
                            if not has_transactions:
                                logger.info(f"No transactions found in image {i+1}, stopping processing")
                                break
                        else:
                            logger.warning(f"No valid result from image {i+1}, stopping processing")
                            break
                            
                    except Exception as e:
                        logger.error(f"Error processing image {i+1}: {str(e)}")
                        break
                
        except Exception as e:
            logger.error(f"Error in process_all_images: {str(e)}")
            return None
        
        if not all_markdown:
            return None
        
        combined_markdown = "\n\n---\n\n".join(all_markdown)
        return combined_markdown if combined_markdown.strip() else None