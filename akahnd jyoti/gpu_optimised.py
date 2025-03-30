import requests
from bs4 import BeautifulSoup
import os
import time
import re
import json
from urllib.parse import urljoin
import concurrent.futures
import torch
from fake_useragent import UserAgent
import random
import logging
from tqdm import tqdm
import hashlib

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('akhandjyoti_scraper.log'),
        logging.StreamHandler()
    ]
)

# Define base URL for Akhand Jyoti literature
BASE_URL = "http://literature.awgp.org/hindi/akhandjyoti/"

# Specify the range of years to scrape
START_YEAR = 1950
END_YEAR = 2023  # Adjust as needed

# Directory to store extracted content
OUTPUT_DIR = "akhandjyoti_content"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Request configuration
MAX_CONCURRENT_REQUESTS = 8  # Conservative to avoid blocks
MIN_DELAY = 0.5  # Minimum delay between requests in seconds
MAX_RETRIES = 3  # Maximum retries for failed requests
TIMEOUT = 30  # Request timeout in seconds

# Session management
SESSION_DURATION = 300  # 5 minutes
SESSION_REQUESTS = 100  # Max requests per session
PROXY_LIST = []  # Add proxies if available

# Initialize GPU if available
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
if str(device) == 'cuda':
    logging.info(f"Using GPU acceleration: {torch.cuda.get_device_name(0)}")

class RequestManager:
    def __init__(self):
        self.session = requests.Session()
        self.user_agent = UserAgent()
        self.last_request_time = 0
        self.request_count = 0
        self.session_start = time.time()
        self.proxy_rotation = False
        
        if PROXY_LIST:
            self.proxy_rotation = True
            self.current_proxy = 0
            
    def get_proxy(self):
        if not self.proxy_rotation:
            return None
        proxy = PROXY_LIST[self.current_proxy % len(PROXY_LIST)]
        self.current_proxy += 1
        return {'http': proxy, 'https': proxy}
    
    def rotate_session(self):
        self.session.close()
        self.session = requests.Session()
        self.session_start = time.time()
        self.request_count = 0
        
    def make_request(self, url):
        # Implement intelligent rate limiting
        elapsed = time.time() - self.last_request_time
        if elapsed < MIN_DELAY:
            time.sleep(MIN_DELAY - elapsed)
            
        # Rotate session if needed
        if (time.time() - self.session_start > SESSION_DURATION or 
            self.request_count >= SESSION_REQUESTS):
            self.rotate_session()
            
        headers = {
            'User-Agent': self.user_agent.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Referer': BASE_URL,
            'DNT': '1'
        }
        
        proxies = self.get_proxy()
        
        for attempt in range(MAX_RETRIES):
            try:
                response = self.session.get(
                    url,
                    headers=headers,
                    proxies=proxies,
                    timeout=TIMEOUT,
                    stream=True
                )
                
                if response.status_code == 200:
                    self.last_request_time = time.time()
                    self.request_count += 1
                    return response
                elif response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 30))
                    logging.warning(f"Rate limited. Waiting {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue
                else:
                    logging.warning(f"Request failed with status {response.status_code} on attempt {attempt + 1}")
                    time.sleep(2 ** attempt)  # Exponential backoff
            except Exception as e:
                logging.warning(f"Request error on attempt {attempt + 1}: {str(e)}")
                time.sleep(2 ** attempt)
                
        return None

# Initialize request manager
request_manager = RequestManager()

def get_month_links(year_url):
    """Gets links for all months for a given year with improved parsing"""
    response = request_manager.make_request(year_url)
    if not response:
        return {}
    
    soup = BeautifulSoup(response.text, 'html.parser')
    months_data = {}
    
    # Predefined month names in English and Hindi
    english_months = ['January', 'February', 'March', 'April', 'May', 'June',
                     'July', 'August', 'September', 'October', 'November', 'December']
    hindi_months = ['जनवरी', 'फरवरी', 'मार्च', 'अप्रैल', 'मई', 'जून',
                   'जुलाई', 'अगस्त', 'सितंबर', 'अक्टूबर', 'नवंबर', 'दिसंबर']
    
    # Find all elements that might contain month information
    potential_elements = soup.find_all(['div', 'tr', 'li', 'p', 'table'])
    
    for element in potential_elements:
        text = element.get_text().strip()
        
        # Check for month names in the element
        found_month = None
        for eng_month, hi_month in zip(english_months, hindi_months):
            if eng_month in text or hi_month in text:
                found_month = eng_month
                break
                
        if not found_month:
            continue
            
        # Look for scan and text version links within this element
        scan_link = None
        text_link = None
        
        for link in element.find_all('a'):
            link_text = link.get_text().strip()
            href = link.get('href', '')
            
            if 'scan' in link_text.lower() or 'स्कैन' in link_text.lower():
                scan_link = urljoin(year_url, href)
            elif 'text' in link_text.lower() or 'टेक्स्ट' in link_text.lower():
                text_link = urljoin(year_url, href)
                
        if scan_link or text_link:
            months_data[found_month] = {
                'scan': scan_link,
                'text': text_link
            }
    
    return months_data

def get_pagination_links(url, max_pages=36):
    """Generates pagination links with intelligent page count detection"""
    page_urls = [url]
    
    # First try to detect actual page count from the first page
    response = request_manager.make_request(url)
    if response:
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Look for pagination elements
        pagination = soup.find(class_='pagination') or soup.find(id='pagination')
        if pagination:
            page_links = pagination.find_all('a')
            max_detected = 0
            for link in page_links:
                try:
                    page_num = int(link.get_text())
                    if page_num > max_detected:
                        max_detected = page_num
                except ValueError:
                    pass
                    
            if max_detected > 0:
                max_pages = min(max_pages, max_detected)
    
    # Generate page URLs
    for page in range(2, max_pages + 1):
        page_url = f"{url}.{page}" if '.' not in url else f"{url[:-2]}.{page}"
        page_urls.append(page_url)
    
    return page_urls

def download_text_content(url, output_file):
    """Downloads text content with parallel processing"""
    page_urls = get_pagination_links(url)
    all_text = ""
    content_found = False
    
    # Use ThreadPoolExecutor for parallel downloads
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CONCURRENT_REQUESTS) as executor:
        future_to_url = {executor.submit(process_text_page, page_url): page_url for page_url in page_urls}
        
        for future in tqdm(concurrent.futures.as_completed(future_to_url), total=len(page_urls), desc="Downloading pages"):
            page_url = future_to_url[future]
            try:
                page_text = future.result()
                if page_text:
                    all_text += page_text
                    content_found = True
            except Exception as e:
                logging.warning(f"Error processing {page_url}: {str(e)}")
    
    if not content_found:
        return False
        
    # Save combined content
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(all_text)
    
    return True

def process_text_page(page_url):
    """Processes a single text page (for parallel execution)"""
    response = request_manager.make_request(page_url)
    if not response:
        return None
        
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find main content using multiple selectors
    content_selectors = [
        'div#contentArtcile', 'div.article-content', 'div.content',
        'article', 'main', 'div.post-content', 'div.entry-content'
    ]
    
    content = None
    for selector in content_selectors:
        content = soup.select_one(selector)
        if content and len(content.get_text(strip=True)) > 100:
            break
            
    if not content:
        # Fallback: find largest text block
        text_blocks = [tag for tag in soup.find_all(['div', 'article', 'section']) 
                      if len(tag.get_text(strip=True)) > 500]
        if text_blocks:
            content = max(text_blocks, key=lambda tag: len(tag.get_text(strip=True)))
    
    if not content:
        return None
        
    # Extract and clean text
    text = content.get_text(separator='\n\n')
    text = re.sub(r'\n{3,}', '\n\n', text)
    
    # Add page marker
    page_num = re.search(r'\.(\d+)$', page_url)
    page_num = page_num.group(1) if page_num else "1"
    return f"\n\n--- PAGE {page_num} ---\n\n{text}"

def download_scan_images(url, output_dir):
    """Downloads scanned images with parallel processing"""
    page_urls = get_pagination_links(url, max_pages=66)
    os.makedirs(output_dir, exist_ok=True)
    
    # Use ThreadPoolExecutor for parallel downloads
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CONCURRENT_REQUESTS) as executor:
        futures = []
        
        for page_index, page_url in enumerate(page_urls):
            futures.append(executor.submit(
                process_scan_page,
                page_url,
                page_index,
                output_dir
            ))
        
        # Track progress
        downloaded = 0
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Downloading scans"):
            try:
                downloaded += future.result()
            except Exception as e:
                logging.warning(f"Error processing scan page: {str(e)}")
    
    return downloaded > 0

def process_scan_page(page_url, page_index, output_dir):
    """Processes a single scan page (for parallel execution)"""
    response = request_manager.make_request(page_url)
    if not response:
        return 0
        
    soup = BeautifulSoup(response.text, 'html.parser')
    downloaded = 0
    
    # Find all potential image elements
    img_elements = soup.find_all('img')
    
    for i, img in enumerate(img_elements):
        if 'src' not in img.attrs:
            continue
            
        src = img['src']
        if any(x in src.lower() for x in ['icon', 'logo', 'button', 'nav']):
            continue
            
        # Skip small images
        width = img.get('width', '0')
        height = img.get('height', '0')
        try:
            if int(width.replace('px', '')) < 300 or int(height.replace('px', '')) < 300:
                continue
        except (ValueError, AttributeError):
            pass
            
        # Download the image
        img_url = urljoin(page_url, src)
        img_response = request_manager.make_request(img_url)
        
        if not img_response:
            continue
            
        # Generate unique filename
        img_hash = hashlib.md5(img_url.encode()).hexdigest()[:8]
        ext = 'jpg'
        content_type = img_response.headers.get('Content-Type', '').lower()
        if 'png' in content_type:
            ext = 'png'
        elif 'gif' in content_type:
            ext = 'gif'
            
        filename = os.path.join(output_dir, f"page_{page_index+1:03d}_{img_hash}.{ext}")
        
        with open(filename, 'wb') as f:
            f.write(img_response.content)
            
        downloaded += 1
    
    return downloaded

def main():
    """Main function with improved error handling and progress tracking"""
    # Create metadata file
    metadata_file = os.path.join(OUTPUT_DIR, "metadata.json")
    metadata = {}
    
    # Load existing metadata
    if os.path.exists(metadata_file):
        try:
            with open(metadata_file, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
        except Exception as e:
            logging.error(f"Error loading metadata: {str(e)}")
    
    # Initialize progress tracking
    total_years = END_YEAR - START_YEAR + 1
    years_processed = 0
    
    with tqdm(total=total_years, desc="Processing years") as pbar:
        for year in range(START_YEAR, END_YEAR + 1):
            year_url = f"{BASE_URL}{year}"
            logging.info(f"Processing year: {year}")
            
            # Create directory for this year
            year_dir = os.path.join(OUTPUT_DIR, str(year))
            os.makedirs(year_dir, exist_ok=True)
            
            # Initialize year in metadata if not exists
            if str(year) not in metadata:
                metadata[str(year)] = {}
            
            # Get links for all months
            months_data = get_month_links(year_url)
            
            if not months_data:
                logging.warning(f"No months found for year {year}")
                pbar.update(1)
                continue
            
            # Process each month
            for month, versions in months_data.items():
                logging.info(f"Processing {month} {year}")
                
                # Create directory for this month
                month_dir = os.path.join(year_dir, month)
                os.makedirs(month_dir, exist_ok=True)
                
                # Initialize metadata for this month
                month_metadata = {
                    "year": year,
                    "month": month,
                    "has_text": False,
                    "has_scan": False,
                    "text_source": None,
                    "scan_source": None
                }
                
                # Try text version first
                text_downloaded = False
                if versions.get('text'):
                    text_file = os.path.join(month_dir, f"{month}_{year}_text.txt")
                    
                    if not os.path.exists(text_file):
                        logging.info(f"Downloading text version for {month} {year}")
                        success = download_text_content(versions['text'], text_file)
                        if success:
                            month_metadata["has_text"] = True
                            month_metadata["text_source"] = versions['text']
                            text_downloaded = True
                    else:
                        month_metadata["has_text"] = True
                        month_metadata["text_source"] = versions['text']
                        text_downloaded = True
                
                # Fall back to scan if text not available
                if not text_downloaded and versions.get('scan'):
                    scan_dir = os.path.join(month_dir, "scanned_pages")
                    logging.info(f"Downloading scan version for {month} {year}")
                    success = download_scan_images(versions['scan'], scan_dir)
                    if success:
                        month_metadata["has_scan"] = True
                        month_metadata["scan_source"] = versions['scan']
                
                # Update metadata
                metadata[str(year)][month] = month_metadata
                
                # Save metadata after each month
                with open(metadata_file, 'w', encoding='utf-8') as f:
                    json.dump(metadata, f, indent=2, ensure_ascii=False)
            
            # Update progress bar
            pbar.update(1)
            years_processed += 1
            
            # Random delay between years to avoid detection
            if years_processed % 5 == 0:
                delay = random.uniform(5, 15)
                logging.info(f"Random delay of {delay:.1f} seconds to avoid detection")
                time.sleep(delay)
    
    logging.info(f"\nDownload process completed! Content saved to {OUTPUT_DIR}")
    logging.info(f"Metadata saved to {metadata_file}")

if __name__ == "__main__":
    main()