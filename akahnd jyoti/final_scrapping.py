import requests
from bs4 import BeautifulSoup
import os
import time
import re
import json
from urllib.parse import urljoin

# Define base URL for Akhand Jyoti literature - updated based on search result [6]
BASE_URL = "http://literature.awgp.org/hindi/akhandjyoti/"

# Specify the range of years to scrape
START_YEAR = 1950
END_YEAR = 1951

# Directory to store extracted content
OUTPUT_DIR = "akhandjyoti_content"
os.makedirs(OUTPUT_DIR, exist_ok=True)  # Create directory if it doesn't exist

# Request counter and delay settings
request_counter = 0
DELAY_AFTER_REQUESTS = 3
DELAY_SECONDS = 10

def make_request(url):
    """Makes an HTTP request with delay logic implemented"""
    global request_counter
    
    request_counter += 1
    
    # Implement delay after every 3 requests
    if request_counter % DELAY_AFTER_REQUESTS == 0:
        print(f"Pausing for {DELAY_SECONDS} seconds after {request_counter} requests...")
        time.sleep(DELAY_SECONDS)
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code != 200:
            print(f"Failed to fetch {url}, status code: {response.status_code}")
            return None
        return response
    except Exception as e:
        print(f"Error fetching {url}: {str(e)}")
        return None

def get_month_links(year_url):
    """Gets links for all months for a given year"""
    response = make_request(year_url)
    if not response:
        return {}
    
    soup = BeautifulSoup(response.text, 'html.parser')
    months_data = {}
    
    # Looking directly for "Scan Version" and "Text Version" links
    english_months = ['January', 'February', 'March', 'April', 'May', 'June',
                     'July', 'August', 'September', 'October', 'November', 'December']
    hindi_months = ['जनवरी', 'फरवरी', 'मार्च', 'अप्रैल', 'मई', 'जून',
                   'जुलाई', 'अगस्त', 'सितंबर', 'अक्टूबर', 'नवंबर', 'दिसंबर']
    
    # Find all links in the page
    links = soup.find_all('a')
    
    # Group links by month
    for month_idx, (eng_month, hi_month) in enumerate(zip(english_months, hindi_months)):
        scan_link = None
        text_link = None
        
        for link in links:
            link_text = link.get_text().strip()
            href = link.get('href', '')
            
            # Check if this is a scan or text version link
            is_scan = 'Scan Version' in link_text or 'स्कैन वर्जन' in link_text
            is_text = 'Text Version' in link_text or 'टेक्स्ट वर्जन' in link_text
            
            if not (is_scan or is_text):
                continue
            
            # Find the month this link belongs to by checking nearby text
            # First check if the link's parent contains month name
            parent_text = ''.join(parent.get_text() for parent in link.parents if parent.name != 'html')
            
            month_match = None
            if eng_month in parent_text or hi_month in parent_text:
                month_match = eng_month
            
            # If no direct match, check previous siblings or elements
            if not month_match:
                prev_elem = link.find_previous(string=lambda s: s and (eng_month in s or hi_month in s))
                if prev_elem:
                    month_match = eng_month
            
            # If we found a month match and this is a valid version link
            if month_match:
                if is_scan and not scan_link:
                    scan_link = urljoin(year_url, href)
                elif is_text and not text_link:
                    text_link = urljoin(year_url, href)
        
        # If we found links for this month, add them to our results
        if scan_link or text_link:
            months_data[eng_month] = {
                'scan': scan_link,
                'text': text_link
            }
    
    # If no months found using the above method, try a more direct approach
    if not months_data:
        # Look for the specific pattern in result [6]
        for month in english_months:
            month_elem = soup.find(string=lambda s: s and month in s)
            if month_elem:
                # Find scan and text links after this month
                next_scan = None
                next_text = None
                
                # Check all scan version links
                scan_links = soup.find_all('a', string='Scan Version')
                for link in scan_links:
                    if month in str(link.previous_element) or month in str(link.previous_sibling):
                        next_scan = urljoin(year_url, link['href'])
                        break
                
                # Check all text version links
                text_links = soup.find_all('a', string='Text Version')
                for link in text_links:
                    if month in str(link.previous_element) or month in str(link.previous_sibling):
                        next_text = urljoin(year_url, link['href'])
                        break
                
                if next_scan or next_text:
                    months_data[month] = {
                        'scan': next_scan,
                        'text': next_text
                    }
    
    return months_data

def get_pagination_links(url, max_pages=3):
# def get_pagination_links(url, max_pages=36):
    """Extracts all page links from a magazine issue up to specified max_pages"""
    page_urls = [url]
    
    # Extract base URL and version number
    match = re.search(r'(.*/v\d+)', url)
    if not match:
        return page_urls  # Return just the original URL if pattern doesn't match
    
    base_url = match.group(1)
    
    # Generate all possible page URLs
    for page in range(2, max_pages + 1):
        page_url = f"{base_url}.{page}"
        page_urls.append(page_url)
    
    print(f"Generated {len(page_urls)} pages for {url}")
    return page_urls

def download_text_content(url, output_file):
    """Downloads and saves text content from all pages up to .36"""
    page_urls = get_pagination_links(url)
    all_text = ""
    content_found = False
    
    for i, page_url in enumerate(page_urls):
        print(f"Processing page {i+1}/{len(page_urls)}: {page_url}")
        response = make_request(page_url)
        if not response:
            continue
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the main content - try multiple selectors
        content = None
        for selector in ['div#contentArtcile', 'div.article-content', 
                        'div.content', 'article', 'main']:
            content = soup.select_one(selector)
            if content and len(content.get_text(strip=True)) > 100:
                break
                
        if not content:
            # Try to find largest text block as fallback
            text_blocks = [tag for tag in soup.find_all(['div', 'article', 'section']) 
                         if len(tag.get_text(strip=True)) > 500]
            if text_blocks:
                content = max(text_blocks, key=lambda tag: len(tag.get_text(strip=True)))
        
        if not content:
            print(f"No substantial content found on page {page_url}")
            continue
            
        content_found = True
        
        # Extract and clean the text
        text = content.get_text(separator='\n\n')
        text = re.sub(r'\n{3,}', '\n\n', text)  # Clean up excess newlines
        
        # Add page marker and content to combined text
        all_text += f"\n\n--- PAGE {i+1} ---\n\n"
        all_text += text
    
    if not content_found:
        print(f"No content found for {url}")
        return False
        
    # Save combined content from all pages
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(all_text)
    
    print(f"Saved complete text content to {output_file}")
    return True

def download_scan_images(url, output_dir):
    """Downloads all scanned images from all pages of a magazine issue"""
    page_urls = get_pagination_links(url, max_pages=2)  # Updated to 66 pages for images
    # page_urls = get_pagination_links(url, max_pages=66)
    os.makedirs(output_dir, exist_ok=True)
    total_downloaded = 0
    
    for page_index, page_url in enumerate(page_urls):
        print(f"Processing scan page {page_index+1}/{len(page_urls)}: {page_url}")
        response = make_request(page_url)
        if not response:
            continue
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find all images that might be scanned pages
        images = soup.find_all('img')
        page_downloaded = 0
        
        for i, img in enumerate(images):
            if 'src' not in img.attrs:
                continue
            
            # Skip navigation elements, icons, etc.
            src = img['src']
            if any(x in src.lower() for x in ['icon', 'logo', 'button', 'nav']):
                continue
            
            # Try to identify substantial content images
            is_large_image = True
            if 'width' in img.attrs and 'height' in img.attrs:
                try:
                    width = int(img['width'].replace('px', ''))
                    height = int(img['height'].replace('px', ''))
                    if width < 300 or height < 300:  # Likely not a scan page
                        is_large_image = False
                except (ValueError, AttributeError):
                    pass
            
            if not is_large_image:
                continue
            
            # Download the image
            img_url = urljoin(page_url, src)
            img_response = make_request(img_url)
            
            if not img_response:
                continue
            
            # Determine appropriate file extension
            ext = 'jpg'  # Default extension
            content_type = img_response.headers.get('Content-Type', '').lower()
            if 'png' in content_type:
                ext = 'png'
            elif 'gif' in content_type:
                ext = 'gif'
            
            # Save the image with page number in filename for proper ordering
            filename = os.path.join(output_dir, f"page_{page_index+1:03d}img{i+1:03d}.{ext}")
            
            with open(filename, 'wb') as f:
                f.write(img_response.content)
            
            page_downloaded += 1
            total_downloaded += 1
            print(f"Downloaded image: {filename}")
        
        # If no images found directly, look for frames that might contain images
        if page_downloaded == 0:
            frames = soup.find_all(['frame', 'iframe'])
            for j, frame in enumerate(frames):
                if 'src' in frame.attrs:
                    frame_url = urljoin(page_url, frame['src'])
                    frame_response = make_request(frame_url)
                    
                    if frame_response:
                        frame_soup = BeautifulSoup(frame_response.text, 'html.parser')
                        frame_images = frame_soup.find_all('img')
                        
                        for k, img in enumerate(frame_images):
                            if 'src' in img.attrs:
                                img_url = urljoin(frame_url, img['src'])
                                img_response = make_request(img_url)
                                
                                if img_response:
                                    filename = os.path.join(output_dir, f"page_{page_index+1:03d}frame{j+1:02d}img{k+1:02d}.jpg")
                                    with open(filename, 'wb') as f:
                                        f.write(img_response.content)
                                    total_downloaded += 1
    
    print(f"Downloaded {total_downloaded} images across {len(page_urls)} pages to {output_dir}")
    return total_downloaded > 0

def main():
    """Main function to coordinate the downloading process"""
    global BASE_URL  # Declare BASE_URL as global so we can modify it
    
    # Create metadata file
    metadata_file = os.path.join(OUTPUT_DIR, "metadata.json")
    metadata = {}
    
    # Load existing metadata if file exists
    if os.path.exists(metadata_file):
        try:
            with open(metadata_file, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
        except json.JSONDecodeError:
            print("Error reading existing metadata file. Creating new one.")
            metadata = {}
    
    # First try the current base URL format
    test_year = 1948  # Known to exist from search result [6]
    test_url = f"{BASE_URL}{test_year}"
    
    print(f"Testing URL: {test_url}")
    response = make_request(test_url)
    
    # If first attempt fails, try alternative URL formats
    if not response:
        alternative_urls = [
            "https://www.awgp.org/en/literature/akhandjyoti/",
            "https://literature.awgp.org/english/akhandjyoti/",
            "http://literature.awgp.org/english/akhandjyoti/"
        ]
        
        for alt_url in alternative_urls:
            test_alt_url = f"{alt_url}{test_year}"
            print(f"Testing alternative URL: {test_alt_url}")
            alt_response = make_request(test_alt_url)
            
            if alt_response:
                print(f"Found working URL format: {alt_url}")
                BASE_URL = alt_url
                break
    
    for year in range(START_YEAR, END_YEAR + 1):
        year_url = f"{BASE_URL}{year}"
        print(f"\nProcessing year: {year}")
        
        # Create directory for this year
        year_dir = os.path.join(OUTPUT_DIR, str(year))
        os.makedirs(year_dir, exist_ok=True)
        
        # Initialize year in metadata if not exists
        if str(year) not in metadata:
            metadata[str(year)] = {}
        
        # Get links for all months
        months_data = get_month_links(year_url)
        
        if not months_data:
            print(f"No months found for year {year}")
            continue
        
        for month, versions in months_data.items():
            print(f"\nProcessing {month} {year}")
            
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
            
            # Check if text version is available and download it
            text_downloaded = False
            if versions.get('text'):
                text_file = os.path.join(month_dir, f"{month}_{year}_text.txt")
                
                # Check if text file already exists
                if os.path.exists(text_file):
                    print(f"Text version already exists for {month} {year}")
                    month_metadata["has_text"] = True
                    month_metadata["text_source"] = versions['text']
                    text_downloaded = True
                else:
                    print(f"Downloading text version for {month} {year}")
                    success = download_text_content(versions['text'], text_file)
                    if success:
                        month_metadata["has_text"] = True
                        month_metadata["text_source"] = versions['text']
                        text_downloaded = True
            
            # Only download scanned version if text version is not available or failed to download
            if not text_downloaded and versions.get('scan'):
                scan_dir = os.path.join(month_dir, "scanned_pages")
                print(f"Downloading scan version for {month} {year} (text not available)")
                success = download_scan_images(versions['scan'], scan_dir)
                if success:
                    month_metadata["has_scan"] = True
                    month_metadata["scan_source"] = versions['scan']
            
            if not (versions.get('text') or versions.get('scan')):
                print(f"No versions available for {month} {year}")
            
            # Save metadata for this month
            metadata[str(year)][month] = month_metadata
            
            # Update metadata file after each month to avoid data loss
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
    
    print(f"\nDownload process completed! Content saved to {OUTPUT_DIR}")
    print(f"Metadata saved to {metadata_file}")

if _name_ == "_main_":
    main()

#only non text ke scan