import requests
from bs4 import BeautifulSoup
import mysql.connector
from urllib.parse import urljoin
import time
import sys
from datetime import datetime
import re
import random
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging

# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configure retry strategy
retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session = requests.Session()
session.mount("https://", adapter)
session.mount("http://", adapter)

# Constants
TEST_MODE = False  # Set to True to only scrape 1 listing for testing
MIN_DELAY = 1  # Minimum delay between requests in seconds
MAX_DELAY = 3  # Maximum delay between requests in seconds
MAX_LISTINGS = 1 if TEST_MODE else 500

def scrape_listing(url):
    """Scrape a single listing page"""
    try:
        # Add random delay for rate limiting
        delay = random.uniform(MIN_DELAY, MAX_DELAY)
        time.sleep(delay)
        
        logger.info(f"Scraping listing: {url}")
        response = session.get(url, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        data = {
            'url': url,
            'scrape_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

        # Title
        title_elem = soup.select_one('h1.entry-title.entry-prop')
        data['title'] = title_elem.text.strip() if title_elem else ''

        # Price
        price_area = soup.select_one('.price_area')
        if price_area:
            price_text = price_area.text.strip()
            price_match = re.search(r'[\$\€]?\s*([\d,]+(?:\.\d{2})?)\s*([$€])?', price_text)
            if price_match:
                data['price'] = price_match.group(1).replace(',', '')
                data['currency'] = 'MXN' if 'MXN' in price_text else 'USD'

        # Location
        location_elem = soup.select_one('.property_categs')
        if location_elem:
            location_links = location_elem.find_all('a')
            if len(location_links) >= 2:
                data['city'] = location_links[0].text.strip()
                data['area'] = location_links[1].text.strip()

        # Property Details
        details_section = soup.select_one('#accordion_prop_details')
        if details_section:
            details = details_section.select('.listing_detail')
            for detail in details:
                text = detail.text.strip()
                if 'Property Id' in text:
                    data['property_id'] = text.split(':')[1].strip()
                elif 'Bedrooms' in text:
                    data['bedrooms'] = re.search(r'\d+', text).group() if re.search(r'\d+', text) else ''
                elif 'Bathrooms' in text:
                    data['bathrooms'] = re.search(r'\d+\.?\d*', text).group() if re.search(r'\d+\.?\d*', text) else ''
                elif 'Size' in text:
                    size_match = re.search(r'(\d+(?:,\d+)?)\s*m', text)
                    if size_match:
                        data['size_m2'] = size_match.group(1).replace(',', '')

        # Images from owl carousel
        owl_carousel = soup.select_one('#owl-demo')
        if owl_carousel:
            all_images = []
            # Get all item divs that have either background-image or data-lzl-bg
            items = owl_carousel.select('.item')
            
            for item in items:
                # Try data-lzl-bg first (full quality image)
                img_url = item.get('data-lzl-bg')
                if not img_url:
                    # Try background-image style as fallback
                    style = item.get('style', '')
                    bg_match = re.search(r'url\(["\']?(.*?)["\']?\)', style)
                    if bg_match:
                        img_url = bg_match.group(1)
                
                if img_url and img_url not in all_images and not img_url.startswith('data:image'):
                    all_images.append(img_url)
            
            # Remove duplicates while preserving order
            all_images = list(dict.fromkeys(all_images))
            
            # Set main image as first image
            if all_images:
                data['main_image'] = all_images[0]
                data['all_images'] = all_images
                data['image_captions'] = []  # No captions in this layout

        # Coordinates
        map_div = soup.select_one('#googleMap_shortcode')
        if map_div:
            data['latitude'] = map_div.get('data-cur_lat')
            data['longitude'] = map_div.get('data-cur_long')

        # Description
        desc_elem = soup.select_one('.wpestate_property_description')
        if desc_elem:
            # Remove the "Description" title if present
            description_title = desc_elem.find('h4')
            if description_title:
                description_title.decompose()
            data['description'] = desc_elem.get_text(strip=True, separator='\n')
        else:
            data['description'] = ''

        # Features
        features_section = soup.select_one('#accordion_prop_features')
        if features_section:
            # Remove the "Other Features" title if present
            features_title = features_section.find('h4')
            if features_title:
                features_title.decompose()
            features = features_section.select('.listing_detail')
            data['features'] = [f.text.strip() for f in features if f.text.strip() and not f.find('h4')]

        # Agent Info
        agent_name = soup.select_one('.agent_details h3 a')
        data['agent_name'] = agent_name.text.strip() if agent_name else ''
        
        agent_phone = soup.select_one('.agent_phone_class a')
        data['agent_phone'] = agent_phone.text.strip() if agent_phone else ''
        
        agent_email = soup.select_one('.agent_email_class a')
        data['agent_email'] = agent_email.text.strip() if agent_email else ''

        return data

    except Exception as e:
        logger.error(f"Error scraping listing {url}: {str(e)}")
        raise

def url_exists_in_database(url):
    """Check if a URL already exists in the database"""
    db_config = {
        'host': 'junction.proxy.rlwy.net',
        'user': 'root',
        'password': 'rMoaqPfFxeerOSJXPZAXJfZknAiPMSGP', 
        'database': 'railway',
        'port': 25520
    }
    
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        
        check_sql = "SELECT COUNT(*) FROM rpemx_property_listings WHERE url = %s"
        cursor.execute(check_sql, (url,))
        count = cursor.fetchone()[0]
        
        return count > 0
    except Exception as e:
        logger.error(f"Error checking URL in database: {str(e)}")
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def save_to_database(data):
    """Save a single listing to database"""
    if not data:
        print("No data to save.")
        return

    db_config = {
        'host': 'junction.proxy.rlwy.net',
        'user': 'root',
        'password': 'rMoaqPfFxeerOSJXPZAXJfZknAiPMSGP', 
        'database': 'railway',
        'port': 25520
    }

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Create table if doesn't exist
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS rpemx_property_listings (
            id INT AUTO_INCREMENT PRIMARY KEY,
            property_id VARCHAR(50),
            title VARCHAR(255),
            price DECIMAL(15,2),
            currency VARCHAR(10),
            description TEXT,
            area VARCHAR(100),
            city VARCHAR(100),
            size_m2 VARCHAR(50),
            bedrooms VARCHAR(10),
            bathrooms VARCHAR(10),
            agent_name VARCHAR(100),
            agent_phone VARCHAR(50),
            agent_email VARCHAR(100),
            features TEXT,
            url VARCHAR(255),
            scrape_date DATETIME,
            main_image TEXT,
            all_images LONGTEXT,
            image_captions TEXT,
            processed TINYINT(1) DEFAULT 0,
            latitude DECIMAL(10,8),
            longitude DECIMAL(11,8)
        )
        """
        cursor.execute(create_table_sql)
        conn.commit()

        # Add new columns if they don't exist or modify existing ones
        new_columns = [
            ("main_image", "TEXT"),
            ("all_images", "LONGTEXT"),
            ("image_captions", "TEXT"),
            ("processed", "TINYINT(1) DEFAULT 0"),
            ("latitude", "DECIMAL(10,8)"),
            ("longitude", "DECIMAL(11,8)")
        ]
        
        for col_name, col_type in new_columns:
            try:
                # First try to modify existing column
                modify_sql = f"ALTER TABLE rpemx_property_listings MODIFY COLUMN {col_name} {col_type}"
                cursor.execute(modify_sql)
                conn.commit()
            except mysql.connector.Error as err:
                if err.errno == 1054:  # Column doesn't exist
                    try:
                        # Then try to add new column
                        alter_sql = f"ALTER TABLE rpemx_property_listings ADD COLUMN {col_name} {col_type}"
                        cursor.execute(alter_sql)
                        conn.commit()
                    except mysql.connector.Error as add_err:
                        if add_err.errno == 1060:  # Duplicate column error
                            continue
                        else:
                            raise
                else:
                    raise

        # Check if listing exists
        check_sql = """
        SELECT property_id FROM rpemx_property_listings 
        WHERE url = %s
        """
        cursor.execute(check_sql, (data.get('url'),))
        existing = cursor.fetchone()

        if existing:
            # Update existing listing
            update_sql = """
            UPDATE rpemx_property_listings SET
                property_id=%s, title=%s, price=%s, currency=%s, description=%s,
                area=%s, city=%s, size_m2=%s, bedrooms=%s, bathrooms=%s,
                agent_name=%s, agent_phone=%s, agent_email=%s, features=%s,
                scrape_date=%s, main_image=%s, all_images=%s, image_captions=%s,
                processed=0, latitude=%s, longitude=%s
            WHERE url=%s
            """
            values = (
                data.get('property_id'),
                data.get('title'),
                float(data.get('price', 0)) if data.get('price') else 0,
                data.get('currency'),
                data.get('description'),
                data.get('area'),
                data.get('city'),
                data.get('size_m2'),
                data.get('bedrooms'),
                data.get('bathrooms'),
                data.get('agent_name'),
                data.get('agent_phone'),
                data.get('agent_email'),
                '\n'.join(data.get('features', [])),
                data.get('scrape_date'),
                data.get('main_image', ''),
                '\n'.join(data.get('all_images', [])),
                '\n'.join(data.get('image_captions', [])),
                data.get('latitude'),
                data.get('longitude'),
                data.get('url')
            )
            cursor.execute(update_sql, values)
        else:
            # Insert new listing
            insert_sql = """
            INSERT INTO rpemx_property_listings (
                property_id, title, price, currency, description,
                area, city, size_m2, bedrooms, bathrooms,
                agent_name, agent_phone, agent_email, features,
                url, scrape_date, main_image, all_images, image_captions,
                processed, latitude, longitude
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s,
                0, %s, %s
            )
            """
            values = (
                data.get('property_id'),
                data.get('title'),
                float(data.get('price', 0)) if data.get('price') else 0,
                data.get('currency'),
                data.get('description'),
                data.get('area'),
                data.get('city'),
                data.get('size_m2'),
                data.get('bedrooms'),
                data.get('bathrooms'),
                data.get('agent_name'),
                data.get('agent_phone'),
                data.get('agent_email'),
                '\n'.join(data.get('features', [])),
                data.get('url'),
                data.get('scrape_date'),
                data.get('main_image', ''),
                '\n'.join(data.get('all_images', [])),
                '\n'.join(data.get('image_captions', [])),
                data.get('latitude'),
                data.get('longitude')
            )
            cursor.execute(insert_sql, values)

        conn.commit()
        print(f"Successfully saved listing with ID: {data.get('property_id')}")

    except Exception as e:
        print(f"Database error: {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def get_listing_urls_from_api(page=1):
    """Get listing URLs from the REST API"""
    api_url = f"https://realestate.puerto-escondido.mx/wp-json/wp/v2/estate_property?per_page=100&page={page}"
    
    try:
        response = session.get(api_url, timeout=30)
        response.raise_for_status()
        listings = response.json()
        
        if not listings:
            logger.info(f"No listings found on page {page}")
            return []
        
        urls = [listing['link'] for listing in listings if listing.get('link')]
        logger.info(f"Found {len(urls)} listings on page {page}")
        return urls
        
    except Exception as e:
        logger.error(f"Error getting listings from API: {str(e)}")
        return []

def main():
    total_listings = 0
    start_time = datetime.now()
    scraped_urls = set()
    current_page = 1
    
    logger.info("Starting scraper")

    try:
        while total_listings < MAX_LISTINGS:
            print(f"Fetching listings from API page: {current_page}")
            listing_urls = get_listing_urls_from_api(current_page)
            
            if not listing_urls:
                print(f"No more listings found on API page {current_page}")
                break
            
            for url in listing_urls:
                if url not in scraped_urls and not url_exists_in_database(url):
                    total_listings += 1
                    print(f"Scraping listing {total_listings}: {url}")
                    try:
                        listing_data = scrape_listing(url)
                        save_to_database(listing_data)  # Save immediately after scraping
                        scraped_urls.add(url)
                        
                        elapsed_time = datetime.now() - start_time
                        print(f"Progress: {total_listings} listings scraped. Time elapsed: {elapsed_time}")
                        
                    except Exception as e:
                        print(f"Error scraping {url}: {str(e)}")
                        
                    time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

                    if total_listings >= MAX_LISTINGS:
                        print(f"Reached maximum number of listings ({MAX_LISTINGS})")
                        break
                else:
                    print(f"Skipping already scraped/existing URL: {url}")

            current_page += 1

    except KeyboardInterrupt:
        print("\nScraping interrupted by user")
    
    finally:
        total_time = datetime.now() - start_time
        print(f"\nScraping completed. Total listings: {total_listings}")
        print(f"Total time elapsed: {total_time}")

if __name__ == "__main__":
    main()
