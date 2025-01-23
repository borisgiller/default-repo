import requests
from bs4 import BeautifulSoup
import mysql.connector
from urllib.parse import urljoin
import time
import sys
from datetime import datetime, timedelta
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
TEST_MODE = True  # Set to True to only scrape 1 listing for testing
MIN_DELAY = 2  # Minimum delay between requests in seconds
MAX_DELAY = 5  # Maximum delay between requests in seconds

MAX_LISTINGS = 1 if TEST_MODE else 35  # Limit listings based on test mode

def extract_gps_coordinates(soup):
    """Extract GPS coordinates from the listing page"""
    try:
        # Look for coordinates in map iframe
        map_iframe = soup.find('iframe', src=lambda x: x and 'google.com/maps' in x)
        if map_iframe:
            src = map_iframe.get('src', '')
            coords_match = re.search(r'q=(-?\d+\.\d+),(-?\d+\.\d+)', src)
            if coords_match:
                return float(coords_match.group(1)), float(coords_match.group(2))
        
        # Look for coordinates in script tags
        scripts = soup.find_all('script', string=lambda x: x and 'var wdk_map' in x)
        for script in scripts:
            lat_match = re.search(r'lat\s*:\s*(-?\d+\.\d+)', script.string)
            lng_match = re.search(r'lng\s*:\s*(-?\d+\.\d+)', script.string)
            if lat_match and lng_match:
                return float(lat_match.group(1)), float(lng_match.group(2))
    except Exception as e:
        logger.error(f"Error extracting GPS coordinates: {e}")
    
    return 0.0, 0.0

def scrape_listing(url):
    """Scrape a single listing page with error handling and rate limiting"""
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
        
        # Core Property Details
        title_elem = soup.select_one('h1.property-title')
        data['title'] = title_elem.text.strip() if title_elem else ''
        
        # Property ID from listing ID text
        listing_id_elem = soup.select_one('span[style="font-size: 18pt;"]')
        if listing_id_elem:
            id_text = listing_id_elem.text.strip()
            id_match = re.search(r'ID\s*:\s*(\d+)', id_text)
            data['property_id'] = id_match.group(1) if id_match else ''
        
        # Price and Currency
        price_elem = soup.select_one('h1[style*="text-align: right; color: #00a7b8;"]')
        if price_elem:
            price_text = price_elem.text.strip()
            # Extract numeric price and currency
            price_match = re.search(r'[\$\€]?\s*([\d,]+(?:\.\d{2})?)\s*([$€])?', price_text)
            if price_match:
                data['price'] = price_match.group(1).replace(',', '')
                data['currency'] = 'USD' if '$' in price_text else 'MXN'
        
        # Property Type and Status
        type_status_elem = soup.select_one('.wpestate_estate_property_design_intext_details span[style="font-size: 18pt;"]')
        if type_status_elem:
            type_status = type_status_elem.text.strip().split()
            data['property_type'] = type_status[0] if type_status else ''
            data['status'] = ' '.join(type_status[1:]) if len(type_status) > 1 else ''
        
        # Location Details
        address_elems = soup.select('[id^="accordion_prop_addr"] .panel-body .listing_detail')
        for elem in address_elems:
            key = elem.find('strong').text.strip().rstrip(':').lower()
            value = elem.text.replace(elem.find('strong').text, '').strip()
            if key == 'city':
                data['city'] = value
            elif key == 'area':
                data['area'] = value
            elif key == 'state/county':
                data['state'] = value
            elif key == 'country':
                data['country'] = value
            elif key == 'zip':
                data['zip'] = value
        
        # Physical Characteristics
        details_elems = soup.select('[id^="accordion_prop_details"] .panel-body .listing_detail')
        for detail in details_elems:
            key = detail.find('strong').text.strip().rstrip(':').lower()
            value = detail.text.replace(detail.find('strong').text, '').strip()
            
            # Extract numeric values only
            numeric_value = re.search(r'[\d,.]+', value)
            if numeric_value:
                numeric_value = numeric_value.group().replace(',', '')
            
            if key == 'bedrooms':
                data['bedrooms'] = numeric_value
            elif key == 'bathrooms':
                data['bathrooms'] = numeric_value
                # Check for half baths
                if '.5' in value:
                    data['half_baths'] = '1'
            elif key == 'property size':
                data['interior_space'] = value.replace('ft2', '').replace(',', '').strip()
            elif key == 'land size':
                data['land_size'] = value.replace('ft2', '').replace(',', '').strip()
            elif key == 'parking spot number':
                data['parking_spaces'] = numeric_value
            elif key in ['living rooms', 'kitchens', 'storage rooms', 'terraces']:
                data[key.replace(' ', '_')] = numeric_value or '1' if value else '0'
        
        # Description
        desc_elem = soup.select_one('[id^="collapseDesc"] .panel-body')
        data['description'] = desc_elem.get_text(strip=True, separator='\n') if desc_elem else ''
        
        # Features and Amenities
        feature_flags = [
            'appliances', 'beach_access', 'close_to_airport', 'close_to_beach',
            'electricity', 'furnished', 'gated_community', 'high_rental_revenue',
            'investment_opportunity', 'storage_area', 'sun_deck', 'swimming_pool',
            'terrace', 'unique_location', 'water'
        ]
        
        # Initialize all features as False
        for feature in feature_flags:
            data[feature] = False
            
        features_block = soup.select_one('div.panel-body div.feature_block_others')
        if features_block:
            feature_items = features_block.select('div.listing_detail:not(.feature_chapter_name)')
            features_text = [item.text.strip().lower() for item in feature_items if item.text.strip()]
            
            # Set features found in the HTML to True
            for feature in feature_flags:
                feature_text = feature.replace('_', ' ')
                if any(feature_text in f for f in features_text):
                    data[feature] = True
            
            # Store full features list separately
            data['features_list'] = [item.text.strip() for item in feature_items if item.text.strip()]
        
        # Agent Information
        agent_name_elem = soup.select_one('.agent_details h3 a')
        data['agent_name'] = agent_name_elem.text.strip() if agent_name_elem else ''
        
        agent_phone_elem = soup.select_one('.agent_detail.agent_phone_class a')
        data['agent_phone'] = agent_phone_elem.text.strip() if agent_phone_elem else ''
        
        agent_email_elem = soup.select_one('.agent_detail.agent_email_class a')
        data['agent_email'] = agent_email_elem.text.strip() if agent_email_elem else ''
        
        # Agent Photo
        agent_photo = soup.select_one('.agentpict')
        if agent_photo:
            data['agent_photo'] = agent_photo.get('style', '')
            # Extract URL from background-image style
            photo_match = re.search(r'url\([\'"]?(.*?)[\'"]?\)', data['agent_photo'])
            if photo_match:
                data['agent_photo'] = photo_match.group(1)
                
        # Agent Bio
        agent_bio = soup.select_one('.agent_position')
        data['agent_bio'] = agent_bio.text.strip() if agent_bio else ''
        
        # Media Content
        # Get all image URLs from carousel
        carousel = soup.select_one('#carousel-listing .carousel-inner')
        if carousel:
            # Get main (active) image
            main_image_elem = carousel.select_one('.item.active img')
            data['main_image'] = main_image_elem['src'] if main_image_elem else ''
            
            # Get all images including those in lazy-load items
            all_images = []
            
            # Direct image sources
            for img in carousel.select('.item img'):
                if img.get('src'):
                    all_images.append(img['src'])
                    
            # Lazy load images
            for img in carousel.select('.item.lazy-load-item img'):
                lazy_src = img.get('data-lazy-load-src')
                if lazy_src and lazy_src not in all_images:
                    all_images.append(lazy_src)
                    
            data['all_images'] = all_images
            data['image_captions'] = [img.get('alt', '') for img in carousel.select('.item img')]
            
            # Debug output for images
            print("\nImage URLs found:")
            print(f"Main image: {data['main_image']}")
            print("All images:")
            for idx, img in enumerate(all_images, 1):
                print(f"{idx}. {img}")
        
        # Virtual Tour
        virtual_tour = soup.select_one('iframe[src*="virtualtour"]')
        data['virtual_tour_url'] = virtual_tour['src'] if virtual_tour else ''
        
        # Map Data
        map_data = soup.select_one('.googleMap_shortcode_class')
        if map_data:
            data['latitude'] = map_data.get('data-cur_lat', '')
            data['longitude'] = map_data.get('data-cur_long', '')
            data['map_zoom'] = map_data.get('data-zoom', '')

        # Contact Form Hidden Fields
        hidden_fields = soup.select('.cf-7-hidden-fields input[type="hidden"]')
        for field in hidden_fields:
            field_name = field.get('name', '').lower()
            field_value = field.get('value', '')
            data[f'form_{field_name}'] = field_value

        # Ensure all required fields are present
        required_fields = [
            'property_id', 'title', 'status', 'price', 'currency', 'description',
            'area', 'city', 'state', 'country', 'interior_space', 'land_size',
            'bedrooms', 'bathrooms', 'parking_spaces', 'agent_name', 'agent_phone',
            'agent_email', 'latitude', 'longitude'
        ]
        
        for field in required_fields:
            if field not in data:
                data[field] = ''
        
        return data
        
    except Exception as e:
        logger.error(f"Error scraping listing {url}: {str(e)}")
        raise

def save_to_database(data_list):
    if not data_list:
        print("No data to save.")
        return
        
    if TEST_MODE:
        print("\nDEBUG: Data being saved to database:")
        for data in data_list:
            print(f"\nProperty ID: {data.get('property_id')}")
            print(f"Main image: {data.get('main_image')}")
            print(f"Number of additional images: {len(data.get('all_images', []))}")

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

        # Create table if it doesn't exist
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS property_listings (
            id INT AUTO_INCREMENT PRIMARY KEY,
            property_id VARCHAR(50),
            title VARCHAR(255),
            status VARCHAR(50),
            isnew BOOLEAN DEFAULT TRUE,
            price DECIMAL(15,2),
            main_image VARCHAR(1024),
            all_images TEXT,
            currency VARCHAR(10),
            description TEXT,
            area VARCHAR(100),
            city VARCHAR(100),
            state VARCHAR(100),
            country VARCHAR(100),
            interior_space VARCHAR(50),
            land_size VARCHAR(50),
            bedrooms VARCHAR(10),
            bathrooms VARCHAR(10),
            parking_spaces VARCHAR(10),
            agent_name VARCHAR(100),
            agent_phone VARCHAR(50),
            agent_email VARCHAR(100),
            latitude VARCHAR(20),
            longitude VARCHAR(20),
            url VARCHAR(255),
            scrape_date DATETIME
        )
        """
        cursor.execute(create_table_sql)

        # Check and insert data
        check_existing_sql = """
        SELECT property_id, isnew FROM property_listings 
        WHERE property_id = %s
        """
        
        insert_sql = """
        INSERT INTO property_listings (
            property_id, title, status, price, currency, description,
            area, city, state, country, interior_space, land_size,
            bedrooms, bathrooms, parking_spaces, agent_name,
            agent_phone, agent_email, latitude, longitude, url, scrape_date,
            isnew, main_image, all_images
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s
        )
        """

        update_sql = """
        UPDATE property_listings SET
            title=%s, status=%s, price=%s, currency=%s, description=%s,
            area=%s, city=%s, state=%s, country=%s, interior_space=%s, 
            land_size=%s, bedrooms=%s, bathrooms=%s, parking_spaces=%s, 
            agent_name=%s, agent_phone=%s, agent_email=%s, latitude=%s, 
            longitude=%s, url=%s, scrape_date=%s,
            main_image=%s, all_images=%s
        WHERE property_id=%s
        """

        for data in data_list:
            try:
                # Check if listing already exists
                cursor.execute(check_existing_sql, (data.get('property_id'),))
                existing = cursor.fetchone()
                
                # Keep fetching until no more results
                while cursor.nextset():
                    pass
                    
                if existing:
                # Update existing listing but preserve isnew status
                update_values = (
                    data.get('title'),
                    data.get('status'),
                    float(data.get('price', 0)) if data.get('price') else 0,
                    data.get('currency'),
                    data.get('description'),
                    data.get('area'),
                    data.get('city'),
                    data.get('state'),
                    data.get('country'),
                    data.get('interior_space'),
                    data.get('land_size'),
                    data.get('bedrooms'),
                    data.get('bathrooms'),
                    data.get('parking_spaces'),
                    data.get('agent_name'),
                    data.get('agent_phone'),
                    data.get('agent_email'),
                    data.get('latitude'),
                    data.get('longitude'),
                    data.get('url'),
                    data.get('scrape_date'),
                    data.get('main_image'),
                    ','.join(data.get('all_images', [])),
                    data.get('property_id')
                )
                cursor.execute(update_sql, update_values)
            else:
                # Insert new listing with isnew=True
                insert_values = (
                    data.get('property_id'),
                    data.get('title'),
                    data.get('status'),
                    float(data.get('price', 0)) if data.get('price') else 0,
                    data.get('currency'),
                    data.get('description'),
                    data.get('area'),
                    data.get('city'),
                    data.get('state'),
                    data.get('country'),
                    data.get('interior_space'),
                    data.get('land_size'),
                    data.get('bedrooms'),
                    data.get('bathrooms'),
                    data.get('parking_spaces'),
                    data.get('agent_name'),
                    data.get('agent_phone'),
                    data.get('agent_email'),
                    data.get('latitude'),
                    data.get('longitude'),
                    data.get('url'),
                    data.get('scrape_date'),
                    True,  # isnew flag
                    data.get('main_image'),  # main image URL
                    ','.join(data.get('all_images', []))  # all image URLs as comma-separated string
                )
                cursor.execute(insert_sql, insert_values)

        conn.commit()
        print(f"Successfully saved {len(data_list)} listings to database")
        
        # Verify the saved data
        try:
            verify_cursor = conn.cursor()
            verify_cursor.execute("""
                SELECT property_id, main_image, LENGTH(all_images) as img_count 
                FROM property_listings 
                ORDER BY id DESC LIMIT 1
            """)
            result = verify_cursor.fetchone()
            
            # Clear any remaining result sets
            while verify_cursor.nextset():
                pass
                
            if result:
                print("\nVerification of saved data:")
                print(f"Property ID: {result[0]}")
                print(f"Main image: {result[1]}")
                print(f"All images length: {result[2]} characters")
        finally:
            verify_cursor.close()

    except Exception as e:
        print(f"Error saving to database: {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def get_listing_urls(page_url):
    response = requests.get(page_url)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    listing_elements = soup.select('div.col-md-6.has_prop_slider.listing_wrapper.property_unit_type2')
    urls = []
    for elem in listing_elements:
        link = elem.select_one('h4 a')
        if link and link.has_attr('href'):
            urls.append(urljoin(page_url, link['href']))
    return urls

def get_next_page_url(page_url):
    response = requests.get(page_url)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    next_page = soup.select_one('li.roundright a')
    return urljoin(page_url, next_page['href']) if next_page else None

def main():
    base_url = 'https://baysiderealestate.com/city/puerto-escondido/'
    all_listings_data = []
    total_listings = 0
    start_time = datetime.now()
    scraped_urls = set()
    error_count = 0
    MAX_ERRORS = 5
    
    logger.info("Starting scraper")

    try:
        while base_url and total_listings < MAX_LISTINGS:
            print(f"Scraping page: {base_url}")
            listing_urls = get_listing_urls(base_url)
            
            for url in listing_urls:
                if url.startswith('http') and url not in scraped_urls:
                    total_listings += 1
                    print(f"Scraping listing {total_listings}: {url}")
                    try:
                        listing_data = scrape_listing(url)
                        all_listings_data.append(listing_data)
                        scraped_urls.add(url)
                        elapsed_time = datetime.now() - start_time
                        print(f"Progress: {total_listings} listings scraped. Time elapsed: {elapsed_time}")
                    except Exception as e:
                        print(f"Error scraping {url}: {str(e)}")
                    time.sleep(1)  # Be polite, wait a second between requests

                    if total_listings >= MAX_LISTINGS:
                        print(f"Reached maximum number of listings ({MAX_LISTINGS}). Stopping.")
                        break
                elif url in scraped_urls:
                    print(f"Encountered duplicate listing: {url}. Stopping.")
                    base_url = None
                    break

            if base_url:
                base_url = get_next_page_url(base_url)

    except KeyboardInterrupt:
        print("\nScraping interrupted by user. Saving collected data...")
    
    finally:
        if all_listings_data:
            save_to_database(all_listings_data)
        
        total_time = datetime.now() - start_time
        print(f"\nScraping completed. Total listings scraped: {total_listings}")
        print(f"Total time elapsed: {total_time}")

if __name__ == "__main__":
    main()
