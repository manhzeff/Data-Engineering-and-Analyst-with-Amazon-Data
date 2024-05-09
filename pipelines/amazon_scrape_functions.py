import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import pandas as pd
import logging
import json
from datetime import datetime

# Thiết lập logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

custom_headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, như Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept-Language': 'da, en-gb, en',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Referer': 'https://www.google.com/'
}

def get_review_count(soup):
    try:
        review_count = soup.find("span", attrs={'id': 'acrCustomerReviewText'}).string.strip()
    except AttributeError:
        review_count = ""
    return review_count

def get_availability(soup):
    try:
        available = soup.find("div", attrs={'id': 'availability'})
        available = available.find("span").string.strip()
    except AttributeError:
        available = "Not Available"
    return available

def get_product_info(url, custom_headers):
    logger.info(f"Fetching product info from URL: {url}")
    response = requests.get(url, headers=custom_headers)
    if response.status_code != 200:
        logger.error(f"Error in getting webpage: {url}, Status Code: {response.status_code}")
        return None

    soup = BeautifulSoup(response.text, "lxml")
    title_element = soup.select_one("#productTitle")
    title = title_element.text.strip() if title_element else None
    price_element = soup.select_one('span.a-offscreen')
    price = price_element.text if price_element else None
    rating_element = soup.select_one("#acrPopover")
    rating_text = rating_element.attrs.get("title") if rating_element else None
    rating = rating_text.replace("out of 5 stars", "") if rating_text else None
    image_element = soup.select_one("#landingImage")
    image = image_element.attrs.get("src") if image_element else None
    description_element = soup.select_one("#productDescription")
    description = description_element.text.strip() if description_element else None

    reviews = get_review_count(soup)
    availability = get_availability(soup)

    logger.info(f"Product fetched: {title}")

    return {
        "title": title,
        "price": price,
        "rating": rating,
        "image": image,
        "description": description,
        "reviews": reviews,
        "availability": availability,
        "url": url
    }

def parse_listing(listing_url, custom_headers, visited_urls, max_products):
    product_count = 0
    all_products = []

    logger.info(f"Starting to parse listing: {listing_url}")

    while product_count < max_products and listing_url:
        response = requests.get(listing_url, headers=custom_headers)
        if response.status_code != 200:
            logger.error(f"Failed to retrieve URL: {listing_url}, Status Code: {response.status_code}")
            break

        soup_search = BeautifulSoup(response.text, "lxml")
        link_elements = soup_search.select("[data-asin] h2 a")

        for link in link_elements:
            if product_count >= max_products:
                break
            full_url = urljoin(listing_url, link.attrs.get("href"))
            if full_url not in visited_urls:
                visited_urls.add(full_url)
                product_info = get_product_info(full_url, custom_headers)
                if product_info:
                    all_products.append(product_info)
                    product_count += 1

        next_page_el = soup_search.select_one('a.s-pagination-next')
        listing_url = urljoin(listing_url, next_page_el.attrs.get('href')) if next_page_el else None
        logger.info(f"Next page URL: {listing_url}")

    logger.info(f"Finished parsing listing")

    return all_products

def transform_amazon_data(**kwargs):
    """Transforms raw data fetched from Amazon into structured format, applying data cleaning."""
    ti = kwargs['ti']
    data_json = ti.xcom_pull(task_ids='crawl_amazon', key='product_data')
    if not data_json:
        logging.error("No data received from crawl_amazon.")
        return "No data to transform."

    data = json.loads(data_json)
    if not data:
        logging.error("JSON data is empty.")
        return "No data to transform."

    # Convert data into a DataFrame for easier manipulation
    df = pd.DataFrame(data)
    if 'price' not in df.columns:
        logging.error("Price column is missing in the data.")
        return "Required data missing."

    # Handle non-numeric and unexpected strings in 'price' column before converting
    df['price'] = df['price'].replace('[\$,]', '', regex=True)
    df['price'] = pd.to_numeric(df['price'], errors='coerce')

    # Calculate mean for price and fill missing values
    mean_price = df['price'].mean()
    df['price'] = df['price'].fillna(mean_price)

    # Ensure rating is treated as string if it's not null, then convert the first part to float
    df['rating'] = df['rating'].apply(lambda x: float(x.split()[0]) if pd.notnull(x) and 'out of' in x else x)
    df['rating'] = pd.to_numeric(df['rating'], errors='coerce')  # Convert all ratings to numeric
    mean_rating = df['rating'].mean()
    df['rating'] = df['rating'].fillna(mean_rating)

    # Clean reviews field, convert non-digit characters, and safely convert to integer
    df['reviews'] = df['reviews'].replace('[^\d]', '', regex=True)
    df['reviews'] = pd.to_numeric(df['reviews'], errors='coerce')
    mean_reviews = df['reviews'].mean()
    df['reviews'] = df['reviews'].fillna(mean_reviews).astype(int)

    # Process availability field to boolean
    df['availability'] = df['availability'].map(lambda x: 'In Stock' in x if pd.notnull(x) else False)

    # Push the cleaned and transformed data back to XCom for further use
    ti.xcom_push(key='transformed_data', value=df.to_json(orient='records'))
    logging.info("Data transformation successful")
    return "Transformation successful"

def write_amazon_data(**kwargs):
    """Writes transformed data into a CSV file."""
    ti = kwargs['ti']
    data_json = ti.xcom_pull(task_ids='transform_amazon_data', key='transformed_data')
    # Check if there is any data to process
    if not data_json:
        logging.error("No data received from transform_amazon_data.")
        return "No data to write."
    # Load the data from JSON
    data = json.loads(data_json)
    # Convert the data to a DataFrame
    df = pd.DataFrame(data)
    # Prepare the filename with a timestamp
    filename = f"amazon_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv('data/' + filename, index=False)
    logging.info(f"Data written to file: {filename}")
    return "Data written to file"
