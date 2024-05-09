from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import json
from airflow.utils.log.logging_mixin import LoggingMixin



log = LoggingMixin().log

# Custom headers as used in your amazon_crawl script
custom_headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept-Language': 'da, en-gb, en',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Referer': 'https://www.google.com/'
}

# Initialize global variables
product_count = 0
visited_urls = set()

def fetch_page(url):
    """Fetches HTML content from a given Amazon product page URL."""
    log.info(f"Fetching Amazon page: {url}")
    try:
        response = requests.get(url, headers=custom_headers, timeout=10)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        log.error(f"Failed to fetch page: {e}")
        return None

def parse_html(html_content):
    """Parses HTML content and extracts necessary data."""
    soup = BeautifulSoup(html_content, 'lxml')

    title_element = soup.find(id='productTitle')
    price_element = soup.select_one('span.a-offscreen')
    rating_element = soup.select_one("#acrPopover")
    image_element = soup.select_one("#landingImage")
    description_element = soup.select_one("#productDescription")

    title = title_element.text.strip() if title_element else 'N/A'
    price = price_element.text.strip() if price_element else 'N/A'
    rating = rating_element['title'].replace(" out of 5 stars", "").strip() if rating_element else 'N/A'
    image_url = image_element['src'] if image_element else 'N/A'
    description = description_element.text.strip() if description_element else 'N/A'

    review_count = soup.find("span", attrs={'id': 'acrCustomerReviewText'}).string.strip() if soup.find("span", attrs={'id': 'acrCustomerReviewText'}) else 'N/A'
    availability_element = soup.find("div", attrs={'id': 'availability'})
    availability = availability_element.find("span").string.strip() if availability_element and availability_element.find("span") else 'N/A'

    return {
        "title": title,
        "price": price,
        "rating": rating,
        "image_url": image_url,
        "description": description,
        "reviews": review_count,
        "availability": availability,
        "url": url
    }

def extract_amazon_data(**kwargs):
    """Extracts product data from Amazon and handles pagination."""
    global product_count
    ti = kwargs['ti']
    url = kwargs['url']
    if product_count >= 30:
        log.info("Product count limit reached.")
        return

    html_content = fetch_page(url)
    if not html_content:
        return

    product_data = parse_html(html_content)
    ti.xcom_push(key='product_data', value=json.dumps(product_data))
    log.info(f"Data extracted for URL: {url}")
    product_count += 1

    if product_count < 30:
        next_page_url = urljoin(url, soup.find('a', {'class': 's-pagination-next'})['href'])
        log.info(f"Scraping next page: {next_page_url}")
        return extract_amazon_data(ti=ti, url=next_page_url)


def transform_amazon_data(**kwargs):
    """Transforms raw data fetched from Amazon into structured format, applying data cleaning."""
    ti = kwargs['ti']
    data_json = ti.xcom_pull(task_ids='extract_amazon_data', key='product_data')
    data = json.loads(data_json)

    # Convert data into a DataFrame for easier manipulation
    df = pd.DataFrame([data])

    # Handle non-numeric and unexpected strings in 'price' column before converting
    df['price'] = df['price'].replace('[\$,]', '', regex=True)
    df['price'] = pd.to_numeric(df['price'], errors='coerce')

    # Calculate mean for price and fill missing values
    mean_price = df['price'].mean()
    df['price'] = df['price'].fillna(mean_price)

    # Ensure rating is treated as string if it's not null, then convert the first part to float
    df['rating'] = df['rating'].apply(lambda x: float(x.split()[0]) if pd.notnull(x) and 'out of' in x else x)
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
    log.info("Data transformation successful")
    return "Transformation successful"

def write_amazon_data(**kwargs):
    """Writes transformed data into a CSV file."""
    ti = kwargs['ti']
    data_json = ti.xcom_pull(task_ids='transform_amazon_data', key='transformed_data')
    data = json.loads(data_json)
    df = pd.DataFrame(data)
    filename = f"amazon_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv('data/' + filename, index=False)
    log.info(f"Data written to file: {filename}")
    return "Data written to file"
