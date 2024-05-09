import re

from pipelines.amazon_crawl import custom_headers


def clean_text(text):
    if text:
        # Normalize unicode characters to ASCII
        text = re.sub(r'[^\x00-\x7F]+', ' ', text)

        # Normalize white space, replace multiple spaces/tabs/newlines with a single space
        text = re.sub(r'\s+', ' ', text).strip()

        # Remove HTML tags
        text = re.sub(r'<[^>]*>', '', text)

        # Replace HTML entities with their corresponding characters
        text = text.replace('&amp;', '&')
        text = text.replace('&gt;', '>')
        text = text.replace('&lt;', '<')
        text = text.replace('&quot;', '"')
        text = text.replace('&#39;', "'")

        # Remove other web or scripting elements that might have slipped through, if any
        text = re.sub(r'[\[\]\{\}]', '', text)  # Removes any stray bracket characters

        return text
    return ''


# Example usage
sample_title = "  Skullcandy Crusher ANC 2 Over-Ear Noise Canceling... <b>NEW</b> &amp; Improved!"
sample_description = "The next generation of our iconic Crusher® headphones brings you refined sound. &#39;Listen to your music&#39; &amp; feel it too!"
cleaned_title = clean_text(sample_title)
cleaned_description = clean_text(sample_description)
print("Cleaned Title:", cleaned_title)
print("Cleaned Description:", cleaned_description)


import requests
from bs4 import BeautifulSoup
import pandas as pd
import re

def get_amazon_page(url):
    print("Getting amazon page...", url)
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"An error occurred: {e}")
        return None

def parse_amazon_html(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    return soup

def clean_text(text):
    if text:
        text = re.sub(r'[^\x00-\x7F]+', ' ', text)
        text = re.sub(r'\s+', ' ', text).strip()
        text = re.sub(r'<[^>]*>', '', text)
        text = text.replace('&amp;', '&')
        text = text.replace('&gt;', '>')
        text = text.replace('&lt;', '<')
        text = text.replace('&quot;', '"')
        text = text.replace('&#39;', "'")
        return text
    return ''

def extract_amazon_data(soup):
    try:
        title = soup.find(id='productTitle').get_text(strip=True)
        price = soup.find('span', {'class': 'a-price-whole'}).get_text(strip=True)
        rating = soup.find('span', {'class': 'a-icon-alt'}).get_text(strip=True)
        image = soup.find('img', {'id': 'landingImage'})['src']
        description = soup.find('div', {'id': 'feature-bullets'}).get_text(strip=True)
        reviews = soup.find('span', {'id': 'acrCustomerReviewText'}).get_text(strip=True)
        availability = soup.find('div', {'id': 'availability'}).get_text(strip=True)
        url = soup._last_retrieved_url  # or another method to capture the URL

        data = {
            'title': clean_text(title),
            'price': price,
            'rating': rating,
            'image': image,
            'description': clean_text(description),
            'reviews': reviews,
            'availability': availability,
            'url': url
        }
        return data
    except AttributeError as e:
        print("Failed to extract some of the data:", e)
        return None

def transform_amazon_data(data):
    transformed_data = {}
    if data:
        transformed_data['price'] = float(data['price'].replace('$', '').replace(',', ''))
        rating_text = data['rating'].split(' ')[0]
        transformed_data['rating'] = float(rating_text) if rating_text.replace('.', '', 1).isdigit() else None
        reviews_text = data['reviews'].split(' ')[0].replace(',', '')
        transformed_data['reviews'] = int(reviews_text) if reviews_text.isdigit() else None
        transformed_data['availability'] = 'In Stock' in data['availability']
        transformed_data['title'] = data['title']
        transformed_data['image'] = data['image']
        transformed_data['description'] = data['description']
        transformed_data['url'] = data['url']
    return transformed_data

def write_amazon_data(data, filename='headphones.csv'):
    df = pd.DataFrame([data])
    df.to_csv(filename, mode='a', index=False, header=not pd.read_csv(filename).empty)

# Example usage
url = "https://www.amazon.com/example-product-url"
html_content = get_amazon_page(url)
if html_content:
    soup = parse_amazon_html(html_content)
    raw_data = extract_amazon_data(soup)
    if raw_data:
        cleaned_data = transform_amazon_data(raw_data)
        write_amazon_data(cleaned_data)



import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
from datetime import datetime
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log


def get_amazon_page(url):
    """Retrieve HTML content of the Amazon product page."""
    try:
        response = requests.get(url, headers=custom_headers)
        response.raise_for_status()  # Will raise an HTTPError for bad requests (400 or 500 level status codes)
        return response.text
    except requests.RequestException as e:
        log.error(f"Failed to retrieve page: {e}")
        return None

def extract_amazon_data(url):
    """Extracts detailed product information from an Amazon product page."""
    html_content = get_amazon_page(url)
    if not html_content:
        return None

    soup = BeautifulSoup(html_content, 'lxml')
    title = soup.select_one("#productTitle").text.strip() if soup.select_one("#productTitle") else "N/A"
    price = soup.select_one('span.a-offscreen').text if soup.select_one('span.a-offscreen') else "N/A"
    rating = soup.select_one("#acrPopover").attrs.get("title").split(' out of')[0] if soup.select_one("#acrPopover") else "N/A"
    image = soup.select_one("#landingImage").attrs.get("src") if soup.select_one("#landingImage") else "N/A"
    description = soup.select_one("#productDescription").text.strip() if soup.select_one("#productDescription") else "N/A"
    reviews = soup.find("span", attrs={'id': 'acrCustomerReviewText'}).text.strip() if soup.find("span", attrs={'id': 'acrCustomerReviewText'}) else "N/A"
    availability = soup.find("div", attrs={'id': 'availability'}).find("span").text.strip() if soup.find("div", attrs={'id': 'availability'}) else "N/A"

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

def transform_amazon_data(data):
    """Transform the extracted data for storage or further processing."""
    try:
        data['price'] = float(data['price'].replace('$', '').replace(',', ''))
        data['rating'] = float(data['rating']) if data['rating'] != "N/A" else None
        data['reviews'] = int(data['reviews'].split()[0].replace(',', '')) if 'reviews' in data and data['reviews'] != "N/A" else 0
        return data
    except Exception as e:
        log.error(f"Error transforming data: {e}")
        return None

def write_amazon_data(ti):
    """Write data to a CSV file, data is pulled from XCom."""
    try:
        # Pull data from XCom
        data_json = ti.xcom_pull(key='rows', task_ids='transform_amazon_data')
        if not data_json:
            log.error("No data received for writing.")
            return None

        # Convert JSON string to dictionary if necessary, then to DataFrame
        data = json.loads(data_json) if isinstance(data_json, str) else data_json
        df = pd.DataFrame([data])

        # Construct file name with current datetime, ensuring proper string formatting
        current_date = datetime.now().date()
        current_time = datetime.now().time().strftime('%H_%M_%S')
        file_name = f'amazon_data_cleaned_{current_date}_{current_time}.csv'
        df.to_csv(file_name, index=False)
        log.info(f"Data successfully written to {file_name}")

    except Exception as e:
        log.error(f"Error during data writing: {e}")


