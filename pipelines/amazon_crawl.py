import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import pandas as pd

custom_headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept-Language': 'da, en-gb, en',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Referer': 'https://www.google.com/'
}

visited_urls = set()
product_count = 0  # Initialize product count

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

def get_product_info(url):
    global product_count
    if product_count >= 30:
        return None

    response = requests.get(url, headers=custom_headers)
    if response.status_code != 200:
        print(f"Error in getting webpage: {url}")
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

    product_count += 1
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

def parse_listing(listing_url):
    global visited_urls, product_count
    if product_count >= 30:
        return []

    response = requests.get(listing_url, headers=custom_headers)
    print(response.status_code)
    soup_search = BeautifulSoup(response.text, "lxml")
    link_elements = soup_search.select("[data-asin] h2 a")
    page_data = []

    for link in link_elements:
        if product_count >= 30:
            break
        full_url = urljoin(listing_url, link.attrs.get("href"))
        if full_url not in visited_urls:
            visited_urls.add(full_url)
            product_info = get_product_info(full_url)
            if product_info:
                page_data.append(product_info)

    if product_count < 30:
        next_page_el = soup_search.select_one('a.s-pagination-next')
        if next_page_el:
            next_page_url = next_page_el.attrs.get('href')
            next_page_url = urljoin(listing_url, next_page_url)
            print(f'Scraping next page: {next_page_url}', flush=True)
            page_data += parse_listing(next_page_url)

    return page_data

def main():
    data = []
    search_url = "https://www.amazon.com/s?k=ps4&crid=1WLAMH40P8E1K&sprefix=ps4%2Caps%2C841&ref=nb_sb_ss_ts-doa-p_1_3"
    data = parse_listing(search_url)
    df = pd.DataFrame(data)
    df.to_csv("amazon_data.csv", index=False)

if __name__ == '__main__':
    main()
