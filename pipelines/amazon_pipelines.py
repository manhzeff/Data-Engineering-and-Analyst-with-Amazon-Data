
def get_amazon_page(url)
    import requests

    print("Getting amazon page....",url)

    try:
        response=requests.get(url,timeout=10)
        response.raise_for_status()

        return response.text
    except requests.RequestException as e:
        print(f"An error occured:{e}")