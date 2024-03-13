import asyncio
import csv

import aiohttp
from lxml import html


async def get_product_urls(response):
    """
    Collects all product URL from a listing page response.
    Args:
        response:
            aiohttp response
    Returns:
        list of urls. List of product page urls returned.
    """
    parser = html.fromstring(await response.text())
    product_urls = parser.xpath('//li/a[contains(@class, "product__link")]/@href')
    return product_urls


def verify_response(response):
    """
    Verify if we received valid response or not
    Args:
        response:
            aiohttp response
    Returns:
        based on response status returns boolean value
    """
    return True if response.status == 200 else False


async def send_request(url):
    """
    Send request and handle retries.
    Args:
        url:
            request url
    Returns:
        Response we received after sending request to the URL.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.5"
    }
    max_retry = 3
    for retry in range(max_retry):
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                if verify_response(response):
                    await response.text()
                    return response

    print("Invalid response received even after retrying. URL with the issue is:", url)
    raise Exception("Stopping the code execution as invalid response received.")


async def get_next_page_url(response):
    """
    Collect pagination URL.
    Args:
        response:
            aiohttp response
    Returns:
        next listing page url
    """
    parser = html.fromstring(await response.text())
    next_page_url = parser.xpath('(//a[@class="next page-numbers"])[1]/@href')[0]
    return next_page_url


def clean_stock(stock):
    """
    Clean the data stock by removing unwanted text present in it.
    Args:
        stock:
            uncleaned stock string
    Returns:
        Stock data. Stock number will be returned by removing extra string.
    """
    stock = clean_string(stock)
    if stock:
        stock = stock.replace(' in stock', '')
        return stock
    else:
        return None


def clean_string(list_or_txt, connector=' '):
    """
    Clean and fix list of objects received. We are also removing unwanted white spaces.
    Args:
        list_or_txt:
            uncleaned string
        connector:
            connecting string
    Returns:
        Cleaned string.
    """
    if not list_or_txt:
        return None
    return ' '.join(connector.join(list_or_txt).split())


async def get_product_data(response):
    """
    Collect all details of a product.
    Args:
        response: aiohttp response
    Returns:
        All data of a product.
    """
    parser = html.fromstring(await response.text())
    product_url = response.url
    title = parser.xpath('//h1[contains(@class, "product_title")]/text()')
    price = parser.xpath('//p[@class="price"]//text()')
    stock = parser.xpath('//p[contains(@class, "in-stock")]/text()')
    description = parser.xpath('//div[contains(@class,"product-details__short-description")]//text()')
    image_url = parser.xpath('//div[contains(@class, "woocommerce-product-gallery__image")]/a/@href')
    product_data = {
        'Title': clean_string(title), 'Price': clean_string(price), 'Stock': clean_stock(stock),
        'Description': clean_string(description), 'Image_URL': clean_string(list_or_txt=image_url, connector=' | '),
        'Product_URL': product_url}
    return product_data


def save_data_to_csv(data, filename):
    """
    save list of dict to csv.
    Args:
        data: Data to be saved to csv
    Returns:
        filename: Filename of csv
    """
    keys = data[0].keys()
    with open(filename, 'w', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=keys)
        writer.writeheader()
        writer.writerows(data)


async def start_scraping():
    """
    Starting function.
    """

    listing_page_tasks = []
    for listing_page_number in range(1, 6):
        listing_page_url = f"https://scrapeme.live/shop/page/{listing_page_number}/"
        listing_page_request = send_request(listing_page_url)
        listing_page_tasks.append(listing_page_request)

    listing_page_responses = await asyncio.gather(*listing_page_tasks)

    product_urls = []
    for each_listing_page_response in listing_page_responses:
        products_from_current_page = await get_product_urls(each_listing_page_response)
        product_urls.extend(products_from_current_page)

    product_request_tasks = []
    for url in product_urls:
        product_request = send_request(url)
        product_request_tasks.append(product_request)

    product_responses = await asyncio.gather(*product_request_tasks)

    results = []
    for each_product_response in product_responses:
        product_result = await get_product_data(each_product_response)
        results.append(product_result)

    save_data_to_csv(data=results, filename='scrapeme_live_Python_data.csv')
    print('Data saved as csv')


if __name__ == "__main__":
    asyncio.run(start_scraping())
