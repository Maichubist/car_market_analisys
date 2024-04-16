import aiohttp
import asyncio
import pandas as pd
from bs4 import BeautifulSoup
from aiohttp import ClientError, ClientOSError

BASE_URL = 'https://auto.ria.com/uk/'

async def get_soup(session, url, retries=4):
    for attempt in range(retries):
        try:
            async with session.get(url) as response:
                if response.status == 200:
                   
                    charset = response.charset or 'utf-8'
                  
                    content = await response.text(encoding=charset, errors='ignore')
                    return BeautifulSoup(content, 'html.parser')
                else:
                    print(f"Failed to retrieve data: {response.status}")
            break 
        except (ClientError, ClientOSError, asyncio.TimeoutError) as e:
            print(f"Request failed: {e}, attempt {attempt + 1} of {retries}")
            if attempt + 1 == retries:
                return None
            await asyncio.sleep(2**attempt)

async def get_brands(session, url):
    soup = await get_soup(session, url)
    if soup:
        brands = soup.find_all('li', class_='list-item')
        return [i.text.lower() for i in brands][22:236]
    return []

def parse_car_data(section):
    name_year = section.select_one(".ticket-title a").text
    price_element = section.select_one(".price-ticket [data-currency='USD']").text
    mileage_element = section.select_one(".item-char.js-race").text
    location_element = section.select_one(".item-char.view-location.js-location").text
    characteristics = section.select(".definition-data .item-char")
    
    engine_info = "N/A"
    transmission = "N/A"
    for char in characteristics:
        text = char.get_text(strip=True).lower()  
        if "л." in text or "електро" in text or "бензин" in text or "дизель" in text or "газ" in text:
            engine_info = text
        elif "типтронік" in text or "автомат" in text or "механіка" or "варіатор" in text:  
            transmission = text
    engine = engine_info.split(',')
    if len(engine) > 1:
        volume = engine[1]
    else:
        volume = 'N/A'
    return [name_year[1:-5].strip(), name_year[-5:-1], price_element.replace(' ',''), mileage_element, engine[0], volume, transmission, location_element[2:-9],]


async def get_section(session, brand, pages):
    soup = await get_soup(session, f'https://auto.ria.com/uk/legkovie/{brand}/?page={pages+1}')
    if soup:
        print(f'https://auto.ria.com/uk/legkovie/{brand}/?page={pages+1}')
        selection = soup.select("section.ticket-item")
        if selection:
            return selection
    return []

async def get_car_info():
    
    async with aiohttp.ClientSession() as session:
        brands = await get_brands(session, BASE_URL)
        for brand in brands:
            brand = brand.replace(' ', '')
            cars_list_data = []
            
            soup = await get_soup(session, f'https://auto.ria.com/uk/legkovie/{brand}/')
            if soup:
                page_snap = soup.find_all('span', class_='page-item mhide')
                if page_snap:
                    last_page = int(page_snap[-1].text.replace(' ', ''))
                    tasks = [get_section(session, brand, i) for i in range(int(last_page))]
                else:
                    tasks = [get_section(session, brand, 0)]
                
                sections_lists = await asyncio.gather(*tasks)
                for sections in sections_lists:
                    for section in sections:
                        cars_list_data.append(parse_car_data(section))
            headers = ['Model', 'Year', 'Price', 'Mileage', 'Engine', 'Volume', 'Transmission', 'Location']
            data = pd.DataFrame(cars_list_data, columns=headers)
            
            data.to_excel(f'data\one_{brand}_cars_info.xlsx', index=False)
    
    return True


async def main():
    try:
        await get_car_info()
    except Exception as e:
        print(f"An error occurred: {e}")

asyncio.run(main())
