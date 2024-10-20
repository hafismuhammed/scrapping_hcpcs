import requests
import aiohttp
import asyncio
import pandas as pd
from bs4 import BeautifulSoup

BASE_URL = 'https://www.hcpcsdata.com'
FILE_HEADERS = ['Group', 'Category', 'Code', 'Long Description', 'Short Description']


async def get_webpage_response(session, url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }
    async with session.get(url, headers=headers) as response:
        return await response.text()

   
async def create_hcpc_data_csv():
    async with aiohttp.ClientSession() as session:
        response_text = await get_webpage_response(session, f'{BASE_URL}/Codes')
        soup = BeautifulSoup(response_text, 'html.parser')
        main_table = soup.find('table', class_='table-hover')
        main_table_rows = main_table.find_all('tr')
        
        csv_data = []

        tasks = []
        for row in main_table_rows[1:]:
            cols = row.find_all('td')
            group = f'HCPCS {cols[0].text.strip()}'
            category = cols[2].text.strip()
            
            # Find the group table (HCPCS codes table) page link and fetch the data from the page
            code_link = row.find('a')['href']
            tasks.append(
                get_group_code_data(
                    session, f'{BASE_URL}{code_link}', group, category
                    )
                )
        
        results = await asyncio.gather(*tasks)
        
        for result in results:
            csv_data.extend(result)
        
    # Save all HCPCS details to a CSV file
    df = pd.DataFrame(csv_data, columns=FILE_HEADERS)
    df.to_csv('hcps_code_dtl.csv', index=False)
    

async def get_group_code_data(session, group_url, group, category):
    page_content = await get_webpage_response(session, group_url)
    soup = BeautifulSoup(page_content, 'html.parser')
    
    group_code_table = soup.find('table')
    code_table_rows = group_code_table.find_all('tr')
    
    codes_data = []
    for code_table_row in code_table_rows[1:]:
        table_cols = code_table_row.find_all('td')
        code = table_cols[0].text.strip()
        long_desc = table_cols[1].text.strip()
        
        # Find the code detail page link and fetch the Short Description 
        code_dtl_link = table_cols[0].find('a')['href']
        code_dtl_url = f'{BASE_URL}{code_dtl_link}'
       
        code_dtl_page = await get_webpage_response(session, code_dtl_url)
        code_dtl_soup = BeautifulSoup(code_dtl_page, 'html.parser')
        
        code_dtl_table = code_dtl_soup.find('table', id='codeDetail')
        if code_dtl_table:
            code_dtl_row = code_dtl_table.find('tr').find_all('td')
            short_desc = code_dtl_row[1].text.strip()
        else:
            print(f"Warning: No 'codeDetail' table found for {code}")
            short_desc = "N/A"

        codes_data.append([group, category, code, long_desc, short_desc])
    
    return codes_data

                
if __name__ == '__main__':
    asyncio.run(create_hcpc_data_csv())
