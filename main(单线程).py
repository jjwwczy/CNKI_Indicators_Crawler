import asyncio
import sqlite3
import re
from urllib.parse import urlencode
from playwright.async_api import async_playwright
import pandas as pd


 


def setup_indicator_database():
    """Create table for CNKI indicator data with a composite unique constraint."""
    conn = sqlite3.connect('crawler_results.db')
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS cnki_indicators (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        zcode TEXT,
        indicate_name TEXT,
        search_mode_one INTEGER,
        area TEXT,
        begin_year INTEGER,
        end_year INTEGER,
        data_type TEXT,

        time TEXT NOT NULL,
        region TEXT NOT NULL,
        indicator TEXT NOT NULL,
        value TEXT,
        unit TEXT,
        source TEXT,
        page_no TEXT,
        download_url TEXT,
        crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

        UNIQUE(indicator, region, time, source, page_no, download_url)
    )
    ''')
    conn.commit()
    conn.close()
    print("[Database] 指标库初始化完成（cnki_indicators）。")

def save_indicators_to_db(rows: list, params: dict):
    """Save parsed CNKI indicator rows into SQLite, ignoring duplicates."""
    if not rows:
        return 0
    conn = sqlite3.connect('crawler_results.db')
    cursor = conn.cursor()
    meta = (
        params.get('zcode'),
        params.get('indicateName'),
        int(params.get('searchModeOne')) if isinstance(params.get('searchModeOne'), (int, str)) and str(params.get('searchModeOne')).isdigit() else None,
        params.get('area'),
        params.get('beginYear'),
        params.get('endYear'),
        params.get('dataType'),
    )
    records = []
    for r in rows:
        records.append(meta + (
            r.get('time'),
            r.get('region'),
            r.get('indicator'),
            r.get('value'),
            r.get('unit'),
            r.get('source'),
            r.get('page_no'),
            r.get('download_url'),
        ))
    try:
        cursor.executemany('''
        INSERT OR IGNORE INTO cnki_indicators (
            zcode, indicate_name, search_mode_one, area, begin_year, end_year, data_type,
            time, region, indicator, value, unit, source, page_no, download_url
        ) VALUES (?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?)
        ''', records)
        conn.commit()
        print(f"[Database] 指标新增 {cursor.rowcount} 条（去重后）。")
        return cursor.rowcount
    except sqlite3.Error as e:
        print(f"[Database] 指标写入错误: {e}")
    finally:
        conn.close()


async def crawl_cnki_indicators(final_url: str, maxpages: int, get_download_links: bool, sort_by: str = None) -> list:
    """Use pure Playwright to load CNKI pages, handle pagination, and intercept download links."""
    all_rows = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False) # Set headless=True for background execution
        page = await browser.new_page()

        try:
            for page_num in range(maxpages):
                print(f"[CNKI] Processing page {page_num + 1}/{maxpages}...")
                
                if page_num == 0:
                    await page.goto(final_url, wait_until="networkidle")
                    # Handle sorting preference on the first page
                    if sort_by == '相关度':
                        try:
                            sort_button = await page.query_selector('.valueSearch_similar__2TkeM')
                            if sort_button:
                                print("[Sort] Clicking 'sort by similarity' button.")
                                await sort_button.click()
                                # Wait for the page to reload with the new sort order
                                await page.wait_for_load_state("networkidle")
                            else:
                                print("[Sort] 'Sort by similarity' button not found.")
                        except Exception as e:
                            print(f"[Sort] Error clicking sort button: {e}")
                else:
                    next_button = await page.query_selector('.btn-next:not([disabled])')
                    if not next_button:
                        print(f"[CNKI] No more pages found. Stopping at page {page_num}.")
                        break
                    await next_button.click()
                    await page.wait_for_load_state("networkidle")

                # --- Multi-Step Validation Scraping Strategy ---
                await page.wait_for_selector('tbody > tr', timeout=10000)
                candidate_rows = await page.query_selector_all('tbody > tr')
                validated_rows_for_page = []

                for row_handle in candidate_rows:
                    # 1. Check for a valid bounding box (must be physically rendered)
                    box = await row_handle.bounding_box()
                    if not box or box['height'] == 0:
                        continue

                    # 2. Check for correct cell count
                    cells = await row_handle.query_selector_all('td')
                    if len(cells) < 8:
                        continue

                    # 3. Check for non-empty critical data
                    indicator_text = await cells[3].inner_text()
                    if not indicator_text.strip():
                        continue

                    # --- Row is validated, now extract data ---
                    async def get_clean_text(cell):
                        text = await cell.inner_text()
                        return ' '.join(text.split()).strip()

                    row_data = {
                        'time': await get_clean_text(cells[1]),
                        'region': await get_clean_text(cells[2]),
                        'indicator': indicator_text.strip(),
                        'value': await get_clean_text(cells[4]),
                        'unit': await get_clean_text(cells[5]),
                        'source': await get_clean_text(cells[6]),
                        'page_no': await get_clean_text(cells[7]),
                        'download_url': ''
                    }

                    # 4. Handle download link if required
                    if get_download_links:
                        excel_icon = await row_handle.query_selector('.valueSearch_excel__3GKnk')
                        if excel_icon:
                            try:
                                async with page.expect_download(timeout=5000) as download_info:
                                    await excel_icon.click()
                                download = await download_info.value
                                row_data['download_url'] = download.url
                                await download.cancel()
                            except Exception as e:
                                print(f"[Download] Failed to intercept for '{row_data['indicator']}': {e}")
                    
                    validated_rows_for_page.append(row_data)

                print(f"[Validator] Found {len(candidate_rows)} total rows, validated {len(validated_rows_for_page)}.")
                all_rows.extend(validated_rows_for_page)

                await asyncio.sleep(1) # Delay before next page

        finally:
            await browser.close()
            
    return all_rows



def clear_indicator_database():
    """Deletes all records from the cnki_indicators table."""
    conn = sqlite3.connect('crawler_results.db')
    cursor = conn.cursor()
    try:
        cursor.execute('DELETE FROM cnki_indicators')
        conn.commit()
        print("[Database] All records have been deleted from cnki_indicators.")
    except sqlite3.Error as e:
        print(f"[Database] Error clearing table: {e}")
    finally:
        conn.close()

def main():
    """Main function to read tasks from Excel and crawl CNKI indicators."""
    setup_indicator_database()

    # --- Control Parameters ---
    maxpages = 5
    #是否获取excel的下载链接
    GET_DOWNLOAD_LINKS = False
    ##是否清空当前的数据库
    CLEAR_DATABASE_BEFORE_CRAWL = False

    if CLEAR_DATABASE_BEFORE_CRAWL:
        clear_indicator_database()

    try:
        # Read excel and keep empty cells as empty strings instead of NaN
        tasks_df = pd.read_excel('tasks.xlsx').fillna('')
    except FileNotFoundError:
        print("[Error] The 'tasks.xlsx' file was not found. Please create it and define your tasks.")
        return

    base_url = "https://data.cnki.net/trade/valueSearch/index"

    for index, task in tasks_df.iterrows():
        params = task.to_dict()
        
        # Filter out any parameters that are empty strings or None
        cleaned_params = {k: v for k, v in params.items() if v is not None and v != ''}

        print(f"\n--- [Task {index + 1}/{len(tasks_df)}] Starting task for indicator: {cleaned_params.get('indicateName')} in {cleaned_params.get('area', 'N/A')} ---")

        query_string = urlencode(cleaned_params)
        final_url = f"{base_url}?{query_string}"
        print(f"[CNKI] URL: {final_url}")

        try:
            sort_preference = params.get('sort')
            rows = asyncio.run(crawl_cnki_indicators(final_url, maxpages=maxpages, get_download_links=GET_DOWNLOAD_LINKS, sort_by=sort_preference))
            print(f"[CNKI] Parsed {len(rows)} records for this task.")
            if rows:
                save_indicators_to_db(rows, params)
        except Exception as e:
            print(f"!!!!!! [Error] An error occurred during task {index + 1}: {e} !!!!!!!")
            continue # Move to the next task

    print("\n--- All tasks completed. ---")

if __name__ == "__main__":
    main()

#在cmd使用官网下载地址下载。
# set PLAYWRIGHT_DOWNLOAD_HOST=https://playwright.azureedge.net
# playwright install