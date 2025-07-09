from src.booksdata import BookScraper
import asyncio

async def main():
    async with BookScraper() as scraper:
        df = await scraper.save_data("Default", "df")
        raw_data = await scraper.scrape("New Adult")
        assert not df.empty, raw_data
        return df, raw_data

if __name__ == "__main__":
    save_data_result, scrape_result = asyncio.run(main())
    print(f"Result of the save_data() method: \n{save_data_result}\nResult of the scrape() method: \n{scrape_result}")