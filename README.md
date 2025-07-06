Installation:
Install via `git`:
```bash
git clone https://github.com/your_login/booksdata.git
cd booksdata
```
or press the "Download Zip" button (on GitHub) to install without `git`.
Then open a terminal in the project folder and execute:
```bash
# Create virutal environment (recommended)
python -m venv venv

# Activate it
# for Windows:
venv\Scripts\activate
# for Linux/Mac:
source venv/bin/activate

# Install libraries
pip install -r requirements.txt
```
Description:
The library supports scraping only one site (https://books.toscrape.com). The `BookScraper` class, designed for data scraping one category, requires an asynchronous context manager - it allows implicit closing of an `aiohttp.ClientSession` instance. The class covers not all cases, but the most common cases (scraping and a data export), while allowing data processing and other operations. The class provides two asynchronous methods: `scrape()`, which returns raw data about each product from the specified category as a list of dictionaries, and `save_data()`, which processes the raw data returned by `scrape()` (Type casting of some columns to `float` from `str` and other operations, more detailed in the docstring of `save_data()`)  and then exports it in one of 3 supported formats: `"df"` (the `pandas.DataFrame` class), `"csv"` (using `pandas.DataFrame.to_csv`), `"excel"` (using `pandas.DataFrame.to_excel`). To prevent excess abstractions and allow a simple export, the first format is intended to be used by users familiar with `pandas` and need an advanced processing, an export in unsupported formats and the last two formats are intended to be used by users unfamiliar with `pandas` and need a simple scraping and a data export. 

Examples:
If you compute an average book price of the "Default" category, you can save data in the `"df"` format and use the respective `pandas` methods:
```python
async with BookScraper() as bookscraper:
	data = await bookscraper.save_data("Default", "df")
	data["Price (excl. tax)"].mean()
```
You can use the `to_json()` method of the `pandas.DataFrame` class to save data in the json format:
```python
async with BookScraper() as bookscraper:
	data = await bookscraper.save_data("Default", "df")
	data.to_json("data.json")
```
Should you specify `"csv"` format and pass a path to future .csv file into the `path` parameter to export data in the supported csv format (it's also required for  the `"excel"` format, but not for the `"df"` format):
```python
async with BookScraper() as bookscraper:
	await bookscraper.save_data("Default", "csv", path="default_books.csv")
```
Note: When using `"csv"` or `"excel"` format the `save_data()` method returns `None`. 

For more information about operations with `pandas.DataFrame` and other opportunities provided by `pandas`, see the `pandas` documentation: https://pandas.pydata.org/docs/ 
Learn more about two provided methods (`save_data()`, `scrape()`) in the method's docstrings. 
