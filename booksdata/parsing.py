from asyncio import TaskGroup
from aiohttp import (ClientSession, ClientTimeout,
                     TCPConnector, BaseConnector,
                     ClientError, ConnectionTimeoutError)
from aiohttp.abc import AbstractResolver
from aiohttp.resolver import AsyncResolver
from bs4 import BeautifulSoup
from functools import partial
from typing import Optional, Callable, Mapping, Any, Sequence, Iterable
from pandas import DataFrame
from booksdata.settings import load_config

base_connector_settings, base_features, base_headers, base_name_dns_servers, base_payload, base_timeout_settings, base_url = load_config()

class BookScraper:
    def __init__(self,
                 timeout_settings: Optional[dict[str, float]] = base_timeout_settings,
                 connector_settings: Optional[Mapping[str, Any]] = base_connector_settings,
                 headers: Mapping[str, str] = base_headers,
                 data: Any = base_payload,
                 name_dns_servers: Optional[Iterable[str]] = base_name_dns_servers,
                 parser_features: str | Sequence[str] = base_features):
        """
        Initializes the class with default values
        """

        self._headers = headers
        self._payload = data
        self._timeout: Optional[ClientTimeout] = ClientTimeout(**timeout_settings) if timeout_settings else None
        self._resolver: Optional[AbstractResolver] = AsyncResolver(nameservers=name_dns_servers) if name_dns_servers else None
        self._connector: Optional[BaseConnector] = TCPConnector(**connector_settings, resolver=self._resolver) if connector_settings or self._resolver else None
        self._html_parser: Callable[[str], BeautifulSoup] = partial(lambda features, markup: BeautifulSoup(markup, features), parser_features)
        self._session: Optional[ClientSession] =  None

    async def __aenter__(self):
        if not self._session or self._session.closed:
            self._session = ClientSession(base_url, timeout=self._timeout, headers=self._headers, connector=self._connector)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._session:
            await self._session.close() if not self._session.closed else print("ClientSession have already closed")

    async def _request_to_page(self, path: str) -> str:
        """
        Performs a single request to the specified path


        Parameters
        ----------
        path: str
            The resource path to be requested


        Returns
        -------
        str
            A raw HTML string of page


        Raises
        ------
        aiohttp.ClientResponseError: a response code is not in the range from 200 to 400

        aiohttp.ConnectionTimeoutError: the timeout is reached


        Notes
        -----
        Performs a request to the specified path (not to the base URL). The URL of main page is passed to  ClientSession instance.
        """
        async with self._session.get(path, data=self._payload) as response:
            response.raise_for_status()
            print(f"No errors: {response.status} : {response.url}")
            return await response.text()

    def _parse_category_paths(self, markup: str) -> dict[str, str]:
        """
        Parses an HTML markup and extracts category paths in a dict, that contains category names as keys and category paths as values


        Parameters
        ----------
        markup: str
            A raw HTML string used by BeautifulSoup, that contains tags representing categories


        Returns
        -------
        dict[str, str]
            A dictionary, that contains category names as keys and the paths to the categories as values


        Raises
        ------
        TypeError: an invalid type of incoming markup, a href attribute value type doesn't support slicing


        Notes
        -----
        If a raw HTML string doesn't contain tags representing categories or tags representing categories don't have a href attribute,
        it returns an empty dictionary or a dictionary with category names as keys and empty strings as values respectively
        """
        return {category_element.get_text(strip=True).lower() : category_element.get("href")[3:-11]
                for category_element in self._html_parser(markup).select("ul a[href^='../books/']")}

    def _parse_product_paths(self, markup: str) -> list[str]:
        """
        Parses an HTML markup and extracts the product paths into a list


        Parameters
        ----------
        markup: str
            A raw HTML string used by BeautifulSoup that contains tags representing product cards


        Returns
        -------
        list[str]
            A list that contains paths to products


        Raises
        ------
        TypeError: an invalid type of incoming markup, a href attribute value type doesn't support slicing


        Notes
        -----
        If a raw HTML string doesn't contain tags representing product cards or tags representing product cards don't have a href attribute,
        it returns a list of empty strings.
        """
        return [element.get("href")[9:]
                for element in self._html_parser(markup).select("article.product_pod > h3 > a")]

    def _get_page_count(self, markup: str) -> range:
        """
        Parses an HTML markup and returns a range from second page to the last page.


        Parameters
        ----------
        markup: str
            A raw HTML string used by BeautifulSoup that contains a last page element which can be converted to int or doesn't contain this element


        Returns
        -------
        range
            A range from second page to the last page


        Raises
        ------
        ValueError: an invalid type of last page element value

        TypeError: an invalid type of incoming markup


        Notes
        -----
        If a raw HTML string doesn't have a last page element, it returns range(0)
        """
        if pagination_element := self._html_parser(markup).select_one("ul.pager li.current"):
            return range(2, int(pagination_element.get_text(strip=True)[-1]) + 1)
        else:
            return range(0)

    def _convert_rating(self, rating: str) -> Optional[float]:
        """
        Converts a rating string into the float type
        """
        if not isinstance(rating, str):
            raise TypeError("Rating must be string")

        match rating.lower():
            case "one":
                return 1.0
            case "two":
                return 2.0
            case "three":
                return 3.0
            case "four":
                return 4.0
            case "five":
                return 5.0
            case _:
                raise ValueError("Rating must be parsable")

    def _parse_prices(self, df: DataFrame) -> DataFrame:
        """
        Extracts prices and taxes from a DataFrame column and converts them into the float type, if it's possible
        """
        if not isinstance(df, DataFrame):
            raise TypeError("df must be Dataframe instance")

        if df.columns.intersection(["Price (excl. tax)", "Price (incl. tax)", "Tax"]).empty:
            raise ValueError("A tax column and product price columns don't exist")

        return extracted.astype(float) if not (extracted := df[["Price (excl. tax)", "Price (incl. tax)", "Tax"]].replace({r"(?i)(Free|Give Away|Gratis|No charge)" : "0", r"[^\d.]" : ""}, regex=True)).isna().any().any() else extracted

    def _parse_currency(self, df: DataFrame) -> DataFrame:
        """
        Extracts currencies into a new column
        """
        if not isinstance(df, DataFrame):
            raise TypeError("df must be Dataframe instance")

        if df.columns.intersection(["Price (incl. tax)"]).empty:
            raise ValueError("Product price columns don't exist")

        return df["Price (incl. tax)"].str.extract(r".*([$£€]).*")[0]

    def _parse_product_availability(self, df: DataFrame) -> DataFrame:
        """
        Converts a product availability column ("Availability") into the float type, if type casting is possible
        """
        if not isinstance(df, DataFrame):
            raise TypeError("df must be Dataframe instance")

        if df.columns.intersection(["Availability"]).empty:
            raise ValueError("Product availability column doesn't exist")

        return extracted.astype(float) if not (extracted := df["Availability"].replace({r"(?i)(Not in stock|Out of stock|Sold out|Unavailable)": "0", r"[^\d.]" : ""}, regex=True)).isna().any().any() else extracted


    def _parse_product_info(self, markup: str, url: str) -> Optional[dict[str, str]]:
        """
        Parses product information from an HTML string
        """
        product_page_parser = self._html_parser(markup)
        if product_name := product_page_parser.select_one("article.product_page div.col-sm-6.product_main > h1").get_text(strip=True):
            if table := product_page_parser.select_one("article.product_page > table.table.table-striped"):
                    return {**{
                        "Product name": product_name,
                        "URL" : url,
                        "Rating" : product_page_parser.select_one("article.product_page div.col-sm-6.product_main > p[class^=star-rating ]").get("class", "")[1]},
                            **{
                        tr.select_one("th").get_text(strip=True) : tr.select_one("td").get_text(strip=True)
                        for tr in table.select("tr")}
                            }

            return {"Product name" : product_name,
                    "URL" : url,
                    "Rating" : product_page_parser.select_one("article.product_page div.col-sm-6.product_main > p[class^=star-rating ]").get("class", "")[1]}



    async def _collect_paths(self, category: str) -> list[str]:
        """
        Collects all paths to products of the specified category
        """
        if not (path_to_category := self._parse_category_paths(await self._request_to_page("category/books_1/index.html")).get(category.lower())):
            raise ValueError(f"Not found the category: {path_to_category}")

        markup = await self._request_to_page(f"category/{path_to_category}/index.html")
        results = []
        results.extend(self._parse_product_paths(markup))

        if page_count := self._get_page_count(markup):
            try:
                async with TaskGroup() as tg:
                    tasks = [tg.create_task(self._request_to_page(f"category/{path_to_category}/page-{page_number}.html"))
                            for page_number in page_count]

            except* ClientError as err:
                print(f"The HTTP Error: {err}")

            except* ConnectionTimeoutError as err:
                print(f"The timeout is reached: {err}")

            except* (AttributeError, IndexError) as err:
                print(f"The parsing result in the error: {err}")

            except* KeyboardInterrupt as err:
                print(f"The user interrupts the execution: {err}")

            except* SystemExit as err:
                print(f"The system exit cause the error: {err}")

            except* Exception as err:
                print(f"The error: {err}")

            else:
                for task in tasks:
                    results.extend(self._parse_product_paths(task.result()))
        if not results:
            print("Paths to products of the specified category are not found")
        return results

    async def scrape(self, category: str) -> list[dict[str, str]]:
        """
        Extracts raw data about each product in the specified category into a list of dictionaries


        Parameters
        ----------
        category: str
            An existing book category, a book category search is case-insensitive and strict, for example,
            "Default" matches "DEFAULT", but not "Default Category"


        Returns
        -------
        list[dict[str, str]]
            A list of dictionaries in which dictionary representing raw data about product.
            All values of each dictionary in the str type.
            The keys of dictionaries:
                "Product name": a product name, for example, "A Light in the Attic"
                "URL": a product URL, for example, "https://books.toscrape.com/catalogue/a-light-in-the-attic_1000/index.html"
                "Rating": a rating of product, for example, "Three"
                "UPC": a UPC of product, for example, "a897fe39b1053632"
                "Product Type": a product type, for example, "Books"
                "Price (excl. tax)":a product price exclude tax, for example, "£51.77"
                "Price (incl. tax)": a product price include tax, for example, "£51.77"
                "Tax":  a product tax, for example, "£0.00"
                "Availability": a number of product in stock, for example, "In stock (22 available)"
                "Number of reviews": a number of product reviews, for example, "0"


        Raises
        ------
        ValueError: the specified category is not found

        AttributeError, IndexError: the parsing result in the error

        ConnectionTimeoutError: the timeout is reached

        ClientError: the HTTP Error


        Notes
        -----
        Supports pagination (Each product on each page is processed).
        """
        paths = await self._collect_paths(category)

        if paths:
            try:
                async with TaskGroup() as tg:
                    tasks = {base_url + path : tg.create_task(self._request_to_page(path)) for path in paths}

            except* ClientError as err:
                print(f"The HTTP Error: {err}")

            except* ConnectionTimeoutError as err:
                print(f"The timeout is reached: {err}")

            except* (AttributeError, IndexError) as err:
                print(f"The parsing result in the error: {err}")

            except* KeyboardInterrupt as err:
                print(f"The user interrupts the execution: {err}")

            except* SystemExit as err:
                print(f"The system exit cause the error: {err}")

            except* Exception as err:
                print(f"The error: {err}")

            else:
                return [self._parse_product_info(task.result(), url) for url, task in tasks.items()]

    async def save_data(self, category: str,
                        fmt: str,
                        path: Optional[str] = None,
                        product_name_as_index: bool = False,
                        rating_as_float: bool = True,
                        parse_prices: bool = True,
                        parse_currency: bool = True,
                        parse_product_availability: bool = True) -> Optional[DataFrame]:
        """
        Converts data about all products in the specified category returned by scrape()
        into one of the three supported formats: "df" (using pandas.DataFrame), "csv" (using pandas.DataFrame.to_csv), "excel" (using pandas.DataFrame.to_excel).


        Parameters
        ----------
        category: str
            An existing book category, passed into scrape()

        fmt: str
            A format of returned data must be one of the mentioned format: "df", "csv", "excel"

        path: Optional[str]
            A path to file is required, when converting data into "csv" or "excel" formats. This parameter is not required for "df" format.

        product_name_as_index: bool = False
            This parameter determines whether a product name column is used as index for pandas.Dataframe. This parameter is False by default. For example, if the product_name_as_index parameter is True, you would see a result like this:
                                                                                                           URL  ...  Currency
                Product name                                                                                    ...
                Without Borders (Wanderlove #1)              https://books.toscrape.com/catalogue/without-b...  ...         £
                The Mistake (Off-Campus #2)                  https://books.toscrape.com/catalogue/the-mista...  ...         £
                The Matchmaker's Playbook (Wingmen Inc. #1)  https://books.toscrape.com/catalogue/the-match...  ...         £
                The Hook Up (Game On #1)                     https://books.toscrape.com/catalogue/the-hook-...  ...         £
                Shameless                                    https://books.toscrape.com/catalogue/shameless...  ...         £
                Off Sides (Off #1)                           https://books.toscrape.com/catalogue/off-sides...  ...         £

        rating_as_float: bool = True
            This parameter determines whether a rating is converted from the str type into the float type. This parameter is True by default. For example, if rating_as_float is False, you would see a rating column like this:
                0      Two
                1    Three
                2      One
                3     Five
                4    Three
                5     Five
                Name: Rating, dtype: object

        parse_prices: bool = True
            This parameter determines whether a tax column and product price columns ("Price (excl. tax)", "Price (incl. tax)", "Tax") are converted into the float type. For example, if parse_prices is False, you would see columns with prices and taxes like this:
                  Price (excl. tax) Price (incl. tax)    Tax
                0            £45.07            £45.07  £0.00
                1            £43.29            £43.29  £0.00
                2            £55.85            £55.85  £0.00
                3            £36.29            £36.29  £0.00
                4            £58.35            £58.35  £0.00
                5            £39.45            £39.45  £0.00

        parse_currency: bool = True
            This parameter determines whether a new currency ("Currency") is created. This parameter is True by default. For example, if parse_currency is False, a currency column doesn't exist.

        parse_product_availability: bool = True
            This parameter determines whether a product availability column ("Availability") is converted into the float type. This parameter is True by default. For example, if parse_product_availability is False, you would see a product availability column like this:
                0    In stock (16 available)
                1    In stock (15 available)
                2    In stock (15 available)
                3     In stock (1 available)
                4     In stock (1 available)
                5     In stock (1 available)
                Name: Availability, dtype: object


        Returns
        -------
        Optional[pandas.DataFrame]
            The "df" format is selected. Returns None if the "csv" or "excel" format is selected (data is saved in a file)


        Raises
        ------
        ValueError:
            An unsupported format


        Notes
        -----
        if a list of dictionaries returned by scrape() is empty, it returns an empty DataFrame instance
        """

        if not (data := await self.scrape(category)):
            print("The scrape() method didn't return data")
            return DataFrame()

        df = DataFrame(data)

        if product_name_as_index:
            df = df.set_index("Product name")

        if rating_as_float:
            df["Rating"] = df["Rating"].apply(self._convert_rating)

        if parse_currency:
            df["Currency"] = self._parse_currency(df)

        if parse_prices:
            df[["Price (excl. tax)", "Price (incl. tax)", "Tax"]] = self._parse_prices(df)

        if parse_product_availability:
            df["Availability"] = self._parse_product_availability(df)

        match fmt:
            case "df":
                return df
            case "csv":
                df.to_csv(path)
            case "excel":
                df.to_excel(path)
            case _:
                raise ValueError(f"Unsupported format: {fmt}")