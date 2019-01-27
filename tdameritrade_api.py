"""
Module to facilitate working with TDAmeritrades
web based API.

Brent Maranzano
July 29, 2018

Example:
    >>td = TDAmeritrade("account_number.txt", "oAuth.txt")
    >>td.get_watchlist()

Class:
    TDAmeritrade

"""
import urllib.request
from urllib.error import HTTPError
from urllib import parse
import logging
import json
from datetime import datetime, timedelta
import numpy as np
import pandas as pd


class TDAmeritradeAPI:
    """Class to format http urls to conform to the TDAmeritrade API.  Sends
    the http request (GET, PUT, or POST). Saves the response (if any) into
    class variable response.

    Class variables
        response (str): Last request response obtained from TD Ameritrade API
        rpc.
        logger (logger): Logger
        account_number: TD Ameritrade account number to post rpcs
        oauth_certificate: OAuth 2.0 certificate used to validate rpc

    Example:
    >>td = TDAmeritrade.create_api_from_account_file(
            filename="my_account_info.txt")
    >>td.get_watchlist()

    """
    def __init__(self, account_number=None, oauth_certificate=None):
        """Create the API class using the account number and oauth certificate
        to validate requests.

        Arguments:
            account_number (str): Account number.
            oauth_certificate (str): oAuth2.0 certificate.
        """
        self._setup_logging()

        if account_number is None:
            self._logger.error(
                "TDAmeritrade instantiation requires an account number.")
        if oauth_certificate is None:
            self._logger.error(
                "TDAmeritrade instantiation requires oAuth certificate.")

        self.account_number = account_number
        self.oauth_certificate = oauth_certificate
        self.response = None

    def _setup_logging(self):
        """Set up a logger.
        """
        # create logger with 'spam_application'
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)
        file_handle = logging.FileHandler('tdameritrade.log')
        file_handle.setLevel(logging.INFO)
        console_handle = logging.StreamHandler()
        console_handle.setLevel(logging.ERROR)
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        formatter = logging.Formatter(log_format)
        file_handle.setFormatter(formatter)
        console_handle.setFormatter(formatter)
        logger.addHandler(file_handle)
        logger.addHandler(console_handle)
        self._logger = logger

    @staticmethod
    def get_access_token(client_id=None, callback_url=None,
                         refresh_token=None):
        """Get an access token using the refesh token.
        see https://developer.tdameritrade.com/content/simple-auth-local-apps

        Arguments:
        client_id (str): API client ID.
        callback_url (str): API callback URL
        refresh_token (str): Refresh token
        """
        url = "https://api.tdameritrade.com/v1/oauth2/token"
        request = urllib.request.Request(url)

        client_id = parse.quote(client_id)
        callback_url = parse.quote(callback_url)
        refresh_token = parse.quote(refresh_token)

        data = "grant_type=refresh_token" + \
            "&refresh_token={}".format(refresh_token) + \
            "&client_id={}".format(client_id) + \
            "&redirect_uri={}".format(callback_url)

        data = data.encode("utf-8")

        request.add_header(key="Content-Type",
                           val="application/x-www-form-urlencoded")
        request.add_header(key="Content-Length", val=len(data))
        try:
            response = urllib.request.urlopen(request, data=data)
        except HTTPError as http_error:
            raise http_error

        return json.loads(response.read().decode())["access_token"]

    @classmethod
    def create_api_from_account_file(cls, filename=None):
        """Instantiate the class using the filenames to get the
        necessary account information.

        Arguments
        filename (str): Name of file with account, API, and token information
        """
        with open(filename) as file_obj:
            information = json.load(file_obj)

        access_token = TDAmeritradeAPI.get_access_token(
            client_id=information["client_id"],
            callback_url=information["callback_url"],
            refresh_token=information["refresh_token"]
        )

        return cls(account_number=information["account_number"],
                   oauth_certificate=access_token)

    def _send_request(self, url, data=None):
        """Make the rpc. Creates a request object from a base url contatenated
        with the additional url information provided by the argument. Adds
        headers, such as content type, length, ..., as well as the OAuth2.0
        header. For PUT calls, converts the data object to JSON and encodes.

        Arguments:
        url (str): Specific url details to add to the base url for the request.
        data (dict): Dictionary with details required for the request.
        """
        base_url = "https://api.tdameritrade.com/v1/"
        url = base_url + url
        request = urllib.request.Request(url)
        request.add_header("Authorization", "Bearer {}"
                           .format(self.oauth_certificate).encode("utf-8"))
        if data is None:
            try:
                response = urllib.request.urlopen(request)
                self._logger.info("URL: %s", request.get_full_url())
                self.response = json.loads(response.read().decode("utf-8"))
            except HTTPError as http_error:
                self._logger.error("URL: %s", request.get_full_url())
                self._logger.error("headers: %s", request.headers)
                raise http_error
        else:
            try:
                request.add_header(key="Content-Type",
                                   val="application/json; charset=utf-8")
                data = json.dumps(data).encode("utf-8")
                request.add_header("Content-Length", len(data))
                self.response = urllib.request.urlopen(request, data=data)
                self._logger.info("URL: %s", request.get_full_url())
            except HTTPError as http_error:
                self._logger.error("URL: %s", request.get_full_url())
                self._logger.error("headers: %s", request.headers)
                self._logger.error("data: %s", data)
                raise http_error

    def get_account_info(self, fields=None):
        """Get account information.

        Arguments:
            fields (list) optional: List of fields requested.
            (e.g. ["positions", "orders"])
        """
        fields = ",".join(fields)
        url = "accounts/{}?fields={}".format(self.account_number, fields)
        self._send_request(url)
        return self.response

    def get_orders(self, max_results=None, from_date=None, to_date=None,
                   order_status=None):
        """Get orders.

        Arguments:
            max_resutls (int): Maximum number of orders to return
            from_date (obj datetime.date): Oldest date to retreive orders
            to_date (obj datetime.date): Most current date to retreive orders
            order_status (str): Most likely FILLED | WORKING
        """
        if to_date is None:
            to_date = datetime.today()
        if from_date is None:
            from_date = (datetime.today() - timedelta(35))

        from_date = from_date.strftime("%Y-%m-%d")
        to_date = to_date.strftime("%Y-%m-%d")

        if order_status is None:
            url = "orders?accountId={}&maxResults={}&fromEnteredTime={}"\
                     + """&toEnteredTime={}"""
            url = url.format(self.account_number, max_results,
                             from_date, to_date)
        else:
            url = "orders?accountId={}&maxResults={}&fromEnteredTime={}"\
                     + """&toEnteredTime={}&status={}"""
            url = url.format(self.account_number, max_results,
                             from_date, to_date, order_status)

        self._send_request(url)

        return self.response

    def get_transactions(self, trans_type="TRADE", from_date=None,
                         to_date=None, symbol=None):
        """Get transactions.

        Arguments:
            trans_type (str): Type of transaction
                (All, TRADE, BUY_ONLY, SELL_ONLY, ...)
            https://developer.tdameritrade.com/transaction-history/apis/get/accounts/%7BaccountId%7D/transactions-0
            from_date (obj datetime.date): Oldest date to retreive orders
            to_date (obj datetime.date): Most current date to retreive orders
            symbol (str): Equity ticker symbol. If symbol is None, then return
                all trades specified by the other parameters.
        """
        # Default the time range from 35 days ago to current.
        if to_date is None:
            to_date = datetime.today()
        if from_date is None:
            from_date = (datetime.today() - timedelta(35))

        from_date = from_date.strftime("%Y-%m-%d")
        to_date = to_date.strftime("%Y-%m-%d")

        if symbol is None:
            url = "accounts/{}/transactions?type={}&startDate={}&endDate={}"
            url = url.format(self.account_number, trans_type,
                             from_date, to_date)
        else:
            url = "accounts/{}/transactions?type={}&symbol={}"\
                + "&startDate={}&endDate={}"
            url = url.format(self.account_number, trans_type, symbol,
                             from_date, to_date)
        self._send_request(url)

        return self.response

    def get_watchlists(self):
        """Get all watchlists in account.
        """
        url = "accounts/{}/watchlists".format(self.account_number)
        self._send_request(url)
        return self.response

    def get_watchlist(self, id_num=None, name=None):
        """Get watchlist. Either the watchlist identification number
        or the watchlist name needs specified. Defaults to identification
        number.

        Arguments:
            id_num (str): Identification number of the watchlist.
            name (str): Name of the watchlist.
        """
        if id_num is not None:
            url = "accounts/{}/watchlists/{}"\
                    .format(self.account_number, id_num)
        elif name is not None:
            watchlists = self.get_watchlists()
            for watchlist in watchlists:
                if watchlist["name"] == name:
                    id_num = watchlist["watchlistId"]
                    break
            url = "accounts/{}/watchlists/{}"\
                .format(self.account_number, id_num)
        else:
            raise ValueError
        self._send_request(url)
        return self.response

    def create_saved_order(self, symbol=None, price=None, quantity=0,
                           instruction=None):
        """Create a saved order.

        Arguments:
        symbol (str): Equity ticker symbol.
        price (float): Order set price.
        quantity (float): Order quantity.
        instruction (str): "BUY" | "SELL"
        """
        url = "accounts/{}/savedorders".format(self.account_number)
        data = {
            "orderType": "LIMIT",
            "session": "NORMAL",
            "price": str(round(price, 2)),
            "duration": "DAY",
            "orderStrategyType": "SINGLE",
            "orderLegCollection": [
                {
                    "instruction": instruction,
                    "quantity": str(int(quantity)),
                    "instrument": {
                        "symbol": symbol,
                        "assetType": "EQUITY"
                    }
                }
            ]
        }
        self._send_request(url, data=data)

    def create_watchlist(self, name=None, symbols=None):
        """Create a watchlist.

        Arguments:
        name (str): Watchlist name.
        symbols (list): List of ticker symbols (strings) to add to
            watchlist.
        """
        url = "accounts/{}/watchlists".format(self.account_number)
        watchlist_items = [
            {"instrument": {"symbol": sym, "assetType": "EQUITY"}}
            for sym in symbols
        ]
        data = {
            "name": name,
            "watchlistItems": watchlist_items
        }
        self._send_request(url, data=data)

    def place_order(self, symbol=None, price=None, quantity=0,
                    instruction=None):
        """Place a order.

        Arguments:
        symbol (str): Equity ticker symbol.
        price (float): Order set price.
        quantity (float): Order quantity.
        instruction (str): "BUY" | "SELL"
        """
        url = "accounts/{}/orders".format(self.account_number)
        data = {
            "orderType": "LIMIT",
            "session": "NORMAL",
            "price": str(price),
            "duration": "DAY",
            "orderStrategyType": "SINGLE",
            "orderLegCollection": [
                {
                    "instruction": instruction,
                    "quantity": str(quantity),
                    "instrument": {
                        "symbol": symbol,
                        "assetType": "EQUITY"
                    }
                }
            ]
        }
        self._send_request(url, data=data)

    def get_price_history(self, symbols=None, frequency_type="daily",
                          frequency=1, start_date=None, end_date=None,
                          extended_hours=False):
        """Get the price history.
        symbols (str|list): Equity ticker symbol or list of ticker symbols.
        start_date (datetime): First date for data retrieval.
        end_date (datetime): Last data for data retrieval.
        frequency_type (str): Type of frequency
            (minute, daily, weekly, monthly)
        frequency (float): Frequency of price data.
            Valid frequencies by frequencyType (defaults with an asterisk):
                minute: 1*, 5, 10, 15, 30
                daily: 1*
                weekly: 1*
                monthly: 1*
        extended_hours (bool): True to return extended hours data
        """
        start_date = int(start_date.strftime("%s"))*1000
        end_date = int(end_date.strftime("%s"))*1000

        # If user passed a string, make it an itterable (list).
        if type(symbols) is str:
            symbols = [symbols]

        url = "frequencyType={}&frequency={}"\
            + "&endDate={}&startDate={}"\
            + "&needExtendedHoursData={}"
        url = url.format(frequency_type, frequency,
                         end_date, start_date,
                         extended_hours)

        # If frequencyType is "minute" then periodType is not needed
        # in the url. However, "periodType" is needed for frequencyType
        # of "daily", "weekly" or "monthly"
        if frequency_type != "minute":
            url = "periodType=year&"+url

        data = dict()
        for symbol in symbols:
            url = "marketdata/{}/pricehistory?".format(symbol) + url
            self._send_request(url)
            # If the frequency_type is minute, then include the
            # time in the date field, but if the frequency_type
            # is not minute, then just use the date (no time).
            if frequency_type == "minute":
                temp_data = np.array([[datetime.fromtimestamp(
                                t["datetime"]/1000.),
                                t["open"], t["high"], t["low"],
                                t["close"], t["volume"]]
                             for t in self.response["candles"]])
            else:
                temp_data = np.array([[datetime.fromtimestamp(
                                t["datetime"]/1000.).date(),
                                t["open"], t["high"], t["low"],
                                t["close"], t["volume"]]
                             for t in self.response["candles"]])

            temp_data = pd.DataFrame(index=temp_data[:, 0],
                                     columns=["open_price", "high", "low",
                                     "close_price", "volume"],
                                     data=temp_data[:, 1:])

            data.update({symbol: temp_data})

        # Create a MultiIndex DataFrame.
        columns = pd.MultiIndex.from_product([sorted(list(data.keys())),
            ["close_price", "high", "low", "open_price", "volume"]],
            names=["symbol", "price"])

        bars = pd.DataFrame(index=temp_data.index, columns=columns)

        for d in data:
            bars[d] = data[d]

        # Try to get rid of any missing data.
        bars.fillna(method="ffill", inplace=True)

        return bars

    def get_quotes(self, symbols=None):
        """Get price quotes

        Arguments:
            symbols (str|list): String or list of strings of the ticker
            symbols.

        Returns Pandas DataFrame of quote.
        """
        if type(symbols) is str:
            symbols = [symbols]

        symbol_url = "%2C".join(symbols)
        url = "marketdata/quotes?symbol={}".format(symbol_url)
        self._send_request(url)

        quotes = pd.DataFrame(self.response).T
        return quotes

    def get_fundamental(self, symbols=None):
        """Get the fundamental data.

        Args
        symbols (list|str): String or list of strings of ticker symbols.

        returns Pandas DataFrame of fundamental data.
        """
        if type(symbols) is str:
            symbols = [symbols]

        symbol_url = "%2C".join(symbols)
        url = "instruments?symbol={}&projection=fundamental".format(symbol_url)
        self._send_request(url)
        data = dict()
        for ticker in self.response.keys():
            data[ticker] = self.response[ticker]['fundamental']

        return pd.DataFrame.from_dict(data).T

    def get_positions(self):
        """
        Get the account positions.

        returns (pandas DataFrame): Account positions.
        """
        positions = self.get_account_info(
            fields=["positions"])["securitiesAccount"]["positions"]
        for pos in positions:
            pos.update({k: v for k, v in pos["instrument"].items()})
            del pos["instrument"]

        return pd.DataFrame.from_records(positions)

    def get_trades(self, from_date=None, to_date=None):
        """Get stock trades.

        Arguments:
            from_date (obj datetime.date): Oldest date to retreive orders
            to_date (obj datetime.date): Most current date to retreive orders

        returns (pandas.DataFrame) Account trades.
        """
        trades = self.get_transactions(trans_type="TRADE", from_date=from_date,
                                       to_date=to_date, symbol=None)

        trades = [{
            "description": t["description"],
            "order_date": datetime.strptime(t["orderDate"].split("T")[0],
                                            "%Y-%m-%d"),
            "amount": t["transactionItem"]["amount"],
            "instruction": t["transactionItem"]["instruction"],
            "symbol": t["transactionItem"]["instrument"]["symbol"],
            "cusip": t["transactionItem"]["instrument"]["cusip"],
            "price": t["transactionItem"]["price"]}
                  for t in trades if t["type"] == "TRADE"]

        return pd.DataFrame.from_records(trades)

    def get_simple_open_orders(self):
        """Get simple (non-conditional, single leg) orders that are currently
        open (QUEUED).

        return (Pandas DataFrame): Orders.
        """
        today = datetime.today()
        today = datetime(today.year, today.month, today.day)

        yesterday = today - timedelta(1)
        yesterday = datetime(yesterday.year, yesterday.month, yesterday.day)

        orders = self.get_orders(max_results=1000, from_date=yesterday,
                                 to_date=today, order_status="QUEUED")

        orders = [{
            "order_id": order["orderId"],
            "duration": order["duration"],
            "quantity": order["quantity"],
            "price": order["price"],
            "instruction": order["orderLegCollection"][0]["instruction"],
            "symbol": order["orderLegCollection"][0]["instrument"]["symbol"],
            "cusip": order["orderLegCollection"][0]["instrument"]["cusip"]
            } for order in orders]

        return pd.DataFrame.from_records(orders)
