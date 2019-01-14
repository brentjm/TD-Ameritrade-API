# TD Ameritrade API

Python 3 classes to facilitate using the TD Ameritrade web based API. The class
creates http requests and parses the response to be used interactively in a
shell or Jupyter Lab.

## Getting Started

* Follow the guidelines on setting up an App for on the TD Ameritrade Developers.
    * [TD API](https://developer.tdameritrade.com/content/getting-started)
* Clone this repository
    git clone https://github.com/brentjm/TD-Ameritrade-API.git
* Change the directory to the file containing tdameritrade_api.py
* Get an access token
    * [token](https://developer.tdameritrade.com/content/simple-auth-local-apps)
* In a python terminal (e.g. Jupyter Lab), get a reference to the API.
    $td_api = td_ameritrade.TDAmeritradeAPI(account_number=*your_account*, oauth_certificate=*your_token*)

## Example
[Demo Jupyter Lab Notebook](http://htmlpreview.github.com/?https://github.com/brentjm/TD-Ameritrade-API/blob/master/TDAmeritradeDemo.html)

# Author
**Brent Maranzano**

# License
This project is licensed under the MIT License - see the LICENSE.md file for details
Example usage of the TD Ameritrade API
