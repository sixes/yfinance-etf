import datetime as dt

from ..data import YfData
from ..data import utils
from ..const import _QUERY1_URL_
import json as _json
import pandas as pd

class Etf:
    def __init__(self, market:'str'=None,  session=None, proxy=None, timeout=30):
        self._top_etf_url = f"{_QUERY1_URL_}/v1/finance/screener/predefined/saved"
        self.market = market
        self.session = session
        self.proxy = proxy
        self.timeout = timeout

        self._data = YfData(session=self.session)
        self._logger = utils.get_yf_logger()
        
        
        self._status = None
        self._summary = None
        self._top_etfs_meta = None
        self._top_etfs = None

    @property
    def top_etfs(self):
        if self._top_etfs is not None:
            return self._top_etfs
        self._fetch_top_etfs()
        print(self._top_etfs)
        return self._top_etfs
    
    def _fetch_top_etfs(self, start:int=0, count:int=250):
        top_etf_params = {
            "formatted": "false", 
            "useRecordsResponse": "true",
            "withReturns": "true", 
            "lang": "en-US", 
            "region": "US", 
            "count": count, 
            "scrIds": "TOP_ETFS_US", 
            "start": start}
        top_etfs_json = self._fetch_json(self._top_etf_url, top_etf_params)
        if top_etfs_json['finance']['result'] is None:
            print(top_etfs_json['finance']['error'])
            return self._top_etfs
        self._top_etfs_meta = top_etfs_json['finance']['result'][0]['criteriaMeta']
        if self._top_etfs is None:
            self._top_etfs = pd.DataFrame(top_etfs_json['finance']['result'][0]['records'])
        else:
            self._top_etfs = pd.concat([self._top_etfs, pd.DataFrame(top_etfs_json['finance']['result'][0]['records'])])

        offset = self._top_etfs_meta['offset']
        total = top_etfs_json['finance']['result'][0]['total']
        if offset + count < total:
            #print(f"Fetching {offset}+{count} of {total}")
            self._fetch_top_etfs(start=offset + count, count=count)
        return self._top_etfs

    def _fetch_json(self, url, params):
        data = self._data.get(url=url, params=params, proxy=self.proxy, timeout=self.timeout)
        if data is None or "Will be right back" in data.text:
            raise RuntimeError("*** YAHOO! FINANCE IS CURRENTLY DOWN! ***\n"
                               "Our engineers are working quickly to resolve "
                               "the issue. Thank you for your patience.")
        try:
            return data.json()
        except _json.JSONDecodeError:
            self._logger.error(f"{self.market}: Failed to retrieve market data and recieved faulty data.")
            return {}
        
    def _parse_data(self):
        # Fetch both to ensure they are at the same time
        if (self._status is not None) and (self._summary is not None):
            return
        
        self._logger.debug(f"{self.market}: Parsing market data")

        # Summary

        summary_url = f"{_QUERY1_URL_}/v6/finance/quote/marketSummary"
        summary_fields = ["shortName", "regularMarketPrice", "regularMarketChange", "regularMarketChangePercent"]
        summary_params = {
            "fields": ",".join(summary_fields),
            "formatted": False,
            "lang": "en-US",
            "market": self.market
        }

        status_url = f"{_QUERY1_URL_}/v6/finance/markettime"
        status_params = {
            "formatted": True,
            "key": "finance",
            "lang": "en-US",
            "market": self.market
        }

        self._summary = self._fetch_json(summary_url, summary_params)
        self._status = self._fetch_json(status_url, status_params)

        try:
            self._summary = self._summary['marketSummaryResponse']['result']
            self._summary = {x['exchange']:x for x in self._summary}
        except Exception as e:
            self._logger.error(f"{self.market}: Failed to parse market summary")
            self._logger.debug(f"{type(e)}: {e}")


        try:
            # Unpack
            self._status = self._status['finance']['marketTimes'][0]['marketTime'][0]
            self._status['timezone'] = self._status['timezone'][0]
            del self._status['time']  # redundant
            try:
                self._status.update({
                    "open": dt.datetime.fromisoformat(self._status["open"]),
                    "close": dt.datetime.fromisoformat(self._status["close"]),
                    "tz": dt.timezone(dt.timedelta(hours=int(self._status["timezone"]["gmtoffset"]))/1000, self._status["timezone"]["short"])
                })
            except Exception as e:
                self._logger.error(f"{self.market}: Failed to update market status")
                self._logger.debug(f"{type(e)}: {e}")
        except Exception as e:
            self._logger.error(f"{self.market}: Failed to parse market status")
            self._logger.debug(f"{type(e)}: {e}")




    @property
    def status(self):
        self._parse_data()
        return self._status


    @property
    def summary(self):
        self._parse_data()
        return self._summary
