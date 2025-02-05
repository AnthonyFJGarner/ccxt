import ccxt

symbol = 'ETH/USD:ETH' 
exchange_id = 'deribit'
exchange_class = getattr(ccxt, exchange_id)
exchange = exchange_class()
exchange.load_markets()
instrument = exchange.markets[symbol]['id']
since = exchange.parse8601 ('2019-05-01T00:00:00')-1
until = exchange.milliseconds()
limit = 1000
funding_rate_history = exchange.fetch_funding_rate_history(symbol, since, limit, params={'paginate': True,'paginationCalls': 70,'until': until})
print(funding_rate_history)


