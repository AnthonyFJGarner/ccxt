import ccxt  # noqa: E402

exchange = ccxt.binance({
    'apiKey': 'HgaJMSm9axBH7Q1biY8Mnc5E5s4W5s0pueNWVKG22xQotyMwiNmvsbJGBbOKWUtY',
    'secret': 'z08FZt6Gmuj7jjU9DfYmNXWHv00rA2iSWazDuC5xeWaTL2bf69kKTwpM9heeoAcM',
})

exchange.load_markets();
exchange.verbose = True
#day in milliseconds
#DAY = 24*60*60*1000
since = exchange.parse8601('2024-08-01T00:00:00Z')
#until = since + (DAY*4)
until = exchange.parse8601('2024-11-12T00:00:00Z')
#check_until=exchange.iso8601 (until)

ledger  = exchange.fetch_ledger(since = since,params = {"until":until,"paginate": True, "paginationDirection": "backward",'portfolioMargin': True, "type": 'linear',"subType": 'linear'})
#deposits = exchange.fetch_deposits(since = since,params = {"until":until,"paginate": True, "paginationDirection": "forward"})
print(ledger)