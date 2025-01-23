import ccxt
import inspect
def classify_exchange_funding_rate_history_support(exchange_id):
    """
    Classifies a single exchange based on its support for fetching funding rate history.

    Categories:
        1. Unified: Has 'fetchFundingRateHistory' in 'has', the method exists, and API definition suggests support.
        2. Implied: No 'fetchFundingRateHistory' in 'has', but API definition suggests support.
        3. Unsupported: No 'fetchFundingRateHistory' in 'has' and API definition does not suggest support.
    """
    try:
        exchange_class = getattr(ccxt, exchange_id)
        exchange = exchange_class()

        description = exchange.describe()
        has_fetch_funding_rate_history = description.get('has', {}).get('fetchFundingRateHistory', False)
        method_exists = hasattr(exchange, 'fetchFundingRateHistory')

        # Check API definition for funding rate history patterns
        api_definition = description.get('api', {})
        funding_rate_history_in_api = False

        def check_api_for_funding_rate(api):
            nonlocal funding_rate_history_in_api
            if isinstance(api, dict):
                for key, value in api.items():
                    if isinstance(value, (dict, list)):
                        check_api_for_funding_rate(value)
                    elif isinstance(key,
                                    str) and 'funding' in key.lower() and 'rate' in key.lower() and 'history' in key.lower():
                        funding_rate_history_in_api = True
                        return
                    elif isinstance(value,
                                    str) and 'funding' in value.lower() and 'rate' in value.lower() and 'history' in value.lower():
                        funding_rate_history_in_api = True
                        return
            elif isinstance(api, list):
                for item in api:
                    if isinstance(item, (dict, list)):
                        check_api_for_funding_rate(item)
                    elif isinstance(item,
                                    str) and 'funding' in item.lower() and 'rate' in item.lower() and 'history' in item.lower():
                        funding_rate_history_in_api = True
                        return

        check_api_for_funding_rate(api_definition)

        if has_fetch_funding_rate_history:
            return exchange_id, "Unified"
        elif not has_fetch_funding_rate_history and funding_rate_history_in_api:
            return exchange_id, "Implied"
        elif not has_fetch_funding_rate_history and not funding_rate_history_in_api:
            return exchange_id, "Unsupported"
        else:
            return exchange_id, "Ambiguous"

    except Exception as e:
        print(f"Error processing {exchange_id}: {e}")
        return exchange_id, "Error"


def get_exchanges_by_funding_rate_history_support():
    """
    Classifies all CCXT exchanges based on their support for fetching funding rate history.
    """
    exchange_ids = ccxt.exchanges
    unified_exchanges = []
    implied_exchanges = []
    unsupported_exchanges = []
    ambiguous_exchanges = []
    error_exchanges = []

    for exchange_id in exchange_ids:
        exchange_id, classification = classify_exchange_funding_rate_history_support(exchange_id)
        if classification == "Unified":
            unified_exchanges.append(exchange_id)
        elif classification == "Implied":
            implied_exchanges.append(exchange_id)
        elif classification == "Unsupported":
            unsupported_exchanges.append(exchange_id)
        elif classification == "Ambiguous":
            ambiguous_exchanges.append(exchange_id)
        else:
            error_exchanges.append(exchange_id)

    return {
        "Unified": unified_exchanges,
        "Implied": implied_exchanges,
        "Unsupported": unsupported_exchanges,
        "Ambiguous": ambiguous_exchanges,
        "Error": error_exchanges
    }


if __name__ == "__main__":
    exchange_classification = get_exchanges_by_funding_rate_history_support()
    print("Exchanges with Unified fetchFundingRateHistory support:")
    for exchange in exchange_classification["Unified"]:
        print(exchange)
    print("\nExchanges with Implied fetchFundingRateHistory support:")
    for exchange in exchange_classification["Implied"]:
        print(exchange)
    print("\nExchanges with Unsupported fetchFundingRateHistory:")
    for exchange in exchange_classification["Unsupported"]:
        print(exchange)
    print("\nExchanges with Ambiguous fetchFundingRateHistory support:")
    for exchange in exchange_classification["Ambiguous"]:
        print(exchange)
    print("\nExchanges with Error during processing:")
    for exchange in exchange_classification["Error"]:
        print(exchange)