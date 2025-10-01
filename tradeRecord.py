class TradeRecord:
    def __init__(self, source_currency, destination_currency, lots, price):
        self.source_currency = source_currency
        self.destination_currency = destination_currency
        self.lots = lots
        self.price = price