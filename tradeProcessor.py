import pyodbc
from tradeRecord import TradeRecord
from dbConnection import dbConnection

class TradeProcessor:
    LOT_SIZE = 100000.0

    def process_trades(self, stream):
        # --- Read lines from stream ---
        lines = [line.strip() for line in stream if line.strip()]

        trades = []
        line_count = 1

        for line in lines:
            fields = line.split(",")

            if not trade_is_valid(fields, line_count):
                line_count += 1
                continue

            trade = extract_trade_information(fields)
            trades.append(trade)
            line_count += 1

        if persist_trade(trades):
            print(f"INFO: {len(trades)} trades processed")

def persist_trade(trades: list) -> bool:
    connection_string = dbConnection.connection_string
    with pyodbc.connect(connection_string) as conn:
        cursor = conn.cursor()
        try:
            for trade in trades:
                cursor.execute(
                            "{CALL dbo.insert_trade (?, ?, ?, ?)}",
                            trade.source_currency,
                            trade.destination_currency,
                            trade.lots,
                            trade.price
                        )
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise

def extract_trade_information(fields: list) -> TradeRecord:
    trade_amount = int(fields[1])
    trade_price = float(fields[2])
    source_currency = fields[0][:3]
    destination_currency = fields[0][3:]

    return TradeRecord(
        source_currency,
        destination_currency,
        trade_amount / TradeProcessor.LOT_SIZE,
        trade_price
    )

def trade_is_valid(fields: list, line_count: int) -> bool:
    if len(fields) != 3:
        print(f"WARN: Line {line_count} malformed. Only {len(fields)} field(s) found.")
        return False

    if len(fields[0]) != 6:
        print(f"WARN: Trade currencies on line {line_count} malformed: '{fields[0]}'")
        return False

    try:
        int(fields[1])
    except ValueError:
        print(f"WARN: Trade amount on line {line_count} not a valid integer: '{fields[1]}'")
        return False

    try:
        float(fields[2])
    except ValueError:
        print(f"WARN: Trade price on line {line_count} not a valid decimal: '{fields[2]}'")
        return False

    return True