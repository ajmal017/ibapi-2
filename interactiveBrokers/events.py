

class Event(object):
    pass

class MarketEvent(Event):
    def __init__(self):
        self.type = 'MARKET'

class SignalEvent(Event):
    def __init__(self, symbol, datetime, signal_type):
        self.type = 'SIGNAL'
        self.symbol = symbol
        self.datetime = datetime # datetime object
        self.signal_type = signal_type # 'LONG' or 'SHORT'

class OrderEvent(Event):
    def __init__(self, symbol, order_type, quantity, direction):
        self.type= 'ORDER'
        self.symbol = symbol
        self.order_type = order_type # 'MARKET' or "LIMIT"
        self.quantity = quantity # SHARES
        self.direction = direction # 'BUY' or 'SELL

    def print_order(self):
        print("Order: Symbol=%s, Type=%s, Quantity=%s, Direction=%s" % \
            (self.symbol, self.order_type, self.quantity, self.direction))

class FillEvent(Event):
    def __init__(self, datetime, symbol, exchange, quantity, 
                 direction, fill_cost, commission=None):
        self.type = 'FILL'
        self.datetime = datetime
        self.symbol = symbol
        self.quantity = quantity # SHARES
        self.direction = direction # 'BUY' or 'SELL'
        self.fill_cost = fill_cost # USD

        # Calculate commission
        if commission is None:
            # calculate_ib_commission Tiered
            # USD0.0035 per share minimum USD 0.35 and max 1% of trade value
            self.commission = max([min([(self.quantity * .0035) , .35 ]), (.1 * self.fill_cost)])
        else:
            self.commission = commission