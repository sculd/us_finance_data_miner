_FILENAME = 'nasdaq.txt'

def get_symbols():
    symbols = []
    for symbol in open(_FILENAME, 'r'):
        symbol = symbol.strip()
        symbols.append(symbol)
    return symbols
