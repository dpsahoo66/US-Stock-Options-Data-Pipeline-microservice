from collector.utils.symbols import symbols


def create_batches(symbol_list, batch_size=8):
    for i in range(0, len(symbol_list), batch_size):
        yield symbol_list[i:i + batch_size]

# # Example usage:
# batches = list(create_batches(symbols, 8))
# for idx, batch in enumerate(batches, 1):
#     print(f"Batch {idx}: {batch}")