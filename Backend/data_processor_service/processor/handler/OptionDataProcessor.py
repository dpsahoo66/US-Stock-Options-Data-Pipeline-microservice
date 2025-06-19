import pandas as pd

def OptionDataProcessor(data):

    df = pd.DataFrame(data['data'])

    processed_data = df.to_dict(orient='records')

    return processed_data
    