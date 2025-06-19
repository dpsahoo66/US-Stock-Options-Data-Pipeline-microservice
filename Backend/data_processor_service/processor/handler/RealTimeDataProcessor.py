import pandas as pd

def RealTimeDataProcessor(data):

    df = pd.DataFrame(data['values'])

    processed_data = df.to_dict(orient='records')

    return processed_data
