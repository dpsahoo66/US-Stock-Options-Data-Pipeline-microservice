import pandas as pd

def RealTimeDataProcessor(data):

    df = pd.DataFrame(data['values'])
    processed_data = df

    return processed_data
