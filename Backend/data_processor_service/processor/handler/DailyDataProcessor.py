import pandas as pd

def DailyDataProcessor(data):

    df = pd.DataFrame(data['values'])
    processed_data = df.to_dict(orient='records')
    return processed_data
