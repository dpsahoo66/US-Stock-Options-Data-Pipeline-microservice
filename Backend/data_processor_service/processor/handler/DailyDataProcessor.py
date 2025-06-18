import pandas as pd

def DailyDataProcessor(data):

    df = pd.DataFrame(data['values'])
    processed_data = df
    return processed_data
