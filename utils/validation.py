def validate_data(df):
    if df is None:
        raise ValueError("DataFrame is None")

    if df.count() == 0:
        raise ValueError("DataFrame is empty")

    print("Data validation successful")
    return True
