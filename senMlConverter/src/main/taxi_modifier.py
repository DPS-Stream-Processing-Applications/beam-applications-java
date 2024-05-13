import pandas as pd


def filter_date(input_file):
    df = pd.read_csv(input_file)
    start_date = pd.to_datetime("2013-01-14 00:00:00")
    end_date = pd.to_datetime("2013-01-21 23:59:59")
    filtered_df = df[
        (pd.to_datetime(df[" pickup_datetime"]) >= start_date)
        & (pd.to_datetime(df[" pickup_datetime"]) <= end_date)
    ]
    return filtered_df


def rename_cols(df):
    df.rename(columns={"medallion": "taxi_identifier"}, inplace=True)
    return df


def drop_cols_trip(df):
    df1_col_drop = [
        " vendor_id",
        " rate_code",
        " store_and_fwd_flag",
        " passenger_count",
    ]
    df.drop(columns=df1_col_drop, axis=1, inplace=True)
    return df


def drop_cols_fare(df):
    df2_col_drop = [" vendor_id", " hack_license", " pickup_datetime"]
    df.drop(columns=df2_col_drop, inplace=True)
    return df


def merge_dfs(input_trip_df, input_fare_df):
    chunksize = 10000
    merged_chunks = []
    num_chunks = -(-len(input_trip_df) // chunksize)
    for i in range(num_chunks):
        start_idx = i * chunksize
        end_idx = (i + 1) * chunksize
        chunk1 = input_trip_df.iloc[start_idx:end_idx]
        chunk2 = input_fare_df.iloc[start_idx:end_idx]
        merged_chunk = pd.merge(chunk1, chunk2, on="taxi_identifier", how="inner")
        merged_chunks.append(merged_chunk)

    merged_df = pd.concat(merged_chunks, ignore_index=True)
    return merged_df


def change_header_order_df(df, new_order):
    df = df[new_order]
    return df
