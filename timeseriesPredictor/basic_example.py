import numpy as np
import pandas as pd
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.statespace.sarimax import SARIMAX
from sklearn.preprocessing import MinMaxScaler
from pmdarima import auto_arima
import matplotlib.pyplot as plt


def load_intervals(path):
    df = pd.read_csv(path, names=["timestamp", "data"], sep="|")

    num_lines = len(df)
    last_timestamp = df["timestamp"].iloc[-1]
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")

    df["minute_timestamp"] = df["timestamp"].dt.floor("min")

    grouped_df = df.groupby("minute_timestamp").size().reset_index(name="message_count")

    grouped_df["minute_timestamp"] = grouped_df["minute_timestamp"].dt.strftime("%H:%M")
    grouped_df.index = grouped_df["minute_timestamp"]
    grouped_df = grouped_df.drop("minute_timestamp", axis=1)

    return grouped_df


def make_model_arima(grouped_df):
    model = auto_arima(grouped_df)
    return model


def make_model_sarimax(grouped_df):
    order = (4, 1, 0)
    seasonal_order = (1, 1, 0, 24)
    model = SARIMAX(endog=grouped_df, order=order, seasonal_order=seasonal_order)
    model = model.fit()
    return model


def make_predictions(model, forecast_periods=5):
    return model.predict(forecast_periods)


def plot_original_data(data):
    data.plot(y="message_count", subplots=True, figsize=(15, 8), fontsize=12)
    plt.xlabel("timestamp", fontsize=12)
    plt.ylabel("message_count", fontsize=12)
    plt.show()


def plot_predicted_data(data):
    data.plot(y="Predicted message_count", subplots=True, figsize=(15, 8), fontsize=12)
    plt.xlabel("timestamp", fontsize=12)
    plt.ylabel("Predicted message_count", fontsize=12)
    plt.show()


def plot_data_and_prediction(original_data, prediction):
    plt.figure(figsize=(15, 8))
    prediction.index = prediction.index.strftime("%H:%M")
    plt.plot(
        original_data.index,
        original_data["message_count"],
        label="Original Data",
        color="blue",
    )

    plt.plot(
        prediction.index,
        prediction["Predicted message_count"],
        label="Predicted Data",
        color="red",
        linestyle="--",
    )

    plt.xlabel("timestamp", fontsize=12)
    plt.ylabel("message_count", fontsize=12)
    plt.title("Original Data vs Predictions", fontsize=14)
    plt.legend()

    plt.show()


def main():
    """
    Main function to load data, forecast intervals using ARIMA, and plot the results.
    """
    path_debug = (
        "/home/jona/Documents/Bachelor_thesis/Datasets/pred/fit/test/debug_fit.csv"
    )
    path_fit = "/home/jona/Documents/Bachelor_thesis/Datasets/pred/fit/test/riot_events_FIT_sc_1.csv"
    path_sys = "/home/jona/Documents/Bachelor_thesis/Datasets/pred/sys/test/riot_events_SYS_sc_1.csv"
    path_taxi = "/home/jona/Documents/Bachelor_thesis/Datasets/pred/taxi/complete/riot_events_TAXI_riotbench_period_scaled_5h.csv"
    original_data = load_intervals(path_taxi)

    length_org_data = original_data.size
    length_train_dataset = 60 * 3
    intervals = original_data.head(length_train_dataset)
    steps = length_org_data - length_train_dataset
    scaler = MinMaxScaler()
    intervals["message_count"] = scaler.fit_transform(intervals[["message_count"]])
    auto_arima(intervals)
    model = make_model_sarimax(intervals)
    res = make_predictions(model, steps)
    forecast_original = scaler.inverse_transform(res.values.reshape(-1, 1))

    forecast_df = pd.DataFrame(
        forecast_original, index=res.index, columns=["Predicted message_count"]
    )
    plot_data_and_prediction(original_data, forecast_df)


if __name__ == "__main__":
    main()
