import numpy as np
import pandas as pd
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.statespace.sarimax import SARIMAX
from sklearn.preprocessing import MinMaxScaler
from pmdarima import auto_arima
import matplotlib.pyplot as plt
from sklearn.metrics import mean_absolute_error, mean_squared_error
from statsmodels.tsa.stattools import adfuller
from sklearn.ensemble import RandomForestRegressor
from skforecast.ForecasterAutoreg import ForecasterAutoreg


def load_intervals_input_rate(path):
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


def load_data_metrics(path):
    df = pd.read_csv(path, names=["timestamp", "data"], sep=",", skiprows=1)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["data"] = pd.to_numeric(df["data"], errors="coerce").astype("float64")
    df = df.dropna(subset=["timestamp"])
    df = df.set_index("timestamp")
    df = df.sort_index()
    return df


def make_model_arima(grouped_df):
    data_column = grouped_df["data"]
    model = auto_arima(
        data_column, 
        seasonal=False
    )
    return model


def make_model_sarimax(grouped_df):
    grouped_df = grouped_df.asfreq("30s")
    order = (4, 1, 0)
    seasonal_order = (1, 1, 0, 24)
    model = SARIMAX(
        endog=grouped_df, order=order, seasonal_order=seasonal_order, freq="30s"
    )
    model = model.fit()
    return model

def make_model_predictor(data):
    data = data.asfreq("30s")
    forecaster = ForecasterAutoreg(
                regressor = RandomForestRegressor(random_state=123),
                lags = 100
             )
    forecaster.fit(data["data"])
    return forecaster

def make_predictions_predictor(model, forecast_periods=5):
    predictions = model.predict(steps=forecast_periods)
    return predictions

def make_predictions_sarimax(model, forecast_periods=5):
    # return model.predict(forecast_periods,return_conf_int=False)

    start = len(model.data.endog)
    end = start + forecast_periods - 1
    return model.predict(start=start, end=end)


def make_predictions_arima(model, forecast_periods=5):
    return model.predict(forecast_periods)


def plot_original_data(data):
    data.plot(y="data", subplots=True, figsize=(15, 8), fontsize=12)
    plt.xlabel("timestamp", fontsize=12)
    plt.ylabel("data", fontsize=12)
    plt.show()


def plot_predicted_data(data):
    data.plot(y="Predicted data", subplots=True, figsize=(15, 8), fontsize=12)
    plt.xlabel("timestamp", fontsize=12)
    plt.ylabel("Predicted data", fontsize=12)
    plt.show()


def plot_data_and_prediction(original_data, prediction):
    plt.figure(figsize=(15, 8))
    plt.plot(
        original_data.index,
        original_data["data"],
        label="Original Data",
        color="blue",
    )

    plt.plot(
        prediction.index,
        prediction["Predicted data"],
        label="Predicted Data",
        color="red",
        linestyle="--",
    )

    plt.xlabel("timestamp", fontsize=12)
    plt.ylabel("data", fontsize=12)
    plt.title("Original Data vs Predictions", fontsize=14)
    plt.legend()

    plt.show()

def compute_errors(original_data, predicted_data):
    # Ensure the indexes align before calculating errors
    aligned_actual, aligned_pred = original_data.align(predicted_data, join='inner')
    joined_df = original_data.join(predicted_data, how="inner")
    if joined_df.empty:
        print("Warning: The joined DataFrames are empty. Please check the indexes.")
        return None
    
    mae = mean_absolute_error(joined_df["data"], joined_df["Predicted data"])
    mse = mean_squared_error(joined_df["data"], joined_df["Predicted data"])
    rmse = np.sqrt(mse)
    
    epsilon = 1e-10
    mape = np.mean(np.abs((joined_df["data"] - joined_df["Predicted data"]) / (joined_df["data"] + epsilon))) * 100

    return {"MAE": mae, "MSE": mse, "RMSE": rmse, "MAPE": mape}

def test_stationarity(time_series):
    result = adfuller(time_series, autolag='AIC')
    
    adf_statistic = result[0]
    p_value = result[1]
    used_lag = result[2]
    n_obs = result[3]
    critical_values = result[4]

    print("Results of Augmented Dickey-Fuller Test:")
    print(f"ADF Statistic: {adf_statistic}")
    print(f"p-value: {p_value}")
    print(f"#Lags Used: {used_lag}")
    print(f"Number of Observations Used: {n_obs}")
    
    print("Critical Values:")
    for key, value in critical_values.items():
        print(f"{key}: {value}")
    
    if p_value < 0.05:
        print("\nConclusion: The data is stationary (reject the null hypothesis).")
    else:
        print("\nConclusion: The data is non-stationary (fail to reject the null hypothesis).")


def main():
    path_cpu_load = "/home/jona/Documents/Bachelor_thesis/Documentation/Measurements/240916/p_t_c_5/flink_taskmanager_Status_JVM_CPU_Load.csv"
    path_cpu_time = "/home/jona/Documents/Bachelor_thesis/Documentation/Measurements/240916/p_f_c_40/flink_taskmanager_Status_JVM_CPU_Time.csv"

    original_data = load_data_metrics(path_cpu_load)
    length_org_data = original_data.size
    length_train_dataset = int(length_org_data * 0.8)
    intervals = original_data.head(length_train_dataset)
    steps = length_org_data - length_train_dataset
    
    scaler = MinMaxScaler()
    scaled_data = pd.DataFrame(
        scaler.fit_transform(intervals[["data"]]),
        columns=["data"],
        index=intervals.index,
    )
    intervals.loc[:, "data"] = scaled_data
    #model = make_model_sarimax(intervals)
    #res = make_predictions_sarimax(model, steps)

    #model = make_model_arima(intervals)
    #res = make_predictions_arima(model, steps)
    model = make_model_predictor(intervals)
    res = make_predictions_predictor(model, steps)
    forecast_original = scaler.inverse_transform(res.values.reshape(-1, 1))
    forecast_df = pd.DataFrame(
        forecast_original, index=res.index, columns=["Predicted data"]
    )
    plot_data_and_prediction(original_data, forecast_df)
    errors = compute_errors(original_data, forecast_df)
    print("Error Metrics:")
    print(f"MAE: {errors['MAE']}")
    print(f"MSE: {errors['MSE']}")
    print(f"RMSE: {errors['RMSE']}")
    print(f"MAPE: {errors['MAPE']}%")



if __name__ == "__main__":
    main()
