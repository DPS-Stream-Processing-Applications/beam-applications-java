import numpy as np


def calculate_utility_value(cpu, latency, throughput, weights):
    w1, w2, w3 = weights
    utility = -w1 * cpu - w2 * latency + w3 * throughput
    return utility


def normalize_dataset(data):
    normalized_data = np.zeros_like(data)
    for i in range(data.shape[1]):
        min_val = np.min(data[:, i])
        max_val = np.max(data[:, i])
        normalized_data[:, i] = (data[:, i] - min_val) / (max_val - min_val)
    return normalized_data


def normalize_weights(weights_list):
    total_sum = sum(weights_list)
    normalized_weights = [w / total_sum for w in weights]
    return normalized_weights


def main(user_weights):
    print(normalize_weights(user_weights))


if __name__ == "__main__":
    weights = [0.5, 0.3, 0.4]
    main(weights)
