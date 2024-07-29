import tensorflow as tf
from tensorflow import keras
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from scipy.fft import rfft, rfftfreq
from scipy import signal
import sklearn
import os

class AutoEncoder:
    def __init__(self):
        # AutoEncoder architecture
        input_freq = keras.Input(shape=(641,))
        encoded = keras.layers.Dense(256, activation="relu")(input_freq)
        encoded = keras.layers.Dense(128, activation="relu")(encoded)
        encoded = keras.layers.Dense(64, activation="relu")(encoded)
        encoded = keras.layers.Dense(32, activation="relu")(encoded)
        encoded = keras.layers.Dense(8, activation="relu")(encoded)

        decoded = keras.layers.Dense(8, activation="relu")(encoded)
        decoded = keras.layers.Dense(32, activation="relu")(decoded)
        decoded = keras.layers.Dense(64, activation="relu")(decoded)
        decoded = keras.layers.Dense(128, activation="relu")(decoded)
        decoded = keras.layers.Dense(641, activation="linear")(decoded)

        self.autoencoder = keras.Model(input_freq, decoded)
        self.encoder = keras.Model(input_freq, encoded)
        self.weights = self.autoencoder.get_weights()
        self.autoencoder.compile(optimizer="adam", loss="mae")

    def fit_autoencoder(self, Xt):
        self.autoencoder.set_weights(self.weights)
        self.autoencoder.fit(
            Xt[:, :641], Xt[:, :641], epochs=100, batch_size=64, shuffle=True, verbose=False
        )

    def encode_data(self, Xt):
        return self.encoder.predict(Xt)

    def get_anomaly_labels(self, Xt, Xf):
        self.autoencoder.set_weights(self.weights)
        self.autoencoder.fit(
            Xt[:, :641], Xt[:, :641], epochs=100, batch_size=64, shuffle=True, verbose=False
        )

        Hencoded_f = self.encoder.predict(Xt[:, :641])
        Hdecoded_f = self.autoencoder.predict(Xf[:, :641])
        score = np.abs(Xf[:, :641] - Hdecoded_f).mean(axis=1)
        anomaly_threshold = np.mean(score) + 3 * np.std(score)

        Vencoded_f = self.encoder.predict(Xt[:, 641:])
        Encoding = np.concatenate((Hencoded_f, Vencoded_f), axis=1)

        kmeans = KMeans(n_clusters=3, random_state=0).fit(Encoding)
        kMeanslabels = kmeans.labels_

        Alabels = np.where(score > anomaly_threshold, 1, 0)
        final_labels = self.correct_labels(kMeanslabels, Alabels)

        return final_labels

    def correct_labels(self, Hl, Al):
        labelsb = Al
        labelsb = np.where(labelsb == 1, 3, 2)
        avg = []
        for i in range(3):
            avg.append(np.sum(np.where(Hl == i)) / len(np.where(Hl == i)[0]))
        new = np.copy(Hl)
        for i in range(3):
            idx = np.argmin(avg)
            new[np.where(Hl == idx)] = i
            avg[idx] = float("inf")
        labelsb[: Hl.shape[0]] = new
        return labelsb