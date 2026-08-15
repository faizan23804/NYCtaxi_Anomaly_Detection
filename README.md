# 🚕 NYC Taxi Traffic Anomaly Detection

An end-to-end Machine Learning project for detecting unusual patterns in NYC taxi traffic using **Isolation Forest**. The project follows a modular ML pipeline architecture, stores data in MongoDB, performs feature engineering and preprocessing, trains an unsupervised anomaly detection model, and provides an interactive Streamlit dashboard for visualization and analysis.

---

## 📌 Project Overview

Taxi traffic can vary significantly depending on the time of day, day of the week, and historical traffic patterns.

This project uses **Isolation Forest**, an unsupervised machine learning algorithm, to identify unusual taxi passenger traffic observations.

The system:

- Extracts NYC taxi traffic data from MongoDB
- Cleans and validates the data
- Creates time-series features
- Scales numerical features
- Trains an Isolation Forest model
- Detects anomalous observations
- Saves the trained model and preprocessing artifacts
- Provides an interactive Streamlit dashboard
- Allows users to explore and download detected anomalies

---

## 🎯 Objectives

The main objectives of this project are:

1. Build an end-to-end anomaly detection pipeline.
2. Detect unusual NYC taxi traffic patterns.
3. Engineer meaningful time-series features.
4. Implement an unsupervised machine learning algorithm.
5. Store and manage data using MongoDB.
6. Save trained models and preprocessing objects as artifacts.
7. Build an interactive dashboard for anomaly analysis.
8. Follow a modular and maintainable project structure.

---

# 🔄 Project Workflow

```text
                    NYC Taxi Traffic Data
                             │
                             ▼
                         MongoDB
                             │
                             ▼
                     Data Ingestion
                             │
                             ▼
                    Data Cleaning
                             │
                             ▼
                  Feature Engineering
                             │
              ┌──────────────┴──────────────┐
              │                             │
           Hour                       Day of Week
           Rolling Mean               Rolling Std
              │                             │
              └──────────────┬──────────────┘
                             ▼
                   Data Transformation
                             │
                             ▼
                       Feature Scaling
                             │
                             ▼
                    Isolation Forest
                             │
                             ▼
                   Anomaly Detection
                             │
                ┌────────────┴────────────┐
                │                         │
             Normal                    Anomaly
                │                         │
                └────────────┬────────────┘
                             ▼
                     Model & Artifacts
                             │
                             ▼
                    Streamlit Dashboard
                             │
                             ▼
                  Visualization & Analysis
