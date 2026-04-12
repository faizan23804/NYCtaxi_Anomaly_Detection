#!/bin/bash
# start.sh — runs pipeline first, then launches app
echo "Running training pipeline..."
python main.py

echo "Starting Streamlit app..."
streamlit run app.py --server.port=8501 --server.address=0.0.0.0