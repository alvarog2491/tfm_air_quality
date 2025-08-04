import os
from typing import Dict, Union

import joblib
import pandas as pd
from flask import Flask, render_template, request
from common.utils.file_utils import load_yaml_config
from modeling.utils.dataset_modeling_utils import (
    one_hot_encode_categorical_features,
    scale_numerical_features,
)

app = Flask(__name__)


# Determine the correct path for models based on environment
def get_project_root():
    """Get the project root directory"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Go up from src/app/ to project root
    return os.path.join(current_dir, "..", "..")


def get_model_path(filename: str):
    """Get the correct model path for both local and Docker environments"""
    # Check if running in Docker (has /app directory structure)
    if os.path.exists("/app/models"):
        return f"/app/models/{filename}"
    else:
        # Local development - use relative path from project root
        project_root = get_project_root()
        return os.path.join(project_root, "models", filename)


# Load the trained model and scaler
model_path = get_model_path("model.pkl")
scaler_path = get_model_path("scaler.pkl")

model = joblib.load(model_path)
scaler = joblib.load(scaler_path)

# Load preprocessing configuration
config = load_yaml_config("params.yaml")["preprocess"]
categorical_features = config["categorical_features"]
numerical_features = config["numerical_features"]
drop_colnames = config["drop_colnames"]


@app.route("/")
def home():
    return render_template("index.html")


@app.route("/predict", methods=["POST"])
def predict():
    try:
        # Get all form data from the HTML page
        form_data: Dict[str, Union[str, int, float]] = {
            "Air Pollutant": request.form.get("air_pollutant", ""),
            "Air Pollutant Description": request.form.get(
                "air_pollutant_description", ""
            ),
            "Data Aggregation Process": request.form.get(
                "data_aggregation_process", ""
            ),
            "Year": request.form.get("year", 0),
            "Air Pollution Level": float(request.form.get("air_pollution_level", 0)),
            "Unit Of Air Pollution Level": request.form.get(
                "unit_of_air_pollution_level", ""
            ),
            "Air Quality Station Type": request.form.get(
                "air_quality_station_type", ""
            ),
            "Air Quality Station Area": request.form.get(
                "air_quality_station_area", ""
            ),
            "Longitude": float(request.form.get("longitude", 0)),
            "Latitude": float(request.form.get("latitude", 0)),
            "Altitude": float(request.form.get("altitude", 0)),
            "Province": request.form.get("province", ""),
            "Quality": request.form.get("quality", ""),
            "Life_expectancy_total": float(
                request.form.get("life_expectancy_total", 0)
            ),
            "pib": float(request.form.get("pib", 0)),
            "Population": float(request.form.get("population", 0)),
            "Respiratory_deaths_per_100k": float(
                request.form.get("respiratory_deaths_per_100k", 0)
            ),
        }

        df = pd.DataFrame([form_data])

        # Apply the same preprocessing as during training
        # 1. Drop columns that were dropped during training
        columns_to_drop = [col for col in drop_colnames if col in df.columns]
        df = df.drop(columns=columns_to_drop)

        # 2. One-hot encode categorical features
        df = one_hot_encode_categorical_features(df, categorical_features)

        # 3. Scale numerical features using the saved scaler
        df, _ = scale_numerical_features(df, numerical_features, scaler)

        # Make prediction
        prediction = model.predict(df)

        # Return a formatted result page
        result_html = f"""
        <html>
        <head>
            <title>Prediction Result</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 40px; background-color: #f5f5f5; }}
                .container {{ max-width: 800px; margin: 0 auto; background-color: white; padding: 30px; border-radius: 8px; }}
                .result {{ background-color: #e7f3ff; padding: 20px; border-radius: 8px; margin: 20px 0; }}
                .back-btn {{ background-color: #007bff; color: white; padding: 10px 20px; text-decoration: none; border-radius: 4px; }}
                .input-summary {{ background-color: #f8f9fa; padding: 15px; border-radius: 4px; margin: 20px 0; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>Air Quality Prediction Result</h1>
                <div class="result">
                    <h2>Predicted Respiratory Diseases Total: {prediction[0]:.2f}</h2>
                    <p><em>This represents the predicted total number of respiratory diseases for the given conditions.</em></p>
                </div>
                <div class="input-summary">
                    <h3>Input Data Summary:</h3>
                    <p><strong>Province:</strong> {form_data['province']}</p>
                    <p><strong>Air Pollutant:</strong> {form_data['air_pollutant']} ({form_data['air_pollutant_description']})</p>
                    <p><strong>Pollution Level:</strong> {form_data['air_pollution_level']} {form_data['unit_of_air_pollution_level']}</p>
                    <p><strong>Location:</strong> {form_data['latitude']}, {form_data['longitude']} (Altitude: {form_data['altitude']}m)</p>
                    <p><strong>Station Type:</strong> {form_data['air_quality_station_type']} - {form_data['air_quality_station_area']}</p>
                </div>
                <a href="/" class="back-btn">Make Another Prediction</a>
            </div>
        </body>
        </html>
        """
        return result_html

    except Exception as e:
        return f"<h2>Error: {str(e)}</h2><p><a href='/'>Go back</a></p>"


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=9001, debug=True, use_reloader=False)
