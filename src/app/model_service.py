from flask import Flask, request, jsonify
import joblib
import pandas as pd
import os

app = Flask(__name__)

# Load
model_path = os.path.join(os.path.dirname(__file__), "../../models/model.pkl")
model = joblib.load(model_path)


@app.route("/predict", methods=["POST"])
def predict():
    try:
        data = request.get_json()
        # Convert to DataFrame
        df = pd.DataFrame([data])

        # Realize prediction
        prediction = model.predict(df)

        return jsonify({"prediction": prediction.tolist(), "status": "success"})
    except Exception as e:
        return jsonify({"error": str(e), "status": "error"}), 400


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
