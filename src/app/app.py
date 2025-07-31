from flask import Flask, render_template, request, jsonify
import joblib
import numpy as np

app = Flask(__name__)

# Load the trained model (adjust path if needed)
model = joblib.load("/app/models/model.pkl")


@app.route("/")
def home():
    return render_template("index.html")


@app.route("/predict", methods=["POST"])
def predict():
    # Get form data from the HTML page
    f1 = float(request.form.get("feature1"))
    f2 = float(request.form.get("feature2"))
    input_data = np.array([[f1, f2]])

    prediction = model.predict(input_data)
    return f"<h2>Prediction: {prediction[0]}</h2>"


@app.route("/api/predict", methods=["POST"])
def api_predict():
    data = request.get_json()
    features = np.array([data["feature1"], data["feature2"]]).reshape(1, -1)
    prediction = model.predict(features)
    return jsonify({"prediction": prediction[0]})


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=9001, debug=False)
