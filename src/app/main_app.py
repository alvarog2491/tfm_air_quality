from flask import Flask, render_template, request, jsonify
import requests
import os

app = Flask(__name__)

# URL del servicio del modelo
MODEL_SERVICE_URL = os.getenv("MODEL_SERVICE_URL", "http://localhost:5000")


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/predict", methods=["POST"])
def predict():
    try:
        # Obtener datos del formulario
        data = request.get_json()

        # Llamar al servicio del modelo
        response = requests.post(f"{MODEL_SERVICE_URL}/predict", json=data)

        if response.status_code == 200:
            return jsonify(response.json())
        else:
            return (
                jsonify(
                    {"error": "Error en el servicio de predicción", "status": "error"}
                ),
                500,
            )

    except Exception as e:
        return jsonify({"error": str(e), "status": "error"}), 500


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
