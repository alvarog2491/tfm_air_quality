import joblib
import logging

logger = logging.getLogger(__name__)

def save_model(model, filename: str = "linear_regression_model.pkl"):
    logger.info(f"Saving model to {filename}...")
    joblib.dump(model.pipeline, filename)
    logger.info("Model saved successfully.")