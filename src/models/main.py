import logging
from linear_regression.linear_regression import LinearRegressionModel
from model_trainer import train_model, cross_validate_model
from model_evaluator import print_metrics
from model_saver import save_model
from utils import load_dataset

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    # Load dataset
    df = load_dataset('../dataset_creator/data/output/dataset.csv')

    # Feature selection
    X = df[['Year', 'Altitude']] 
    y = df['Air Pollution Level']

    # Initialize and train model
    model = LinearRegressionModel()
    trained_model, metrics = train_model(model, X, y)

    # Print performance
    print_metrics(metrics)

    # Cross-validation (optional but recommended)
    cross_validate_model(trained_model, X, y)

    # Save model
    save_model(trained_model)

if __name__ == "__main__":
    main()
