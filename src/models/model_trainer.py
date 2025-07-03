from sklearn.model_selection import train_test_split, cross_val_score
import logging

logger = logging.getLogger(__name__)

def train_model(model, X, y, test_size=0.2, random_state=42):
    logger.info("Splitting data into training and testing sets...")
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=random_state)
    
    logger.info("Training model...")
    model.train(X_train, y_train)
    
    logger.info("Evaluating model...")
    metrics = model.evaluate(X_test, y_test)
    
    logger.info(f"Model evaluation: {metrics}")
    return model, metrics

def cross_validate_model(model, X, y, cv=5):
    logger.info(f"Performing {cv}-fold cross-validation...")
    scores = cross_val_score(model.pipeline, X, y, cv=cv, scoring='r2')
    logger.info(f"Cross-validation scores: {scores}")
    return scores
