def print_metrics(metrics: dict):
    print("\nModel Performance Metrics:")
    for metric, value in metrics.items():
        print(f"{metric.upper()}: {value:.4f}")