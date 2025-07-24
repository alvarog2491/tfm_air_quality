#!/usr/bin/env python3
"""
ETL Pipeline Orchestrator

Coordinates the full ETL process by running each phase sequentially:
Extraction, Transformation, and Loading.
"""

import logging
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from config.logger import setup_logger
from extract import DataExtractionStep
from load import DataExportStep, DataQualityReportStep
from transform import (
    DataCleaningStep,
    DataMergingStep,
    DataTransformationStep,
    DataValidationStep,
    FeatureEngineeringStep,
)
from utils import CheckProjectStructure

from etl_pipeline import ETLStep

setup_logger()


class ETLPipeline:
    """
    Main class responsible for running a full ETL pipeline.¸

    Attributes:
        steps (List[ETLStep]): List of ETL steps to execute in order.
    """

    def __init__(self, steps: Optional[List[ETLStep]] = None):
        """
        Initializes the ETLPipeline.

        Args:
            steps (Optional[List[ETLStep]]): Optional list of ETL steps.
                If None, defaults are loaded.
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.steps = steps or self._get_default_steps()

    def _get_default_steps(self) -> List[ETLStep]:
        """
        Returns the default list of ETL steps.

        Returns:
            List[ETLStep]: Default ETL steps in execution order.
        """
        return [
            DataExtractionStep(),
            DataTransformationStep(),
            DataMergingStep(),
            FeatureEngineeringStep(),
            DataCleaningStep(),
            DataValidationStep(),
            DataExportStep(),
            DataQualityReportStep(),
        ]

    def run(self) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Executes the ETL pipeline by running all configured steps in order.

        Returns:
            Tuple[pd.DataFrame, Dict[str, Any]]:
                - The final processed DataFrame.
                - Dictionary with metadata such as execution time, output file,
                  final shape, and number of executed steps..
        """
        start_time = datetime.now()
        self.logger.info("Starting ETL Pipeline execution...")

        try:
            # Initialize Data
            data_path = CheckProjectStructure().execute()
            dataframes: Dict[str, pd.DataFrame] = {}
            context: Dict[str, Any] = {"data_path": data_path, "export_format": ["csv"]}

            # Run all pipeline steps
            for step in self.steps:
                step.execute(dataframes, context)

            # Show results
            processing_time = datetime.now() - start_time
            output_file_path: str = context["output_file_path"]
            output_file: pd.DataFrame = context["output_file"]
            reports_path: pd.DataFrame = context["reports_path"]

            results: Dict[str, Any] = {
                "execution_time": processing_time,
                "output_file_path": output_file_path,
                "reports_path": reports_path,
                "final_shape": output_file.shape,
                "steps_executed": [
                    f"{i} - {step.__class__.__name__}\n"
                    for i, step in enumerate(self.steps)
                ],
            }
            return output_file, results

        except Exception as e:
            self.logger.error(f"ETL Pipeline failed: {str(e)}")
            raise


def main():
    """
    Main execution entry point for the default ETL pipeline.

    Runs the pipeline and prints a summary of the results.
    """
    print("Starting automated data processing...")
    print("=" * 60)

    try:
        pipeline = ETLPipeline()
        final_df, results = pipeline.run()

        print("\n" + "=" * 60)
        print("✅ Processing completed successfully!")
        print(
            f"Final dataset: {results['final_shape'][0]} rows, {results['final_shape'][1]} columns"
        )
        print(f"Total time: {results['execution_time']}")
        print(f"File saved as: {results['output_file_path']}")
        print(f"Reports saved at: {results['reports_path']}")
        print("Steps executed:\n" + "".join(results["steps_executed"]))
        print("=" * 60)

        if final_df.empty is not False:
            print("\nFinal dataset preview:")
            print(final_df.head())
            print("\nDataset info:")
            print(final_df.info())

        return final_df, results

    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        print(f"❌ Error: {str(e)}")
        # raise e
        sys.exit(1)


if __name__ == "__main__":
    main()
