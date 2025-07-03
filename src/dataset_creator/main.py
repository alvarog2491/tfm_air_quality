#!/usr/bin/env python3
"""
Main orchestrator for data processing.
Automatically runs all data processing pipelines.
"""

import sys
import logging
from pathlib import Path
from datetime import datetime

from config.logger import setup_logger

setup_logger()

def setup_project_structure():
    """
    Check if required project folders exist and create output directory.
    
    Raises:
        FileNotFoundError: If required data directories are missing.
    """
    root_path = Path(__file__).resolve().parent
    data_path = root_path / "data"

    required_dirs = [
        data_path / "air_quality_data",
        data_path / "health_data",
        data_path / "socioeconomic_data",
    ]
    
    # Check for missing required directories
    missing_dirs = [str(p) for p in required_dirs if not p.is_dir()]
    
    if missing_dirs:
        raise FileNotFoundError(
            "The following required directories are missing:\n" +
            "\n".join(missing_dirs)
        )
    
    # Create 'data/output' if it doesn't exist
    output_dir = data_path / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    logging.info("Project structure verified")


def run_data_processing():
    """
    Run all data processing pipelines and merge results.
    
    Returns:
        pandas.DataFrame: Final merged dataset.
        
    Raises:
        Exception: If any processing step fails.
    """
    try:
        # Step 1: Process air quality data
        logging.info("============ Starting air quality data processing ============")
        from processors.air_quality_processor import AirQualityProcessor
        aq_processor = AirQualityProcessor()
        aq_processor.process()
        logging.info(f"Air quality data processed succesfully")
        
        # Step 2: Process health data
        logging.info("============ Starting health data processing ============")
        from processors.health_processor import HealthProcessor
        health_processor = HealthProcessor()
        health_processor.process()
        logging.info(f"Health data processed succesfully")
        
        # Step 3: Process socioeconomic data
        logging.info("============ Starting socioeconomic data processing ============")
        from processors.socioeconomic_processor import SocioeconomicProcessor
        socio_processor = SocioeconomicProcessor()
        socio_processor.process()
        logging.info(f"Socioeconomic data processed succesfully")
        
        # Step 4: Merge all datasets
        logging.info("============ Starting dataset merging ============")
        from processors.data_merger import DataMerger
        merger = DataMerger()
        merged_df = merger.merge_all_data(aq_processor.air_quality_df,
                                         health_processor.health_df,
                                         socio_processor.socioeconomic_df)
        logging.info(f"Dataset merged succesfully: {len(merged_df)} records, {len(merged_df.columns)} columns")
        
        # Step 5: Data Cleaner
        logging.info("============ Starting dataset cleaner ============")
        from processors.dataset_cleaner import DatasetCleaner
        cleaner = DatasetCleaner()
        final_df = cleaner.clean_dataset(merged_df)

        # Save final result
        logging.info("============ Save final dataframe ============")
        data_dir = Path(__file__).resolve().parent / "data" 
        output_path = f"{data_dir}/output/dataset_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        final_df.to_csv(output_path, index=False)
        
        # Also save a version without timestamp for general use
        output_path = f"{data_dir}/output/dataset.csv"
        final_df.to_csv(output_path, index=False)
        logging.info("Final dataset saved")
        
        return final_df
        
    except Exception as e:
        logging.error(f"Error during processing: {str(e)}")
        raise

def main():
    """
    Main function that orchestrates the entire data processing workflow.
    
    Raises:
        SystemExit: If processing fails.
    """
    print("🚀 Starting automated data processing...")
    print("=" * 60)
    
    start_time = datetime.now()
    
    try:
        # Set up project structure
        setup_project_structure()
        
        # Run processing
        final_df = run_data_processing()
        
        # Show summary
        end_time = datetime.now()
        processing_time = end_time - start_time
        
        print("\n" + "=" * 60)
        print("✅ Processing completed successfully!")
        print(f"📊 Final dataset: {len(final_df)} rows, {len(final_df.columns)} columns")
        print(f"⏱️  Total time: {processing_time}")
        print(f"📁 File saved as: output/dataset.csv")
        print("=" * 60)
        
        # Show dataset preview
        print("\n📋 Final dataset preview:")
        print(final_df.head())
        print(f"\n📈 Dataset info:")
        print(final_df.info())
        
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        print(f"❌ Error: {str(e)}")
        raise
        sys.exit(1)

if __name__ == "__main__":
    main()