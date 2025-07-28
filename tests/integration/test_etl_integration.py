"""
Integration tests for ETL pipeline.
Tests the complete ETL pipeline flow using actual repository structure.
"""
import pytest
import pandas as pd
from pathlib import Path

from etl_pipeline.main_orchestrator import ETLPipeline


class TestETLIntegration:
    """Integration tests for the complete ETL pipeline using actual repository code."""

    @pytest.fixture
    def check_raw_data(self):
        """Check if raw data directories exist."""
        expected_data_dirs = [
            "src/etl_pipeline/data/air_quality_data/raw",
            "src/etl_pipeline/data/health_data/raw", 
            "src/etl_pipeline/data/socioeconomic_data/raw"
        ]
        
        for data_dir in expected_data_dirs:
            if not Path(data_dir).exists() or not any(Path(data_dir).glob("*.csv")):
                pytest.skip(f"Required data directory not found or empty: {data_dir}")
        yield

    def test_etl_pipeline_execution(self, check_raw_data):
        """Test ETL pipeline execution using actual orchestrator."""
        
        # Use actual ETL pipeline
        pipeline = ETLPipeline()
        
        # Execute pipeline
        context = pipeline.run()
        
        # Verify the pipeline executed successfully
        assert context is not None, "ETL pipeline returned no context"
        
        # Check that output was created
        output_file = Path("src/etl_pipeline/data/output/dataset.csv")
        assert output_file.exists(), "ETL pipeline did not create dataset.csv"
        
        # Validate output content
        df = pd.read_csv(output_file)
        assert not df.empty, "ETL output dataset is empty"
        assert len(df.columns) >= 5, "ETL output has too few columns"
        
        print(f"ETL pipeline successful! Output shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")

    def test_etl_data_quality_validation(self, check_raw_data):
        """Test ETL pipeline data quality validation."""
        
        # Run ETL pipeline
        pipeline = ETLPipeline()
        context = pipeline.run()
        
        # Check output file
        output_file = Path("src/etl_pipeline/data/output/dataset.csv")
        assert output_file.exists(), "ETL pipeline did not create dataset.csv"
        
        # Load and validate data
        df = pd.read_csv(output_file)
        
        # Basic data quality checks
        assert not df.empty, "Dataset is empty"
        assert not df.isna().all().all(), "Dataset contains only null values"
        
        # Check for expected columns based on the pipeline
        # These should exist based on the air quality analysis project
        expected_columns = [
            'Air Pollution Level',
            'Respiratory_diseases_total',
            'Life_expectancy_total', 
            'pib'
        ]
        
        missing_columns = [col for col in expected_columns if col not in df.columns]
        if missing_columns:
            print(f"Warning: Expected columns not found: {missing_columns}")
            print(f"Available columns: {list(df.columns)}")
        
        # Verify numeric columns contain valid data
        numeric_columns = df.select_dtypes(include=['number']).columns
        assert len(numeric_columns) > 0, "No numeric columns found"
        
        for col in numeric_columns:
            if col in df.columns:
                # Check for non-null numeric values
                non_null_count = df[col].notna().sum()
                assert non_null_count > 0, f"Column {col} contains no valid numeric data"
        
        print(f"Data quality validation passed! Shape: {df.shape}")
        print(f"Numeric columns: {list(numeric_columns)}")

    def test_etl_output_consistency(self, check_raw_data):
        """Test that ETL pipeline produces consistent output across runs."""
        
        output_file = Path("src/etl_pipeline/data/output/dataset.csv")
        
        # Run pipeline first time
        pipeline1 = ETLPipeline()
        context1 = pipeline1.run()
        
        # Check output exists
        assert output_file.exists(), "First run did not create output"
        df1 = pd.read_csv(output_file)
        
        # Run pipeline second time
        pipeline2 = ETLPipeline()
        context2 = pipeline2.run()
        
        # Check output still exists and is consistent
        assert output_file.exists(), "Second run did not create output"
        df2 = pd.read_csv(output_file)
        
        # Compare datasets
        assert df1.shape == df2.shape, "Dataset shapes differ between runs"
        assert list(df1.columns) == list(df2.columns), "Column names differ between runs"
        
        # Check if data is identical (it should be for deterministic processing)
        # Allow for small floating point differences
        for col in df1.columns:
            if df1[col].dtype in ['float64', 'int64']:
                if not df1[col].equals(df2[col]):
                    # Check if differences are negligible
                    diff = (df1[col] - df2[col]).abs().max()
                    assert diff < 1e-10 or pd.isna(diff), f"Significant differences in column {col}"
            else:
                assert df1[col].equals(df2[col]), f"Differences in non-numeric column {col}"
        
        print("ETL pipeline consistency test passed!")

    def test_etl_error_handling(self):
        """Test ETL pipeline error handling with missing directories."""
        
        # This test verifies that the pipeline properly handles missing data directories
        # by checking the project structure validation
        
        pipeline = ETLPipeline()
        
        # The pipeline should either run successfully (if data exists) 
        # or fail gracefully with appropriate error messages
        try:
            context = pipeline.run()
            # If successful, verify output was created
            output_file = Path("src/etl_pipeline/data/output/dataset.csv")
            assert output_file.exists(), "Pipeline succeeded but no output created"
            print("ETL pipeline executed successfully")
            
        except Exception as e:
            # If failed, verify it's due to missing data directories
            error_msg = str(e)
            assert "required directories are missing" in error_msg.lower() or \
                   "no such file" in error_msg.lower() or \
                   "not found" in error_msg.lower(), \
                   f"Unexpected error type: {error_msg}"
            print(f"ETL pipeline correctly failed with expected error: {e}")

    def test_etl_pipeline_steps(self, check_raw_data):
        """Test individual ETL pipeline steps."""
        
        pipeline = ETLPipeline()
        
        # Get default steps
        steps = pipeline._get_default_steps()
        
        # Verify expected steps exist
        step_names = [step.name for step in steps]
        expected_steps = [
            "CheckProjectStructure",
            "DataExtractionStep", 
            "DataMergingStep",
            "DataCleaningStep",
            "DataExportStep",
            "DataQualityReportStep"
        ]
        
        for expected_step in expected_steps:
            assert any(expected_step in step_name for step_name in step_names), \
                   f"Expected step {expected_step} not found in {step_names}"
        
        print(f"ETL pipeline steps validation passed! Steps: {step_names}")

    def test_etl_data_transformations(self, check_raw_data):
        """Test specific data transformations in the ETL pipeline."""
        
        # Run ETL pipeline
        pipeline = ETLPipeline()
        context = pipeline.run()
        
        # Load output
        output_file = Path("src/etl_pipeline/data/output/dataset.csv")
        df = pd.read_csv(output_file)
        
        # Test province name standardization
        if 'Province' in df.columns:
            # Check that province names are standardized
            unique_provinces = df['Province'].unique()
            print(f"Unique provinces: {unique_provinces}")
            
            # Should not contain obvious variations like "Madrid" vs "MADRID"
            province_variations = []
            for province in unique_provinces:
                if pd.notna(province):
                    similar = [p for p in unique_provinces 
                             if pd.notna(p) and p.lower() == province.lower() and p != province]
                    if similar:
                        province_variations.extend([province] + similar)
            
            assert len(province_variations) == 0, \
                   f"Found province name variations that should be standardized: {province_variations}"
        
        # Test data type conversions
        numeric_columns = ['Air Pollution Level', 'Respiratory_diseases_total', 
                          'Life_expectancy_total', 'pib']
        
        for col in numeric_columns:
            if col in df.columns:
                assert pd.api.types.is_numeric_dtype(df[col]), \
                       f"Column {col} should be numeric but is {df[col].dtype}"
        
        print("ETL data transformations validation passed!")

    def test_etl_output_completeness(self, check_raw_data):
        """Test that ETL output contains expected data from all sources."""
        
        # Run ETL pipeline
        pipeline = ETLPipeline()
        context = pipeline.run()
        
        # Load output
        output_file = Path("src/etl_pipeline/data/output/dataset.csv")
        df = pd.read_csv(output_file)
        
        # Check that we have data from all expected sources
        data_source_indicators = {
            'air_quality': ['Air Pollution Level', 'Air Pollutant'],
            'health': ['Respiratory_diseases_total', 'Life_expectancy_total'], 
            'socioeconomic': ['pib', 'Population']
        }
        
        missing_sources = []
        for source, indicators in data_source_indicators.items():
            found_indicators = [ind for ind in indicators if ind in df.columns]
            if not found_indicators:
                missing_sources.append(source)
        
        if missing_sources:
            print(f"Warning: No indicators found for data sources: {missing_sources}")
            print(f"Available columns: {list(df.columns)}")
        
        # Verify we have a reasonable amount of data
        assert len(df) > 100, f"Dataset too small: {len(df)} rows"
        assert len(df.columns) >= 10, f"Dataset too narrow: {len(df.columns)} columns"
        
        print(f"ETL output completeness validated! {len(df)} rows, {len(df.columns)} columns")

    def test_etl_performance_metrics(self, check_raw_data):
        """Test ETL pipeline performance and logging."""
        
        import time
        
        # Measure execution time
        start_time = time.time()
        
        pipeline = ETLPipeline()
        context = pipeline.run()
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        # Verify reasonable execution time (should complete within a few minutes)
        assert execution_time < 300, f"ETL pipeline took too long: {execution_time:.2f} seconds"
        
        # Check that context contains timing information if available
        if context and hasattr(context, 'get'):
            timing_info = context.get('execution_time', None)
            if timing_info:
                print(f"ETL execution time from context: {timing_info}")
        
        print(f"ETL pipeline performance test passed! Execution time: {execution_time:.2f} seconds")