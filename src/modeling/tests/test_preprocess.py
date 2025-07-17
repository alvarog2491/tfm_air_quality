import pytest
import pandas as pd
from unittest.mock import patch, MagicMock, mock_open
import tempfile
import os
from pathlib import Path

# Assuming the main file is named 'preprocessing.py' - adjust import as needed
from modeling.preprocess import main


@pytest.fixture
def mock_config():
    """Mock configuration data"""
    return {
        "preprocess": {
            "drop_colnames": ["id", "timestamp"],
            "categorical_features": ["category", "type"],
            "numerical_features": ["value1", "value2", "value3"],
            "validation_size": 0.2,
            "target_column": "target",
            "var_dtypes": {"value1": "float64", "value2": "float64"}
        }
    }


@pytest.fixture
def mock_dataset():
    """Mock dataset for testing"""
    return pd.DataFrame({
        "category": ["A", "B", "A", "B", "A"],
        "type": ["X", "Y", "X", "Y", "X"],
        "value1": [1.0, 2.0, 3.0, 4.0, 5.0],
        "value2": [10.0, 20.0, 30.0, 40.0, 50.0],
        "value3": [100, 200, 300, 400, 500],
        "target": [0, 1, 0, 1, 0]
    })


@pytest.fixture
def temp_files():
    """Create temporary files for testing"""
    with tempfile.TemporaryDirectory() as temp_dir:
        config_file = os.path.join(temp_dir, "config.yaml")
        raw_dataset = os.path.join(temp_dir, "raw_data.csv")
        output_train = os.path.join(temp_dir, "train.csv")
        output_test = os.path.join(temp_dir, "test.csv")
        
        yield {
            "config_file": config_file,
            "raw_dataset": raw_dataset,
            "output_train": output_train,
            "output_test": output_test
        }


class TestMain:
    """Test the main preprocessing function"""
    
    @patch('preprocessing.load_yaml_config')
    @patch('preprocessing.load_raw_dataset')
    @patch('preprocessing.separate_train_evaluate_dataset')
    @patch('preprocessing.validate_no_missing_values')
    @patch('preprocessing.one_hot_encode_categorical_features')
    @patch('preprocessing.scale_numerical_features')
    @patch('preprocessing.logger')
    def test_main_successful_execution(
        self,
        mock_logger,
        mock_scale_features,
        mock_one_hot_encode,
        mock_validate_missing,
        mock_separate_dataset,
        mock_load_dataset,
        mock_load_config,
        mock_config,
        mock_dataset,
        temp_files
    ):
        """Test successful execution of main function"""
        
        # Setup mocks
        mock_load_config.return_value = mock_config
        mock_load_dataset.return_value = mock_dataset
        
        train_df = mock_dataset.iloc[:3]
        test_df = mock_dataset.iloc[3:]
        mock_separate_dataset.return_value = (train_df, test_df)
        
        mock_validate_missing.side_effect = lambda x: x
        mock_one_hot_encode.side_effect = lambda df, **kwargs: df
        mock_scale_features.side_effect = lambda df, **kwargs: (df, MagicMock())
        
        # Execute
        main(
            config_file=temp_files["config_file"],
            raw_dataset=temp_files["raw_dataset"],
            output_train_dataset=temp_files["output_train"],
            output_test_dataset=temp_files["output_test"]
        )
        
        # Verify function calls
        mock_load_config.assert_called_once_with(temp_files["config_file"])
        mock_load_dataset.assert_called_once()
        mock_separate_dataset.assert_called_once()
        assert mock_validate_missing.call_count == 2
        assert mock_one_hot_encode.call_count == 2
        assert mock_scale_features.call_count == 2
        
        # Verify output files exist
        assert os.path.exists(temp_files["output_train"])
        assert os.path.exists(temp_files["output_test"])
    
    @patch('preprocessing.load_yaml_config')
    @patch('preprocessing.load_raw_dataset')
    @patch('preprocessing.logger')
    def test_main_handles_config_loading_error(
        self,
        mock_logger,
        mock_load_dataset,
        mock_load_config,
        temp_files
    ):
        """Test main function handles configuration loading errors"""
        
        mock_load_config.side_effect = FileNotFoundError("Config file not found")
        
        with pytest.raises(FileNotFoundError):
            main(
                config_file=temp_files["config_file"],
                raw_dataset=temp_files["raw_dataset"],
                output_train_dataset=temp_files["output_train"],
                output_test_dataset=temp_files["output_test"]
            )
    
    @patch('preprocessing.load_yaml_config')
    @patch('preprocessing.load_raw_dataset')
    @patch('preprocessing.logger')
    def test_main_handles_dataset_loading_error(
        self,
        mock_logger,
        mock_load_dataset,
        mock_load_config,
        mock_config,
        temp_files
    ):
        """Test main function handles dataset loading errors"""
        
        mock_load_config.return_value = mock_config
        mock_load_dataset.side_effect = FileNotFoundError("Dataset file not found")
        
        with pytest.raises(FileNotFoundError):
            main(
                config_file=temp_files["config_file"],
                raw_dataset=temp_files["raw_dataset"],
                output_train_dataset=temp_files["output_train"],
                output_test_dataset=temp_files["output_test"]
            )
    
    @patch('preprocessing.load_yaml_config')
    @patch('preprocessing.load_raw_dataset')
    @patch('preprocessing.separate_train_evaluate_dataset')
    @patch('preprocessing.validate_no_missing_values')
    @patch('preprocessing.one_hot_encode_categorical_features')
    @patch('preprocessing.scale_numerical_features')
    @patch('preprocessing.logger')
    def test_main_calls_functions_with_correct_parameters(
        self,
        mock_logger,
        mock_scale_features,
        mock_one_hot_encode,
        mock_validate_missing,
        mock_separate_dataset,
        mock_load_dataset,
        mock_load_config,
        mock_config,
        mock_dataset,
        temp_files
    ):
        """Test that main function calls utility functions with correct parameters"""
        
        # Setup mocks
        mock_load_config.return_value = mock_config
        mock_load_dataset.return_value = mock_dataset
        
        train_df = mock_dataset.iloc[:3]
        test_df = mock_dataset.iloc[3:]
        mock_separate_dataset.return_value = (train_df, test_df)
        
        mock_validate_missing.side_effect = lambda x: x
        mock_one_hot_encode.side_effect = lambda df, **kwargs: df
        mock_scale_features.side_effect = lambda df, **kwargs: (df, MagicMock())
        
        # Execute
        main(
            config_file=temp_files["config_file"],
            raw_dataset=temp_files["raw_dataset"],
            output_train_dataset=temp_files["output_train"],
            output_test_dataset=temp_files["output_test"]
        )
        
        # Verify function calls with correct parameters
        mock_load_dataset.assert_called_once_with(
            filepath=temp_files["raw_dataset"],
            drop_columns=mock_config["preprocess"]["drop_colnames"],
            var_dtypes=mock_config["preprocess"]["var_dtypes"]
        )
        
        mock_separate_dataset.assert_called_once_with(
            df=mock_dataset,
            size=mock_config["preprocess"]["validation_size"]
        )
        
        # Verify one-hot encoding calls
        expected_categorical = mock_config["preprocess"]["categorical_features"]
        mock_one_hot_encode.assert_any_call(
            df=train_df,
            categorical_columns=expected_categorical
        )
        mock_one_hot_encode.assert_any_call(
            df=test_df,
            categorical_columns=expected_categorical
        )
        
        # Verify scaling calls
        expected_numerical = mock_config["preprocess"]["numerical_features"]
        mock_scale_features.assert_any_call(
            df=train_df,
            numerical_features=expected_numerical,
            scaler=None
        )
        mock_scale_features.assert_any_call(
            df=test_df,
            numerical_features=expected_numerical,
            scaler=None
        )
    
    @patch('preprocessing.load_yaml_config')
    @patch('preprocessing.load_raw_dataset')
    @patch('preprocessing.separate_train_evaluate_dataset')
    @patch('preprocessing.validate_no_missing_values')
    @patch('preprocessing.one_hot_encode_categorical_features')
    @patch('preprocessing.scale_numerical_features')
    @patch('preprocessing.logger')
    def test_main_logger_calls(
        self,
        mock_logger,
        mock_scale_features,
        mock_one_hot_encode,
        mock_validate_missing,
        mock_separate_dataset,
        mock_load_dataset,
        mock_load_config,
        mock_config,
        mock_dataset,
        temp_files
    ):
        """Test that logger is called appropriately"""
        
        # Setup mocks
        mock_load_config.return_value = mock_config
        mock_load_dataset.return_value = mock_dataset
        
        train_df = mock_dataset.iloc[:3]
        test_df = mock_dataset.iloc[3:]
        mock_separate_dataset.return_value = (train_df, test_df)
        
        mock_validate_missing.side_effect = lambda x: x
        mock_one_hot_encode.side_effect = lambda df, **kwargs: df
        mock_scale_features.side_effect = lambda df, **kwargs: (df, MagicMock())
        
        # Execute
        main(
            config_file=temp_files["config_file"],
            raw_dataset=temp_files["raw_dataset"],
            output_train_dataset=temp_files["output_train"],
            output_test_dataset=temp_files["output_test"]
        )
        
        # Verify logger calls
        expected_log_messages = [
            "Reading raw data...",
            "Splitting dataset into training and evaluation sets...",
            "Checking for missing values...",
            "One-hot encoding categorical columns...",
            "Scaling features...",
            "Writing processed data...",
            "Done!"
        ]
        
        actual_calls = [call[0][0] for call in mock_logger.info.call_args_list]
        for expected_msg in expected_log_messages:
            assert expected_msg in actual_calls


if __name__ == "__main__":
    pytest.main([__file__])