import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
import tempfile
import os
from modeling.preprocess import main


@pytest.fixture
def mock_config():
    """Mock configuration data for testing preprocessing parameters"""
    return {
        "preprocess": {
            "drop_colnames": ["id", "timestamp"],
            "categorical_features": ["category", "type"],
            "numerical_features": ["value1", "value2", "value3"],
            "validation_size": 0.2,
            "target_column": "target",
            "var_dtypes": {"value1": "float64", "value2": "float64"},
        }
    }


@pytest.fixture
def mock_dataset():
    """Mock dataset for testing with sample data"""
    return pd.DataFrame(
        {
            "category": ["A", "B", "A", "B", "A"],
            "type": ["X", "Y", "X", "Y", "X"],
            "value1": [1.0, 2.0, 3.0, 4.0, 5.0],
            "value2": [10.0, 20.0, 30.0, 40.0, 50.0],
            "value3": [100, 200, 300, 400, 500],
            "target": [0, 1, 0, 1, 0],
        }
    )


@pytest.fixture
def temp_files():
    """Create temporary files for testing file operations"""
    with tempfile.TemporaryDirectory() as temp_dir:
        config_file = os.path.join(temp_dir, "config.yaml")
        raw_dataset = os.path.join(temp_dir, "raw_data.csv")
        output_train = os.path.join(temp_dir, "train.csv")
        output_test = os.path.join(temp_dir, "test.csv")

        yield {
            "config_file": config_file,
            "raw_dataset": raw_dataset,
            "output_train": output_train,
            "output_test": output_test,
        }


class TestMain:
    """Test suite for the main preprocess function"""

    @patch("modeling.preprocess.joblib.dump")
    @patch("modeling.preprocess.validate_file_exists")
    @patch("modeling.preprocess.load_yaml_config")
    @patch("modeling.preprocess.load_raw_dataset")
    @patch("modeling.preprocess.separate_train_evaluate_dataset")
    @patch("modeling.preprocess.validate_no_missing_values")
    @patch("modeling.preprocess.one_hot_encode_categorical_features")
    @patch("modeling.preprocess.scale_numerical_features")
    @patch("modeling.preprocess.logger")
    def test_main_successful_execution(
        self,
        mock_logger,
        mock_scale_features,
        mock_one_hot_encode,
        mock_validate_missing,
        mock_separate_dataset,
        mock_load_dataset,
        mock_load_config,
        mock_validate_file_exists,
        mock_joblib_dump,
        mock_config,
        mock_dataset,
        temp_files,
    ):
        """Test successful execution of main function with all steps"""

        # Setup mocks to return expected values
        mock_load_config.return_value = mock_config
        mock_load_dataset.return_value = mock_dataset

        # Split dataset for train/test
        train_df = mock_dataset.iloc[:3]
        test_df = mock_dataset.iloc[3:]
        mock_separate_dataset.return_value = (train_df, test_df)

        # Configure mocks to pass through data unchanged
        mock_validate_missing.side_effect = lambda x: x
        mock_one_hot_encode.side_effect = lambda df, **kwargs: df
        mock_scale_features.side_effect = lambda df, **kwargs: (df, MagicMock())

        # Execute main function
        main(
            config_file=temp_files["config_file"],
            raw_dataset=temp_files["raw_dataset"],
            output_train_dataset=temp_files["output_train"],
            output_test_dataset=temp_files["output_test"],
        )

        # Verify all functions were called once
        mock_load_config.assert_called_once_with(temp_files["config_file"])
        mock_load_dataset.assert_called_once()
        mock_separate_dataset.assert_called_once()
        # Validation, encoding, and scaling should be called for both train and test
        assert mock_validate_missing.call_count == 2
        assert mock_one_hot_encode.call_count == 2
        assert mock_scale_features.call_count == 2

        # Verify output files were created
        assert os.path.exists(temp_files["output_train"])
        assert os.path.exists(temp_files["output_test"])

    @patch("modeling.preprocess.validate_file_exists")
    @patch("modeling.preprocess.load_yaml_config")
    @patch("modeling.preprocess.load_raw_dataset")
    @patch("modeling.preprocess.logger")
    def test_main_handles_config_loading_error(
        self, mock_logger, mock_load_dataset, mock_load_config, mock_validate_file_exists, temp_files
    ):
        """Test main function handles configuration loading errors properly"""

        # Mock config loading to raise FileNotFoundError
        mock_load_config.side_effect = FileNotFoundError("Config file not found")

        # Verify exception is raised
        with pytest.raises(FileNotFoundError):
            main(
                config_file=temp_files["config_file"],
                raw_dataset=temp_files["raw_dataset"],
                output_train_dataset=temp_files["output_train"],
                output_test_dataset=temp_files["output_test"],
            )

    @patch("modeling.preprocess.validate_file_exists")
    @patch("modeling.preprocess.load_yaml_config")
    @patch("modeling.preprocess.load_raw_dataset")
    @patch("modeling.preprocess.logger")
    def test_main_handles_dataset_loading_error(
        self, mock_logger, mock_load_dataset, mock_load_config, mock_validate_file_exists, mock_config, temp_files
    ):
        """Test main function handles dataset loading errors properly"""

        # Mock successful config loading but failed dataset loading
        mock_load_config.return_value = mock_config
        mock_load_dataset.side_effect = FileNotFoundError("Dataset file not found")

        # Verify exception is raised
        with pytest.raises(FileNotFoundError):
            main(
                config_file=temp_files["config_file"],
                raw_dataset=temp_files["raw_dataset"],
                output_train_dataset=temp_files["output_train"],
                output_test_dataset=temp_files["output_test"],
            )

    @patch("modeling.preprocess.joblib.dump")
    @patch("modeling.preprocess.validate_file_exists")
    @patch("modeling.preprocess.load_yaml_config")
    @patch("modeling.preprocess.load_raw_dataset")
    @patch("modeling.preprocess.separate_train_evaluate_dataset")
    @patch("modeling.preprocess.validate_no_missing_values")
    @patch("modeling.preprocess.one_hot_encode_categorical_features")
    @patch("modeling.preprocess.scale_numerical_features")
    @patch("modeling.preprocess.logger")
    def test_main_calls_functions_with_correct_parameters(
        self,
        mock_logger,
        mock_scale_features,
        mock_one_hot_encode,
        mock_validate_missing,
        mock_separate_dataset,
        mock_load_dataset,
        mock_load_config,
        mock_validate_file_exists,
        mock_joblib_dump,
        mock_config,
        mock_dataset,
        temp_files,
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

        # Execute main function
        main(
            config_file=temp_files["config_file"],
            raw_dataset=temp_files["raw_dataset"],
            output_train_dataset=temp_files["output_train"],
            output_test_dataset=temp_files["output_test"],
        )

        # Verify dataset loading parameters
        mock_load_dataset.assert_called_once_with(
            filepath=temp_files["raw_dataset"],
            drop_columns=mock_config["preprocess"]["drop_colnames"],
            var_dtypes=mock_config["preprocess"]["var_dtypes"],
        )

        # Verify dataset separation parameters
        mock_separate_dataset.assert_called_once_with(
            df=mock_dataset, size=mock_config["preprocess"]["validation_size"]
        )

        # Verify one-hot encoding called with correct categorical columns
        expected_categorical = mock_config["preprocess"]["categorical_features"]
        mock_one_hot_encode.assert_any_call(
            df=train_df, categorical_columns=expected_categorical
        )

        # Verify scaling called with correct numerical features
        expected_numerical = mock_config["preprocess"]["numerical_features"]
        mock_scale_features.assert_any_call(
            df=train_df, numerical_features=expected_numerical, scaler=None
        )

    @patch("modeling.preprocess.joblib.dump")
    @patch("modeling.preprocess.validate_file_exists")
    @patch("modeling.preprocess.load_yaml_config")
    @patch("modeling.preprocess.load_raw_dataset")
    @patch("modeling.preprocess.separate_train_evaluate_dataset")
    @patch("modeling.preprocess.validate_no_missing_values")
    @patch("modeling.preprocess.one_hot_encode_categorical_features")
    @patch("modeling.preprocess.scale_numerical_features")
    @patch("modeling.preprocess.logger")
    def test_main_logger_calls(
        self,
        mock_logger,
        mock_scale_features,
        mock_one_hot_encode,
        mock_validate_missing,
        mock_separate_dataset,
        mock_load_dataset,
        mock_load_config,
        mock_validate_file_exists,
        mock_joblib_dump,
        mock_config,
        mock_dataset,
        temp_files,
    ):
        """Test that logger is called with appropriate messages"""

        # Setup mocks
        mock_load_config.return_value = mock_config
        mock_load_dataset.return_value = mock_dataset

        train_df = mock_dataset.iloc[:3]
        test_df = mock_dataset.iloc[3:]
        mock_separate_dataset.return_value = (train_df, test_df)

        mock_validate_missing.side_effect = lambda x: x
        mock_one_hot_encode.side_effect = lambda df, **kwargs: df
        mock_scale_features.side_effect = lambda df, **kwargs: (df, MagicMock())

        # Execute main function
        main(
            config_file=temp_files["config_file"],
            raw_dataset=temp_files["raw_dataset"],
            output_train_dataset=temp_files["output_train"],
            output_test_dataset=temp_files["output_test"],
        )

        # Verify logger was called with expected messages
        expected_log_messages = [
            "Reading raw data...",
            "Splitting dataset into training and evaluation sets...",
            "Checking for missing values...",
            "One-hot encoding categorical columns...",
            "Scaling features...",
            "Writing processed data...",
            "Done!",
        ]

        # Check that all expected messages were logged
        actual_calls = [call[0][0] for call in mock_logger.info.call_args_list]
        for expected_msg in expected_log_messages:
            assert expected_msg in actual_calls


if __name__ == "__main__":
    pytest.main([__file__])
