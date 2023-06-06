import pytest
import pandas as pd
from ETL import Business_CSVs_to_Pandas_Function

# Assuming you have a function called 'your_function' that returns the DataFrame

def test_data_csv_result():
    '''
    Test the get data.csv function
    It returns a DataFrame
    '''
    # Call the get data.csv function to get the result
    result = your_function()

    # Check if the result is a pandas DataFrame
    assert isinstance(result, pd.DataFrame), "Result is not a DataFrame."

    # Check the shape of the DataFrame
    expected_shape = (137, 62)
    assert result.shape == expected_shape, f"Shape is not {expected_shape}."

    # Check the column names of the DataFrame
    expected_columns = ['name', 'trainer', 'Analytic_W1', 'Independent_W1', 'Determined_W1',
                        'Professional_W1', 'Studious_W1', 'Imaginative_W1', 'Analytic_W2',
                        'Independent_W2', 'Determined_W2', 'Professional_W2', 'Studious_W2',
                        'Imaginative_W2', 'Analytic_W3', 'Independent_W3', 'Determined_W3',
                        'Professional_W3', 'Studious_W3', 'Imaginative_W3', 'Analytic_W4',
                        'Independent_W4', 'Determined_W4', 'Professional_W4', 'Studious_W4',
                        'Imaginative_W4', 'Analytic_W5', 'Independent_W5', 'Determined_W5',
                        'Professional_W5', 'Studious_W5', 'Imaginative_W5', 'Analytic_W6',
                        'Independent_W6', 'Determined_W6', 'Professional_W6', 'Studious_W6',
                        'Imaginative_W6', 'Analytic_W7', 'Independent_W7', 'Determined_W7',
                        'Professional_W7', 'Studious_W7', 'Imaginative_W7', 'Analytic_W8',
                        'Independent_W8', 'Determined_W8', 'Professional_W8', 'Studious_W8',
                        'Imaginative_W8', 'Analytic_W9', 'Independent_W9', 'Determined_W9',
                        'Professional_W9', 'Studious_W9', 'Imaginative_W9', 'Analytic_W10',
                        'Independent_W10', 'Determined_W10', 'Professional_W10', 'Studious_W10',
                        'Imaginative_W10']
    assert list(result.columns) == expected_columns, "Columns do not match the expected values."


def test_business_csv_result():
    '''
    Test the get_business.csv function
    It returns a DataFrame
    '''
    # Call the get data.csv function to get the result
    Folder = 'Academy/Business'
    result = Business_CSVs_to_Pandas_Function.get_csv(Folder)

    # Check if the result is a pandas DataFrame
    assert isinstance(result, pd.DataFrame), "Result is not a DataFrame."

    # Check the shape of the DataFrame
    expected_shape = (121,62)
    assert result.shape == expected_shape, f"Shape is not {expected_shape}."

    # Check the column names of the DataFrame
    expected_columns = ['name', 'trainer', 'Analytic_W1', 'Independent_W1', 'Determined_W1',
                        'Professional_W1', 'Studious_W1', 'Imaginative_W1', 'Analytic_W2',
                        'Independent_W2', 'Determined_W2', 'Professional_W2', 'Studious_W2',
                        'Imaginative_W2', 'Analytic_W3', 'Independent_W3', 'Determined_W3',
                        'Professional_W3', 'Studious_W3', 'Imaginative_W3', 'Analytic_W4',
                        'Independent_W4', 'Determined_W4', 'Professional_W4', 'Studious_W4',
                        'Imaginative_W4', 'Analytic_W5', 'Independent_W5', 'Determined_W5',
                        'Professional_W5', 'Studious_W5', 'Imaginative_W5', 'Analytic_W6',
                        'Independent_W6', 'Determined_W6', 'Professional_W6', 'Studious_W6',
                        'Imaginative_W6', 'Analytic_W7', 'Independent_W7', 'Determined_W7',
                        'Professional_W7', 'Studious_W7', 'Imaginative_W7', 'Analytic_W8',
                        'Independent_W8', 'Determined_W8', 'Professional_W8', 'Studious_W8',
                        'Imaginative_W8', 'Analytic_W9', 'Independent_W9', 'Determined_W9',
                        'Professional_W9', 'Studious_W9', 'Imaginative_W9', 'Analytic_W10',
                        'Independent_W10', 'Determined_W10', 'Professional_W10', 'Studious_W10',
                        'Imaginative_W10']
    assert list(result.columns) == expected_columns, "Columns do not match the expected values."
