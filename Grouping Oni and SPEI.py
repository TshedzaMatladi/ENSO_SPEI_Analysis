# -*- coding: utf-8 -*-
"""
Created on Sat Oct  5 09:38:11 2024

@author: MatladiT
"""

import os
import pandas as pd
from scipy.stats import norm

# Load ONI data from the provided Excel file
def load_oni_data(oni_file):
    # Load ONI data from the provided file path
    oni_df = pd.read_excel(oni_file)

    # Rename columns to match expected format
    oni_df.rename(columns={'YR': 'Year', 'SEAS': 'Season', 'ANOM': 'ONI'}, inplace=True)

    # Map seasons to corresponding months (last month in each season)
    season_to_month = {
        'DJF': 2,  # February
        'MAM': 5,  # May
        'JJA': 8,  # August
        'SON': 11  # November
    }

    # Create a 'Month' column based on the 'Season' column
    oni_df['Month'] = oni_df['Season'].map(season_to_month)

    # Classify the ENSO phase based on ONI values
    oni_df['ENSO_Phase'] = oni_df['ONI'].apply(classify_enso_phase)
    
    return oni_df

# Function to classify ENSO phase based on ONI value
def classify_enso_phase(oni_value):
    if oni_value >= 0.5:
        return 'El Niño'
    elif oni_value <= -0.5:
        return 'La Niña'
    else:
        return 'Neutral'

# Function to calculate water balance (Rain - Penman-Monteith ET0)
def calculate_water_balance_pm(df):
    required_columns = ['Rain', 'PM ET0']
    if not all(col in df.columns for col in required_columns):
        raise KeyError(f"Missing required columns: {required_columns}")
    
    df['Rain'] = pd.to_numeric(df['Rain'], errors='coerce')
    df['PM ET0'] = pd.to_numeric(df['PM ET0'], errors='coerce')

    # Calculate Water Balance (Penman-Monteith method)
    df['Water_Balance_PM'] = df['Rain'] - df['PM ET0']
    
    return df

# Function to calculate cumulative monthly water balance
def calculate_cumulative_water_balance(df):
    if all(col in df.columns for col in ['Year', 'Month', 'Day']):
        df['Date'] = pd.to_datetime(df[['Year', 'Month', 'Day']], errors='coerce')
    elif 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    else:
        raise KeyError("No suitable date columns (Year, Month, Day or Date) found in the data.")
    
    if df['Date'].isnull().any():
        raise ValueError("Invalid or missing dates in the dataset.")
    
    df.set_index('Date', inplace=True)

    # Resample by month and calculate cumulative water balance for Penman-Monteith
    df_monthly = df.resample('M').agg({
        'Water_Balance_PM': 'sum',
        'Year': 'first',
        'Month': 'first'
    }).reset_index()

    return df_monthly

# Function to calculate SPEI
def calculate_spei(df, column_name):
    df[f'{column_name}_rolling'] = df[column_name].rolling(window=12, min_periods=1).sum()
    df[f'SPEI_{column_name}'] = (df[f'{column_name}_rolling'] - df[f'{column_name}_rolling'].mean()) / df[f'{column_name}_rolling'].std()
    df[f'SPEI_Category_{column_name}'] = df[f'SPEI_{column_name}'].apply(categorize_spei)
    return df

# Function to categorize SPEI values (grouping Moderate and Severe conditions)
def categorize_spei(spei_value):
    if spei_value > 2:
        return "Extremely Wet"
    elif 1 < spei_value <= 2:
        return "Wet"  # Grouping Moderately Wet and Severely Wet
    elif -1 < spei_value <= 1:
        return "Normal"
    elif -2 < spei_value <= -1:
        return "Dry"  # Grouping Moderately Dry and Severely Dry
    else:
        return "Extremely Dry"

# Function to merge station data with ONI and classify ENSO phase
def merge_with_oni(station_df, oni_df):
    merged_df = pd.merge(station_df, oni_df, on=['Year', 'Month'], how='left')
    return merged_df

# Count occurrences of grouped SPEI conditions per ENSO phase
def count_spei_conditions_per_enso(df_with_oni):
    # Group by ENSO phase and SPEI category, then count occurrences
    spei_enso_counts = df_with_oni.groupby(['ENSO_Phase', 'SPEI_Category_Water_Balance_PM']).size().reset_index(name='Count')

    # Pivot to display counts by ENSO phase and condition
    spei_enso_pivot = spei_enso_counts.pivot(index='ENSO_Phase', columns='SPEI_Category_Water_Balance_PM', values='Count').fillna(0)

    # Group wet and dry conditions
    spei_enso_pivot['Total_Wet'] = spei_enso_pivot.get('Wet', 0) + spei_enso_pivot.get('Extremely Wet', 0)
    spei_enso_pivot['Total_Dry'] = spei_enso_pivot.get('Dry', 0) + spei_enso_pivot.get('Extremely Dry', 0)

    return spei_enso_pivot[['Total_Wet', 'Total_Dry', 'Normal']]  # Return grouped counts

# Function to process each station file
def process_station(file_path, oni_df):
    df = pd.read_excel(file_path)
    
    # Step 1: Calculate daily water balance
    try:
        df = calculate_water_balance_pm(df)
    except KeyError as e:
        print(f"Error in {file_path}: {e}")
        return None

    # Step 2: Calculate monthly cumulative water balance
    try:
        df_monthly = calculate_cumulative_water_balance(df)
    except (KeyError, ValueError) as e:
        print(f"Error in {file_path}: {e}")
        return None

    # Step 3: Calculate SPEI
    df_monthly = calculate_spei(df_monthly, 'Water_Balance_PM')

    # Step 4: Merge with ONI data and classify ENSO phase
    df_with_oni = merge_with_oni(df_monthly, oni_df)

    return df_with_oni

# Function to process all station files in a directory and save to one Excel file with multiple sheets
def process_all_stations(directory, output_file, oni_file):
    # Load the ONI data
    oni_df = load_oni_data(oni_file)

    # Create an ExcelWriter object to write multiple sheets to one Excel file
    with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
        for filename in os.listdir(directory):
            if filename.startswith('~$') or filename.endswith('_spei_results.xlsx') or filename.startswith('combined_spei_results'):
                continue
            if filename.endswith(".xlsx") or filename.endswith(".xls"):
                file_path = os.path.join(directory, filename)
                print(f"Processing station file: {filename}")
                try:
                    station_result = process_station(file_path, oni_df)

                    if station_result is not None:
                        # Count SPEI condition occurrences per ENSO phase
                        spei_enso_counts = count_spei_conditions_per_enso(station_result)

                        # Save the SPEI and ENSO phase results
                        station_name = os.path.splitext(filename)[0]
                        station_name_cleaned = station_name.replace('.', '')
                        spei_enso_counts.to_excel(writer, sheet_name=station_name_cleaned[:31], index=True)
                        print(f"Saved SPEI and ENSO phase results for {station_name_cleaned} to sheet in {output_file}")

                except KeyError as e:
                    print(f"Error processing {filename}: {e}")
                except Exception as e:
                    print(f"Unexpected error processing {filename}: {e}")

# Set the directory path and ONI file path (adjust for your local environment)
directory_path = r'C:\Users\matladit\SPEI READY'
oni_file = r'C:\Users\matladit\ONI\ONI_Sep2024.xlsx'

# Output file where all the station results will be saved
output_file = os.path.join(directory_path, 'combined_spei_and_oni_results.xlsx')

# Process all stations in the directory and save results to one Excel file with multiple sheets
process_all_stations(directory_path, output_file, oni_file)
