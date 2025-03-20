from airflow import DAG
from airflow.decorators import task
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import json
import logging
import pandas as pd
import pandas.errors
import io
from typing import Dict, List, Optional

# Airflow Variables
AWS_CONN_ID = "etl_aws_conn_id"
S3_BUCKET = "25dataengineering-streaming-data"
S3_METADATA_FOLDER = "Raw_data/"
S3_PROCESSED_FOLDER = "transformed/"
S3_STREAMS_FOLDER = "streams/"  # New folder for streams data

# File names (can be moved to Airflow Variables)
STREAM_DATA_FILE = "stream_data.csv"
USERS_DATA_FILE = "users_data.csv"

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

dag = DAG(
    'data_transformation_pipeline',
    default_args=default_args,
    description='Extract data from metadata, transform it, and store it in processed folder',
    schedule_interval="@daily",
    catchup=False,
    tags=['transform', 'kpi', 's3'],
    max_active_runs=1
)


@task(task_id="extract_metadata")
def extract_metadata(**context) -> Dict:
    """Extracts data from S3 Raw_data folder."""
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    raw_data_keys = s3_hook.list_keys(bucket_name=S3_BUCKET, prefix=S3_METADATA_FOLDER)

    # Group files by type - match the timestamp pattern in filenames
    stream_files = [key for key in raw_data_keys if "_stream_data" in key]
    users_files = [key for key in raw_data_keys if "_users_data" in key]

    # Sort files by timestamp (newest first)
    stream_files.sort(reverse=True)
    users_files.sort(reverse=True)

    logging.info(f"Found {len(stream_files)} stream files and {len(users_files)} users files")
    logging.info(f"Stream files: {stream_files}")
    logging.info(f"Users files: {users_files}")

    # Use latest files of each type
    data_frames = {}

    # Process stream data
    if stream_files:
        latest_stream = stream_files[0]
        try:
            obj = s3_hook.get_key(latest_stream, bucket_name=S3_BUCKET)
            file_content = obj.get()['Body'].read().decode('utf-8')
            df = pd.read_csv(io.StringIO(file_content))

            # Log the actual columns for debugging
            logging.info(f"Stream data columns: {list(df.columns)}")

            # Map columns to expected names if needed
            column_mapping = {
                'listener_id': 'user_id',
                'played_at': 'timestamp',
                'song_id': 'track_id',
                'length_ms': 'duration_ms',
                'genre': 'track_genre',
                'artist': 'artists'
            }

            # Rename columns if they exist with different names
            for old_col, new_col in column_mapping.items():
                if old_col in df.columns and new_col not in df.columns:
                    df = df.rename(columns={old_col: new_col})

            data_frames[f"{S3_METADATA_FOLDER}{STREAM_DATA_FILE}"] = df
            logging.info(f"Successfully read and processed stream file: {latest_stream}")
        except Exception as e:
            logging.error(f"Error processing stream file {latest_stream}: {str(e)}")
            logging.error(f"Exception type: {type(e).__name__}")
            if isinstance(e, pandas.errors.ParserError):
                logging.error(f"CSV parsing error: {str(e)}")

    # Process users data
    if users_files:
        latest_users = users_files[0]
        try:
            obj = s3_hook.get_key(latest_users, bucket_name=S3_BUCKET)
            file_content = obj.get()['Body'].read().decode('utf-8')
            df = pd.read_csv(io.StringIO(file_content))

            # Log the actual columns for debugging
            logging.info(f"Users data columns: {list(df.columns)}")

            # Map columns to expected names if needed
            column_mapping = {
                'listener_id': 'user_id'
                # Add other mappings as needed
            }

            # Rename columns if they exist with different names
            for old_col, new_col in column_mapping.items():
                if old_col in df.columns and new_col not in df.columns:
                    df = df.rename(columns={old_col: new_col})

            data_frames[f"{S3_METADATA_FOLDER}{USERS_DATA_FILE}"] = df
            logging.info(f"Successfully read and processed users file: {latest_users}")
        except Exception as e:
            logging.error(f"Error processing users file {latest_users}: {str(e)}")

    if not data_frames:
        logging.warning("No data frames were successfully loaded!")

    return data_frames

@task(task_id="validate_metadata")
def validate_metadata(metadata: Dict) -> Dict:
    """Validates that all required data and columns exist before transformation."""
    logging.info("Starting data validation")

    required_files = [f"{S3_METADATA_FOLDER}{STREAM_DATA_FILE}", f"{S3_METADATA_FOLDER}{USERS_DATA_FILE}"]
    missing_files = [file for file in required_files if file not in metadata or metadata[file].empty]

    if missing_files:
        logging.error(f"Missing required files: {missing_files}")
        raise ValueError(f"Missing required files: {missing_files}")

    stream_data = metadata[f"{S3_METADATA_FOLDER}{STREAM_DATA_FILE}"]
    user_data = metadata[f"{S3_METADATA_FOLDER}{USERS_DATA_FILE}"]

    # Log all columns for debugging
    logging.info(f"Stream data columns: {list(stream_data.columns)}")
    logging.info(f"User data columns: {list(user_data.columns)}")

    # Define required columns for stream_data
    required_stream_cols = ['user_id', 'timestamp']  # Update as needed

    # Check for missing columns in stream_data
    missing_stream_cols = [col for col in required_stream_cols if col not in stream_data.columns]
    if missing_stream_cols:
        logging.warning(f"Missing required columns in stream_data: {missing_stream_cols}")

        # Add default values for missing columns
        if 'user_id' not in stream_data.columns:
            stream_data['user_id'] = "unknown_user"
            logging.warning("'user_id' column missing. Added default value: 'unknown_user'.")
        if 'timestamp' not in stream_data.columns:
            stream_data['timestamp'] = pd.Timestamp.now()
            logging.warning("'timestamp' column missing. Added current timestamp as default value.")

    # Define required columns for user_data
    required_user_cols = ['user_id']

    # Check for missing columns in user_data
    missing_user_cols = [col for col in required_user_cols if col not in user_data.columns]
    if missing_user_cols:
        logging.error(f"Missing required columns in users_data: {missing_user_cols}")
        raise ValueError(f"Missing required columns in users_data: {missing_user_cols}")

    # Rest of the validation logic remains the same
    # ...

    logging.info("Data validation completed successfully")
    # Update the metadata with our modified DataFrames
    metadata[f"{S3_METADATA_FOLDER}{STREAM_DATA_FILE}"] = stream_data
    metadata[f"{S3_METADATA_FOLDER}{USERS_DATA_FILE}"] = user_data
    return metadata


@task(task_id="merge_streams_data")
def merge_streams_data(validated_metadata: Dict) -> Dict:
    """Merges data from Raw_data folder with files in the streams folder."""
    logging.info("Starting data merge with streams folder")
    
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    
    stream_data = validated_metadata[f"{S3_METADATA_FOLDER}{STREAM_DATA_FILE}"]
    user_data = validated_metadata[f"{S3_METADATA_FOLDER}{USERS_DATA_FILE}"]
    
    # List specific stream files we know exist
    expected_files = [
        f"{S3_STREAMS_FOLDER}streams1.csv",
        f"{S3_STREAMS_FOLDER}streams2.csv",
        f"{S3_STREAMS_FOLDER}streams3.csv"
    ]
    
    # Check if these files exist and process them
    merged_results = []
    
    for stream_file in expected_files:
        try:
            logging.info(f"Attempting to process: {stream_file}")
            
            obj = s3_hook.get_key(stream_file, bucket_name=S3_BUCKET)
            if obj is None:
                logging.warning(f"Could not retrieve object: {stream_file}")
                continue
                
            file_content = obj.get()['Body'].read().decode('utf-8')
            stream_df = pd.read_csv(io.StringIO(file_content))
            
            # Log columns for debugging
            logging.info(f"Columns in {stream_file}: {list(stream_df.columns)}")
            
            # Standardize column names if needed
            column_mapping = {
                'listener_id': 'user_id',
                'played_at': 'timestamp',
                'song_id': 'track_id',
                'length_ms': 'duration_ms',
                'genre': 'track_genre',
                'artist': 'artists'
            }
            
            # Rename columns if they exist with different names
            for old_col, new_col in column_mapping.items():
                if old_col in stream_df.columns and new_col not in stream_df.columns:
                    stream_df = stream_df.rename(columns={old_col: new_col})
            
            # Ensure required columns exist
            required_cols = ['track_id', 'user_id']
            missing_cols = [col for col in required_cols if col not in stream_df.columns]
            
            if missing_cols:
                logging.warning(f"File {stream_file} missing required columns: {missing_cols}")
                continue
            
            # Merge with user data using user_id
            merged_complete = pd.merge(
                stream_df,
                user_data,
                on='user_id',
                how='left',
                suffixes=('', '_user')
            )
            
            # Add source information
            merged_complete['source_file'] = stream_file
            
            merged_results.append(merged_complete)
            logging.info(f"Processed {stream_file} - merged {len(merged_complete)} rows")
            
        except Exception as e:
            logging.error(f"Error processing stream file {stream_file}: {str(e)}")
            continue
    
    # Combine all merged results
    if merged_results:
        new_merge = pd.concat(merged_results, ignore_index=True)
        logging.info(f"First merge (new_merge) contains {len(new_merge)} rows")
        
        # Clean up duplicate columns that might have been created during merges
        columns_to_drop = [col for col in new_merge.columns if col.endswith('_user')]
        if columns_to_drop:
            new_merge = new_merge.drop(columns=columns_to_drop)
            logging.info(f"Dropped duplicate columns: {columns_to_drop}")
        
        return {
            "new_merge": new_merge,
            "original_metadata": validated_metadata
        }
    else:
        logging.warning("No streams data was successfully merged, using original data")
        # If no merges were successful, just merge the original metadata
        return {
            "new_merge": stream_data.merge(user_data, on='user_id', how='left'),
            "original_metadata": validated_metadata
        }


@task(task_id="merge_with_track_id")
def merge_with_track_id(merged_data_dict: Dict) -> Dict:
    """Merges new_merge with streams data using track_id."""
    logging.info("Starting second merge using track_id")
    
    new_merge = merged_data_dict["new_merge"]
    stream_data = merged_data_dict["original_metadata"][f"{S3_METADATA_FOLDER}{STREAM_DATA_FILE}"]
    
    # Ensure required columns exist
    required_cols = ['track_id']
    missing_cols = [col for col in required_cols if col not in new_merge.columns]
    
    if missing_cols:
        logging.error(f"Missing required columns in new_merge: {missing_cols}")
        raise ValueError(f"Missing required columns in new_merge: {missing_cols}")
    
    # Perform the merge using track_id
    final_merge = pd.merge(
        new_merge,
        stream_data,
        on='track_id',
        how='left',
        suffixes=('', '_stream')
    )
    
    # Clean up duplicate columns
    columns_to_drop = [col for col in final_merge.columns if col.endswith('_stream')]
    if columns_to_drop:
        final_merge = final_merge.drop(columns=columns_to_drop)
        logging.info(f"Dropped duplicate columns: {columns_to_drop}")
    
    logging.info(f"Second merge (final_merge) contains {len(final_merge)} rows")
    
    return {
        "final_merge": final_merge,
        "original_metadata": merged_data_dict["original_metadata"]
    }

@task(task_id="perform_transformations")
def perform_transformations(merged_data_dict: Dict) -> Dict:
    """Performs data cleaning, computes KPIs, and prepares final dataset."""
    logging.info("Starting data transformation")

    # Use the merged data if available
    merged_data = merged_data_dict["final_merge"]
    
    # Make a copy to avoid modifying the original
    merged_data = merged_data.copy()

    # Clean duplicates
    merged_data.drop_duplicates(inplace=True)

    # Fill NA values for required columns
    merged_data['duration_ms'] = merged_data['duration_ms'].fillna(0)
    
    # Ensure timestamp is in datetime format
    if 'timestamp' in merged_data.columns:
        merged_data['timestamp'] = pd.to_datetime(merged_data['timestamp'])
        merged_data['hour'] = merged_data['timestamp'].dt.strftime('%Y-%m-%d %H')
    else:
        # If timestamp is missing, create a placeholder hour column
        merged_data['hour'] = 'unknown'

    # Genre KPIs calculation
    if 'track_genre' in merged_data.columns:
        genre_kpis = merged_data.groupby('track_genre').agg(
            listen_count=('track_id', 'count'),
            avg_duration=('duration_ms', 'mean')
        )

        if 'likes' in merged_data.columns and 'shares' in merged_data.columns:
            genre_popularity = merged_data.groupby('track_genre').agg(
                play_count=('track_id', 'count'),
                like_count=('likes', 'sum'),
                share_count=('shares', 'sum')
            )

            for col in ['play_count', 'like_count', 'share_count']:
                max_val = genre_popularity[col].max()
                if max_val > 0:
                    genre_popularity[f'{col}_norm'] = genre_popularity[col] / max_val
                else:
                    genre_popularity[f'{col}_norm'] = 0

            genre_popularity['popularity_index'] = (
                                                       0.5 * genre_popularity['play_count_norm'] +
                                                       0.3 * genre_popularity['like_count_norm'] +
                                                       0.2 * genre_popularity['share_count_norm']
                                               ) * 100

            genre_kpis['popularity_index'] = genre_popularity['popularity_index']
        else:
            max_listen = genre_kpis['listen_count'].max()
            if max_listen > 0:
                genre_kpis['popularity_index'] = (genre_kpis['listen_count'] / max_listen) * 100
            else:
                genre_kpis['popularity_index'] = 0

        most_popular_tracks = {}
        for genre in merged_data['track_genre'].unique():
            genre_data = merged_data[merged_data['track_genre'] == genre]
            if not genre_data.empty:
                track_counts = genre_data['track_id'].value_counts()
                if not track_counts.empty:
                    most_popular_tracks[genre] = track_counts.index[0]
                else:
                    most_popular_tracks[genre] = 'Unknown'

        genre_kpis['most_popular_track'] = genre_kpis.index.map(
            lambda x: most_popular_tracks.get(x, 'Unknown')
        )

        genre_kpis = genre_kpis.reset_index()
    else:
        # Create empty genre_kpis if track_genre is missing
        genre_kpis = pd.DataFrame(columns=['track_genre', 'listen_count', 'avg_duration', 'popularity_index', 'most_popular_track'])

    # Hourly KPIs calculation
    hourly_kpis = merged_data.groupby('hour').agg(
        unique_listeners=('user_id', 'nunique'),
        total_plays=('track_id', 'count')
    )

    top_artists_by_hour = {}
    for hour in merged_data['hour'].unique():
        hour_data = merged_data[merged_data['hour'] == hour]
        if not hour_data.empty and 'artists' in hour_data.columns:
            artist_counts = hour_data['artists'].value_counts()
            if not artist_counts.empty:
                top_artists_by_hour[hour] = artist_counts.index[0]
            else:
                top_artists_by_hour[hour] = 'Unknown'

    hourly_kpis['top_artist'] = hourly_kpis.index.map(
        lambda x: top_artists_by_hour.get(x, 'Unknown')
    )

    track_diversity = {}
    for hour in merged_data['hour'].unique():
        hour_data = merged_data[merged_data['hour'] == hour]
        if not hour_data.empty:
            unique_tracks = hour_data['track_id'].nunique()
            total_plays = len(hour_data)
            if total_plays > 0:
                track_diversity[hour] = unique_tracks / total_plays
            else:
                track_diversity[hour] = 0

    hourly_kpis['track_diversity_index'] = hourly_kpis.index.map(
        lambda x: track_diversity.get(x, 0)
    )

    hourly_kpis = hourly_kpis.reset_index()

    logging.info(f"Transformation complete. Genre KPIs: {len(genre_kpis)} rows, Hourly KPIs: {len(hourly_kpis)} rows")

    # Source file analysis - new KPI based on merged streams data
    if 'source_file' in merged_data.columns:
        source_kpis = merged_data.groupby('source_file').agg(
            total_tracks=('track_id', 'nunique'),
            total_plays=('track_id', 'count'),
            unique_listeners=('user_id', 'nunique')
        )
        
        if 'duration_ms' in merged_data.columns:
            source_kpis['avg_duration'] = merged_data.groupby('source_file')['duration_ms'].mean()
            
        source_kpis = source_kpis.reset_index()
        logging.info(f"Created source file KPIs with {len(source_kpis)} rows")
    else:
        source_kpis = pd.DataFrame(columns=['source_file', 'total_tracks', 'total_plays', 'unique_listeners'])
        logging.info("No source_file column available, created empty source KPIs dataframe")

    return {
        "merged_data": merged_data,
        "genre_kpis": genre_kpis,
        "hourly_kpis": hourly_kpis,
        "source_kpis": source_kpis
    }


@task(task_id="store_transformed_data")
def store_transformed_data(transformed_data: Dict, **context):
    """Partitions data by date and stores in processed folder."""
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    execution_date = context['execution_date'].strftime('%Y/%m/%d')

    logging.info(f"Execution date: {execution_date}")
    logging.info(f"Data keys available: {list(transformed_data.keys())}")
    logging.info(f"S3 bucket: {S3_BUCKET}")
    logging.info(f"S3 processed folder: {S3_PROCESSED_FOLDER}")

    for name, df in transformed_data.items():
        if df.empty:
            logging.warning(f"DataFrame {name} is empty. Skipping save.")
            continue

        logging.info(f"Preparing to save {name} with {len(df)} rows and columns: {list(df.columns)}")

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        s3_key = f"{S3_PROCESSED_FOLDER}{execution_date}/{name}.csv"

        logging.info(f"Saving to S3 path: {s3_key}")

        try:
            s3_hook.load_string(
                string_data=csv_buffer.getvalue(),
                key=s3_key,
                bucket_name=S3_BUCKET,
                replace=True
            )
            logging.info(f"Successfully saved {name} to {s3_key}")

            exists = s3_hook.check_for_key(key=s3_key, bucket_name=S3_BUCKET)
            logging.info(f"File exists check: {exists}")

        except Exception as e:
            logging.error(f"Failed to save {name} to {s3_key}: {str(e)}")


with dag:
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    metadata = extract_metadata()
    validated_data = validate_metadata(metadata)
    new_merge_data = merge_streams_data(validated_data)
    final_merge_data = merge_with_track_id(new_merge_data)
    transformed_data = perform_transformations(final_merge_data)
    store_data = store_transformed_data(transformed_data)

    start >> metadata >> validated_data >> new_merge_data >> final_merge_data >> transformed_data >> store_data >> end