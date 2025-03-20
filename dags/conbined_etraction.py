from airflow import DAG
from airflow.decorators import task
from airflow.operators.dummy import DummyOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import json
import logging
import pandas as pd
import io
from typing import Dict, List, Optional

# Airflow Variables
RDS_CONN_ID = Variable.get("etl_rds_conn_id", "mysqlconnection")
AWS_CONN_ID = Variable.get("etl_aws_conn_id", "userDataEngineering")
S3_BUCKET = Variable.get("etl_s3_bucket", "25dataengineering-streaming-data")
S3_SOURCE_FOLDER = "streams/"
S3_DESTINATION_FOLDER = "Raw_data/"
S3_METADATA_FOLDER = "metadata/"

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

dag = DAG(
    'cdc_etl_pipeline',
    default_args=default_args,
    description='CDC Pipeline to extract only new data from S3 streams folder and RDS, then store to S3 raw_data folder',
    schedule_interval="@daily",
    catchup=False,
    tags=['extract', 's3', 'rds', 'streaming', 'cdc'],
    max_active_runs=1
)

# CDC Metadata Management
@task(task_id="get_processed_files_metadata")
def get_processed_files_metadata(**context) -> Dict:
    """Get metadata of already processed files for CDC."""
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    metadata_key = f"{S3_METADATA_FOLDER}processed_files_metadata.json"
    
    try:
        if s3_hook.check_for_key(metadata_key, bucket_name=S3_BUCKET):
            metadata_obj = s3_hook.get_key(key=metadata_key, bucket_name=S3_BUCKET)
            metadata_content = metadata_obj.get()['Body'].read().decode('utf-8')
            return json.loads(metadata_content)
        else:
            logging.info("No metadata file found, creating new one")
            return {"processed_files": {}, "last_updated": datetime.now().isoformat()}
    except Exception as e:
        logging.error(f"Error reading metadata: {str(e)}")
        return {"processed_files": {}, "last_updated": datetime.now().isoformat()}

@task(task_id="update_processed_files_metadata")
def update_processed_files_metadata(metadata: Dict, newly_processed_files: List[Dict], **context) -> Dict:
    """Update metadata with newly processed files."""
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    metadata_key = f"{S3_METADATA_FOLDER}processed_files_metadata.json"
    
    processed_files = metadata.get("processed_files", {})
    for file_info in newly_processed_files:
        source_key = file_info.get("source")
        if source_key:
            processed_files[source_key] = {
                "processed_at": datetime.now().isoformat(),
                "destination": file_info.get("destination"),
                "file_hash": file_info.get("file_hash", "")
            }
    
    updated_metadata = {
        "processed_files": processed_files,
        "last_updated": datetime.now().isoformat()
    }
    
    s3_hook.load_string(
        string_data=json.dumps(updated_metadata, indent=2),
        key=metadata_key,
        bucket_name=S3_BUCKET,
        replace=True
    )
    
    logging.info(f"Updated metadata with {len(newly_processed_files)} new files")
    return updated_metadata

# Get CDC watermarks for database tables
@task(task_id="get_cdc_watermarks")
def get_cdc_watermarks(**context) -> Dict:
    """Get CDC watermarks for database tables."""
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    watermark_key = f"{S3_METADATA_FOLDER}cdc_watermarks.json"
    
    try:
        if s3_hook.check_for_key(watermark_key, bucket_name=S3_BUCKET):
            watermark_obj = s3_hook.get_key(key=watermark_key, bucket_name=S3_BUCKET)
            watermark_content = watermark_obj.get()['Body'].read().decode('utf-8')
            return json.loads(watermark_content)
        else:
            logging.info("No watermarks file found, creating new one")
            return {
                "stream_data": {"last_id": 0, "last_processed": "2000-01-01T00:00:00"},
                "users": {"last_processed": "2000-01-01T00:00:00"},
                "last_updated": datetime.now().isoformat()
            }
    except Exception as e:
        logging.error(f"Error reading watermarks: {str(e)}")
        return {
            "stream_data": {"last_id": 0, "last_processed": "2000-01-01T00:00:00"},
            "users": {"last_processed": "2000-01-01T00:00:00"},
            "last_updated": datetime.now().isoformat()
        }

@task(task_id="update_cdc_watermarks")
def update_cdc_watermarks(watermarks: Dict, stream_stats: Optional[Dict], users_stats: Optional[Dict], **context) -> Dict:
    """Update CDC watermarks after processing."""
    import numpy as np  # Add this import
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    watermark_key = f"{S3_METADATA_FOLDER}cdc_watermarks.json"
    
    updated_watermarks = watermarks.copy()
    if stream_stats and stream_stats.get("max_id"):
        updated_watermarks["stream_data"]["last_id"] = stream_stats.get("max_id")
        updated_watermarks["stream_data"]["last_processed"] = datetime.now().isoformat()
    
    if users_stats:
        updated_watermarks["users"]["last_processed"] = context['execution_date'].isoformat()
    
    updated_watermarks["last_updated"] = datetime.now().isoformat()
    
    # Using direct recursion to handle nested dictionaries
    def convert_numpy_types(obj):
        if isinstance(obj, dict):
            return {k: convert_numpy_types(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert_numpy_types(item) for item in obj]
        elif isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return obj
    
    # Convert any NumPy types to native Python types
    updated_watermarks = convert_numpy_types(updated_watermarks)
    
    s3_hook.load_string(
        string_data=json.dumps(updated_watermarks, indent=2),
        key=watermark_key,
        bucket_name=S3_BUCKET,
        replace=True
    )
    
    logging.info("Updated CDC watermarks")
    return updated_watermarks

# S3 Processing Tasks
@task(task_id="list_new_source_files")
def list_new_source_files(processed_metadata: Dict, **context) -> List[str]:
    """List only new files in the source S3 folder."""
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    exec_date = context['execution_date']
    look_back_days = 7  # Configurable
    source_keys = []
    
    for i in range(look_back_days):
        check_date = (exec_date - timedelta(days=i)).strftime('%Y-%m-%d')
        prefix = f"{S3_SOURCE_FOLDER}{check_date}/"
        keys = s3_hook.list_keys(bucket_name=S3_BUCKET, prefix=prefix)
        if keys:
            source_keys.extend(keys)
    
    if not source_keys:
        logging.warning("No files found in source folders")
        return []
    
    processed_files = processed_metadata.get("processed_files", {})
    new_files = []
    
    for key in source_keys:
        if key not in processed_files:
            new_files.append(key)
        else:
            source_obj = s3_hook.get_key(key=key, bucket_name=S3_BUCKET)
            current_etag = source_obj.e_tag.strip('"')
            if processed_files[key].get("file_hash", "") != current_etag:
                new_files.append(key)
                logging.info(f"File {key} has changed, will reprocess")
    
    logging.info(f"Found {len(new_files)} new files to process out of {len(source_keys)} total files")
    return new_files

@task(task_id="process_new_s3_files")
def process_new_s3_files(source_keys: List[str], **context) -> List[Dict]:
    """Process only new files from source and save to destination."""
    if not source_keys:
        logging.info("No new files to process")
        return []
    
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    processed_files = []
    exec_date = context['execution_date']
    date_str = exec_date.strftime('%Y-%m-%d')
    
    for source_key in source_keys:
        try:
            source_obj = s3_hook.get_key(key=source_key, bucket_name=S3_BUCKET)
            file_hash = source_obj.e_tag.strip('"')
            file_name = source_key.split('/')[-1]
            file_extension = file_name.split('.')[-1].lower()
            
            if file_extension == 'csv':
                file_content = source_obj.get()['Body'].read().decode('utf-8')
                df = pd.read_csv(io.StringIO(file_content))
                df['processing_timestamp'] = datetime.now().isoformat()
                df['source_file'] = source_key
                
                timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
                dest_file_name = f"{date_str}_{timestamp}_{file_name}"
                dest_key = f"{S3_DESTINATION_FOLDER}{dest_file_name}"
                
                with io.StringIO() as csv_buffer:
                    df.to_csv(csv_buffer, index=False)
                    s3_hook.load_string(
                        string_data=csv_buffer.getvalue(),
                        key=dest_key,
                        bucket_name=S3_BUCKET,
                        replace=True
                    )
                
            elif file_extension == 'json':
                file_content = source_obj.get()['Body'].read().decode('utf-8')
                data = json.loads(file_content)
                
                if isinstance(data, list):
                    for item in data:
                        item['processing_timestamp'] = datetime.now().isoformat()
                        item['source_file'] = source_key
                else:
                    data['processing_timestamp'] = datetime.now().isoformat()
                    data['source_file'] = source_key
                
                timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
                dest_file_name = f"{date_str}_{timestamp}_{file_name}"
                dest_key = f"{S3_DESTINATION_FOLDER}{dest_file_name}"
                
                s3_hook.load_string(
                    string_data=json.dumps(data, indent=2),
                    key=dest_key,
                    bucket_name=S3_BUCKET,
                    replace=True
                )
                
            else:
                timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
                dest_file_name = f"{date_str}_{timestamp}_{file_name}"
                dest_key = f"{S3_DESTINATION_FOLDER}{dest_file_name}"
                
                s3_hook.copy_object(
                    source_bucket_name=S3_BUCKET,
                    source_bucket_key=source_key,
                    dest_bucket_name=S3_BUCKET,
                    dest_bucket_key=dest_key
                )
            
            processed_files.append({
                "source": source_key,
                "destination": dest_key,
                "file_type": file_extension,
                "file_hash": file_hash,
                "processing_time": datetime.now().isoformat()
            })
            
            logging.info(f"Successfully processed file {source_key} to {dest_key}")
            
        except Exception as e:
            logging.error(f"Error processing file {source_key}: {str(e)}")
    
    return processed_files

# RDS Extraction Tasks
@task(task_id="extract_new_rds_stream_data", retries=2)
def extract_new_rds_stream_data(watermarks: Dict, **context) -> Optional[Dict]:
    """Extract only new data from the `stream_data` table in RDS using CDC."""
    logging.info("Starting RDS stream data extraction with CDC")
    hook = MySqlHook(mysql_conn_id=RDS_CONN_ID)
    last_id = watermarks.get("stream_data", {}).get("last_id", 0)
    
    sql = f"""
        SELECT id, track_id, artists, album_name, track_name, popularity, duration_ms, explicit, 
               danceability, energy, `key`, loudness, mode, speechiness, acousticness, 
               instrumentalness, liveness, valence, tempo, time_signature, track_genre
        FROM stream_data
        WHERE id > {last_id}
        ORDER BY id ASC
        LIMIT 50000
    """
    
    try:
        df = hook.get_pandas_df(sql)
        if df.empty:
            logging.warning("No new stream data found in RDS")
            return None
        
        max_id = df['id'].max()
        exec_date = context['execution_date'].strftime('%Y-%m-%d')
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        s3_key = f"{S3_DESTINATION_FOLDER}{exec_date}_{timestamp}_stream_data.csv"
        
        df['processing_timestamp'] = datetime.now().isoformat()
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        with io.StringIO() as csv_buffer:
            df.to_csv(csv_buffer, index=False)
            s3_hook.load_string(
                string_data=csv_buffer.getvalue(),
                key=s3_key,
                bucket_name=S3_BUCKET,
                replace=True
            )
        
        logging.info(f"New stream data saved to s3://{S3_BUCKET}/{s3_key}")
        
        return {
            "record_count": len(df),
            "min_id": df['id'].min() if not df.empty else None,
            "max_id": max_id if not df.empty else None,
            "columns": list(df.columns),
            "execution_date": exec_date,
            "source": "stream_data",
            "destination": s3_key
        }
    except Exception as e:
        logging.error(f"Error extracting stream data: {str(e)}")
        raise

@task(task_id="extract_new_rds_users_data", retries=2)
def extract_new_rds_users_data(watermarks: Dict, **context) -> Optional[Dict]:
    """Extract only new data from the `users` table in RDS using CDC."""
    logging.info("Starting RDS users data extraction with CDC")
    hook = MySqlHook(mysql_conn_id=RDS_CONN_ID)
    last_processed = watermarks.get("users", {}).get("last_processed", "2000-01-01T00:00:00")
    last_processed_date = last_processed.split('T')[0] if 'T' in last_processed else last_processed
    
    exec_date = context['execution_date']
    exec_date_str = exec_date.strftime('%Y-%m-%d')

    sql = f"""
        SELECT user_id, user_name, user_age, user_country, created_at
        FROM users
        WHERE created_at > '{last_processed_date}' 
        AND created_at <= '{exec_date_str}'
        ORDER BY created_at ASC
    """
    
    try:
        df = hook.get_pandas_df(sql)
        if df.empty:
            logging.warning("No new user data found in RDS")
            return None
        
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        s3_key = f"{S3_DESTINATION_FOLDER}{exec_date_str}_{timestamp}_users_data.csv"
        
        df['processing_timestamp'] = datetime.now().isoformat()
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        with io.StringIO() as csv_buffer:
            df.to_csv(csv_buffer, index=False)
            s3_hook.load_string(
                string_data=csv_buffer.getvalue(),
                key=s3_key,
                bucket_name=S3_BUCKET,
                replace=True
            )
        
        logging.info(f"New users data saved to s3://{S3_BUCKET}/{s3_key}")
        
        min_created_at = df['created_at'].min().isoformat() if not df.empty else None
        max_created_at = df['created_at'].max().isoformat() if not df.empty else None
        
        return {
            "record_count": len(df),
            "min_created_at": min_created_at,
            "max_created_at": max_created_at,
            "columns": list(df.columns),
            "execution_date": exec_date_str,
            "source": "users",
            "destination": s3_key
        }
    except Exception as e:
        logging.error(f"Error extracting users data: {str(e)}")
        raise

@task(task_id="create_processing_summary")
def create_processing_summary(
    processed_files: List[Dict],
    stream_stats: Optional[Dict],
    users_stats: Optional[Dict],
    updated_metadata: Dict,
    updated_watermarks: Dict,
    **context
) -> str:
    """Create a summary of all CDC processing and save to S3."""
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    
    summary = {
        "dag_id": context["dag"].dag_id,
        "execution_date": context["execution_date"].isoformat(),
        "processing_completed": datetime.now().isoformat(),
        "s3_files": {
            "files_processed": len(processed_files) if processed_files else 0,
            "processed_files": processed_files if processed_files else []
        },
        "rds_extractions": [],
        "cdc_metrics": {
            "total_new_files": len(processed_files) if processed_files else 0,
            "total_new_stream_records": stream_stats.get("record_count", 0) if stream_stats else 0,
            "total_new_user_records": users_stats.get("record_count", 0) if users_stats else 0,
            "metadata_updated": updated_metadata.get("last_updated") if updated_metadata else None,
            "watermarks_updated": updated_watermarks.get("last_updated") if updated_watermarks else None,
        }
    }
    
    if stream_stats:
        summary["rds_extractions"].append(stream_stats)
    if users_stats:
        summary["rds_extractions"].append(users_stats)
    
    timestamp = context['ts_nodash']
    summary_file_name = f"cdc_processing_summary_{timestamp}.json"
    summary_key = f"{S3_DESTINATION_FOLDER}logs/{summary_file_name}"
    
    # Convert NumPy types to native Python types before JSON serialization
    # Use pandas DataFrame to handle the conversion
    summary_df = pd.DataFrame([summary])
    string_data = summary_df.to_json(orient='records')
    summary_data = json.loads(string_data)[0]
    
    s3_hook.load_string(
        string_data=json.dumps(summary_data, indent=2, default=str),
        key=summary_key,
        bucket_name=S3_BUCKET,
        replace=True
    )
    
    logging.info(f"CDC processing summary saved to s3://{S3_BUCKET}/{summary_key}")
    return summary_key

with dag:
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")
    
    # Get CDC metadata and watermarks
    processed_metadata = get_processed_files_metadata()
    cdc_watermarks = get_cdc_watermarks()
    
    # S3 CDC processing branch
    new_source_files = list_new_source_files(processed_metadata)
    processed_files = process_new_s3_files(new_source_files)
    
    # RDS CDC extraction branches
    stream_stats = extract_new_rds_stream_data(cdc_watermarks)
    users_stats = extract_new_rds_users_data(cdc_watermarks)
    
    # Update CDC metadata and watermarks
    updated_metadata = update_processed_files_metadata(processed_metadata, processed_files)
    updated_watermarks = update_cdc_watermarks(cdc_watermarks, stream_stats, users_stats)
    
    # Create summary of all CDC processing
    summary_key = create_processing_summary(
        processed_files, 
        stream_stats, 
        users_stats, 
        updated_metadata, 
        updated_watermarks
    )
    
    # Define task dependencies
    start >> [processed_metadata, cdc_watermarks]
    processed_metadata >> new_source_files >> processed_files >> updated_metadata
    cdc_watermarks >> [stream_stats, users_stats] >> updated_watermarks
    [updated_metadata, updated_watermarks] >> summary_key >> end