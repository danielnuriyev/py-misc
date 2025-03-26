import boto3
import pandas as pd
from io import StringIO

def get_s3_client():
    """Initialize and return an S3 client"""
    return boto3.client('s3')

def check_alarmnum_presence(bucket_name, prefix='sftp-data/contact_center/rapid_response/Alarm_Data_Daily_'):
    """
    Check each CSV file for presence of AlarmNum values
    """
    s3_client = get_s3_client()
    files_with_issues = []
    
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    
    for page in pages:
        if 'Contents' not in page:
            continue
            
        for obj in page['Contents']:
            key = obj['Key']
            if not key.endswith('.csv'):
                continue
                
            try:
                # Get the file content
                response = s3_client.get_object(Bucket=bucket_name, Key=key)
                csv_content = response['Body'].read().decode('utf-8')
                
                # Read CSV into pandas
                df = pd.read_csv(StringIO(csv_content))
                
                # Check if AlarmNum column exists
                if 'AlarmNum' not in df.columns:
                    files_with_issues.append({
                        'file': key,
                        'issue': 'AlarmNum column missing'
                    })
                    continue
                
                # Check for missing values in AlarmNum
                missing_count = df['AlarmNum'].isna().sum()
                total_rows = len(df)
                
                if missing_count > 0:
                    files_with_issues.append({
                        'file': key,
                        'issue': f'AlarmNum missing in {missing_count} out of {total_rows} rows'
                    })

                is_numeric = pd.to_numeric(df['AlarmNum'], errors='coerce').notna().all()
                if not is_numeric:
                    # Find non-numeric values
                    non_numeric = df[~pd.to_numeric(df['AlarmNum'], errors='coerce').notna()]['AlarmNum'].tolist()
                    files_with_issues.append({
                        'file': key,
                        'issue': f'AlarmNum contains non-numeric values: {non_numeric}'
                    })
                    
            except Exception as e:
                print(f"Error processing {key}: {str(e)}")
                
    return files_with_issues

def main():
    # Replace with your bucket name
    bucket_name = 'ss-bi-datalake-sftp'
    
    print("Checking files for AlarmNum presence...")
    problem_files = check_alarmnum_presence(bucket_name)
    
    # Print results
    if problem_files:
        print("\nFound files with AlarmNum issues:")
        for file_info in problem_files:
            print(f"File: {file_info['file']}")
            print(f"Issue: {file_info['issue']}")
            print()
    else:
        print("\nAll files have AlarmNum present in all rows!")

if __name__ == "__main__":
    # Configure AWS credentials if not already set up
    # boto3.setup_default_session(
    #     aws_access_key_id='YOUR_ACCESS_KEY',
    #     aws_secret_access_key='YOUR_SECRET_KEY',
    #     region_name='YOUR_REGION'
    # )
    
    main()