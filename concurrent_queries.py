import pandas as pd
from datetime import datetime, timedelta
# import matplotlib.pyplot as plt
from collections import defaultdict

def analyze_query_concurrency_from_csv(csv_file_path):
    """
    Analyze query concurrency from CSV with columns: event_time, total_execution_time, count
    
    Args:
        csv_file_path: Path to CSV file
        
    Returns:
        dict: Analysis results including max concurrency and timeline
    """
    # Read the CSV file
    df = pd.read_csv(csv_file_path)
    
    # Validate columns
    required_columns = ['event_time', 'total_execution_time', 'count']
    if not all(col in df.columns for col in required_columns):
        raise ValueError(f"CSV must contain columns: {required_columns}")
    
    # Convert event_time to datetime (handles ISO 8601 format like 2025-06-21T23:53:08Z)
    df['event_time'] = pd.to_datetime(df['event_time'], utc=True)
    
    # Calculate end times (assuming total_execution_time is in seconds)
    df['end_time'] = df['event_time'] + pd.to_timedelta(df['total_execution_time'], unit='s')
    
    # Create events list considering the count column
    events = []
    
    for _, row in df.iterrows():
        # Each row can represent multiple queries (count column)
        query_count = int(row['count'])
        start_time = row['event_time']
        end_time = row['end_time']
        
        # Add start and end events for all queries in this row
        events.append((start_time, query_count))    # Start: add count queries
        events.append((end_time, -query_count))     # End: remove count queries
    
    # Sort events by timestamp
    events.sort(key=lambda x: x[0])
    
    # Calculate concurrency over time
    current_concurrent = 0
    max_concurrent = 0
    max_concurrent_time = None
    timeline = []
    
    for timestamp, change in events:
        current_concurrent += change
        timeline.append({
            'timestamp': timestamp,
            'concurrent_queries': current_concurrent
        })
        
        if current_concurrent > max_concurrent:
            max_concurrent = current_concurrent
            max_concurrent_time = timestamp
    
    # Create timeline DataFrame
    timeline_df = pd.DataFrame(timeline)
    
    # Calculate statistics
    total_queries = df['count'].sum()
    avg_execution_time = (df['total_execution_time'] * df['count']).sum() / total_queries
    
    # Calculate average concurrency (weighted by time duration)
    if len(timeline_df) > 1:
        timeline_df['duration'] = timeline_df['timestamp'].diff().shift(-1)
        timeline_df = timeline_df[:-1]  # Remove last row (no duration)
        timeline_df['duration_seconds'] = timeline_df['duration'].dt.total_seconds()
        
        total_duration = timeline_df['duration_seconds'].sum()
        weighted_concurrency = (timeline_df['concurrent_queries'] * timeline_df['duration_seconds']).sum()
        avg_concurrent = weighted_concurrency / total_duration if total_duration > 0 else 0
    else:
        avg_concurrent = 0
    
    return {
        'max_concurrent': max_concurrent,
        'max_concurrent_time': max_concurrent_time,
        'total_queries': total_queries,
        'avg_concurrent': avg_concurrent,
        'avg_execution_time_seconds': avg_execution_time,
        'timeline': timeline_df,
        'raw_data': df
    }

def plot_concurrency_timeline(results, figsize=(15, 8)):
    """
    Plot the concurrency timeline from analysis results
    
    Args:
        results: Results dictionary from analyze_query_concurrency_from_csv()
        figsize: Figure size tuple
    """
    timeline = results['timeline']
    
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=figsize, height_ratios=[3, 1])
    
    # Main concurrency plot
    ax1.plot(timeline['timestamp'], timeline['concurrent_queries'], 
             linewidth=2, color='blue', marker='o', markersize=3)
    ax1.set_title(f'Query Concurrency Over Time\nMax Concurrent: {results["max_concurrent"]} queries', 
                  fontsize=14)
    ax1.set_ylabel('Number of Concurrent Queries')
    ax1.grid(True, alpha=0.3)
    ax1.axhline(y=results['max_concurrent'], color='red', linestyle='--', alpha=0.7, 
                label=f'Max: {results["max_concurrent"]}')
    ax1.legend()
    
    # Query starts histogram
    raw_data = results['raw_data']
    ax2.hist(raw_data['event_time'], bins=50, alpha=0.7, color='green', 
             weights=raw_data['count'])
    ax2.set_title('Query Start Times Distribution')
    ax2.set_xlabel('Time')
    ax2.set_ylabel('Number of Queries Started')
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.xticks(rotation=45)
    plt.show()

def get_concurrency_at_time(results, target_time):
    """
    Get the number of concurrent queries at a specific time
    
    Args:
        results: Results from analyze_query_concurrency_from_csv()
        target_time: Time to check (string or datetime)
    
    Returns:
        int: Number of concurrent queries at that time
    """
    if isinstance(target_time, str):
        target_time = pd.to_datetime(target_time)
    
    raw_data = results['raw_data']
    concurrent_count = 0
    
    for _, row in raw_data.iterrows():
        if row['event_time'] <= target_time < row['end_time']:
            concurrent_count += row['count']
    
    return concurrent_count

def get_detailed_summary(results):
    """
    Print a detailed summary of the concurrency analysis
    
    Args:
        results: Results from analyze_query_concurrency_from_csv()
    """
    print("=" * 60)
    print("QUERY CONCURRENCY ANALYSIS SUMMARY")
    print("=" * 60)
    print(f"Total Queries Analyzed: {results['total_queries']:,}")
    print(f"Maximum Concurrent Queries: {results['max_concurrent']}")
    print(f"Peak Concurrency Time: {results['max_concurrent_time']}")
    print(f"Average Concurrent Queries: {results['avg_concurrent']:.2f}")
    print(f"Average Execution Time: {results['avg_execution_time_seconds']:.1f} seconds")
    
    # Time period analysis
    raw_data = results['raw_data']
    start_period = raw_data['event_time'].min()
    end_period = raw_data['end_time'].max()
    total_period = (end_period - start_period).total_seconds()
    
    print(f"\nTime Period Analyzed:")
    print(f"  Start: {start_period}")
    print(f"  End: {end_period}")
    print(f"  Duration: {total_period/3600:.1f} hours")
    
    # Concurrency distribution
    timeline = results['timeline']
    if len(timeline) > 0:
        print(f"\nConcurrency Distribution:")
        print(f"  Min Concurrent: {timeline['concurrent_queries'].min()}")
        print(f"  Max Concurrent: {timeline['concurrent_queries'].max()}")
        print(f"  Median Concurrent: {timeline['concurrent_queries'].median():.1f}")
    
    print("=" * 60)

# Quick function for just getting max concurrency:
def quick_max_concurrency(csv_file_path):
    """
    Quick function to get just the maximum concurrency from CSV
    
    Args:
        csv_file_path: Path to CSV file
        
    Returns:
        int: Maximum number of concurrent queries
    """
    df = pd.read_csv(csv_file_path)
    df['event_time'] = pd.to_datetime(df['event_time'], utc=True)
    df['end_time'] = df['event_time'] + pd.to_timedelta(df['total_execution_time'], unit='ms')
    
    events = []
    for _, row in df.iterrows():
        # events.append((row['event_time'], row['count']))
        events.append((row['event_time'], 1))
        # events.append((row['end_time'], -row['count']))
        events.append((row['end_time'], -1))
    
    events.sort()
    
    current = 0
    max_concurrent = 0
    
    for timestamp, change in events:
        current += change
        max_concurrent = max(max_concurrent, current)
    
    return max_concurrent

# Example for just getting the max:

max_concurrent = quick_max_concurrency('queries.csv')
print(f"Maximum concurrent queries: {max_concurrent}")
