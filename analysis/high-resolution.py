import json
from typing import Dict
from utils import (
    get_file_path,
    calculate_euclidean_distance,
    should_skip_coordinate_change,
    parse_timestamp,
    collect_distribution_stats,
)
from db_alchemy.type_ids import EVENT_TYPE_IDS


def analyze_distance_distribution(file_path: str) -> Dict:
    """
    Analyze the normal distributions of coordinate changes.
    Returns statistics about 99% confidence intervals per event type.
    """
    with open(file_path, 'r') as f:
        events = json.load(f)
    
    def extract_distance(event, changed_fields, old_fields, original):
        return calculate_euclidean_distance(
            float(old_fields['x']), float(old_fields['y']),
            float(changed_fields['x']), float(changed_fields['y'])
        )
    
    return collect_distribution_stats(
        events,
        value_extractor=extract_distance,
        should_skip=should_skip_coordinate_change
    )


def analyze_duration_distribution(file_path: str) -> Dict:
    """
    Analyze the normal distributions of edit durations.
    Returns statistics about 99% confidence intervals per event type.
    """
    with open(file_path, 'r') as f:
        events = json.load(f)
    
    def extract_duration(event, changed_fields, old_fields, original):
        if 'occurred_on' in event and 'occurred_on' in original:
            return (parse_timestamp(event['occurred_on']) - 
                   parse_timestamp(original['occurred_on'])).total_seconds()
        return None
    
    return collect_distribution_stats(
        events,
        value_extractor=extract_duration,
        should_skip=should_skip_coordinate_change
    )


def print_distribution_analysis(results: Dict, file_name: str, analysis_type: str):
    """Print the distribution analysis results"""
    print(f"\n{analysis_type} Distribution Analysis for {file_name}")
    print("=" * 100)
    
    header = (
        f"{'Event Type':<30}"
        f"{'Count':>8}"
        f"{'μ':>10}"
        f"{'σ':>10}"
        f"{'99% Range':>20}"
    )
    print(header)
    print("-" * 100)
    
    # Sort by sample size
    for event_type, stats in sorted(
        results.items(),
        key=lambda x: x[1]['sample_size'],
        reverse=True
    ):
        name = f"{stats['name']} ({event_type})"
        if len(name) > 29:
            name = name[:26] + "..."
            
        value_range = f"[{stats['interval'][0]:.1f}, {stats['interval'][1]:.1f}]"
        
        row = (
            f"{name:<30}"
            f"{stats['sample_size']:>8}"
            f"{stats['mean']:>10.2f}"
            f"{stats['std']:>10.2f}"
            f"{value_range:>20}"
        )
        print(row)
    
    print("-" * 100)


if __name__ == "__main__":
    match_info = get_file_path()
    
    # Analyze distances
    distance_results = analyze_distance_distribution(match_info.file_path)
    print_distribution_analysis(distance_results, match_info.match_name, "Distance")
    
    # Analyze durations
    duration_results = analyze_duration_distribution(match_info.file_path)
    print_distribution_analysis(duration_results, match_info.match_name, "Duration")
