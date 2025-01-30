import json
from pathlib import Path
from typing import Dict, DefaultDict, List
from collections import defaultdict
from statistics import mean, stdev
from utils import (
    parse_timestamp,
    calculate_euclidean_distance,
    find_original_event,
    get_file_path,
    should_skip_coordinate_change
)
from db_alchemy.type_ids import EVENT_TYPE_IDS
from constants import SNAPSHOTS_DIR


def analyze_coordinate_changes(file_path: str) -> tuple[Dict, Dict]:
    """
    Analyze coordinate changes per event type.
    Returns tuple of (overall_stats, per_event_stats)
    """
    with open(file_path, 'r') as f:
        events = json.load(f)
    
    # Track position changes per event type
    position_changes: DefaultDict[int, DefaultDict[int, List[Dict]]] = defaultdict(lambda: defaultdict(list))
    
    # Track overall statistics
    all_distances = []
    all_edit_durations = []
    total_edited_events = set()
    total_position_edits = 0
    
    for event in events:
        if event['event_type'] == 'EventEdited':
            changed_fields = event['payload']['changed_fields']
            old_fields = event['payload']['old_fields']
            feed_event_id = event['payload']['feed_event_id']
            original = find_original_event(events, feed_event_id)
            
            if should_skip_coordinate_change(changed_fields, old_fields, original):
                continue
            
            event_type = original['payload']['type_id']
            distance = calculate_euclidean_distance(
                float(old_fields['x']), float(old_fields['y']),
                float(changed_fields['x']), float(changed_fields['y'])
            )
            
            change_info = {
                'distance': distance,
                'time': parse_timestamp(event['occurred_on']),
                'original_time': parse_timestamp(original['occurred_on']),
                'old_pos': (float(old_fields['x']), float(old_fields['y'])),
                'new_pos': (float(changed_fields['x']), float(changed_fields['y'])),
                'feed_event_id': feed_event_id
            }

            # Track for overall stats
            all_distances.append(distance)
            total_edited_events.add(feed_event_id)
            total_position_edits += 1
            position_changes[event_type][feed_event_id].append(change_info)
    
    # Calculate overall statistics
    overall_stats = {
        'total_events_with_coords': len(total_edited_events),
        'total_position_edits': total_position_edits,
        'avg_edits_per_event': total_position_edits / len(total_edited_events) if total_edited_events else 0,
        'avg_distance': mean(all_distances) if all_distances else 0,
        'max_distance': max(all_distances) if all_distances else 0,
        'min_distance': min(all_distances) if all_distances else 0
    }
    
    # Calculate per-event type statistics
    per_event_stats = {}
    for event_type, events_dict in position_changes.items():
        event_distances = []
        event_durations = []
        event_edit_counts = []
        max_distance = 0
        max_distance_event_id = None
        
        for feed_event_id, changes in events_dict.items():
            # Calculate total distance moved for this event
            total_distance = sum(change['distance'] for change in changes)
            event_distances.append(total_distance)
            
            # Track maximum distance and its event
            if total_distance > max_distance:
                max_distance = total_distance
                max_distance_event_id = feed_event_id
            
            # Count number of position edits
            event_edit_counts.append(len(changes))
            
            # Calculate duration between first and last edit
            if len(changes) > 1:
                first_edit = min(change['time'] for change in changes)
                last_edit = max(change['time'] for change in changes)
                duration = (last_edit - first_edit).total_seconds()
                event_durations.append(duration)
                all_edit_durations.append(duration)
        
        per_event_stats[event_type] = {
            'name': EVENT_TYPE_IDS[event_type],
            'events_with_position_changes': len(events_dict),
            'total_position_edits': sum(event_edit_counts),
            'avg_edits_per_event': mean(event_edit_counts),
            'avg_total_distance': mean(event_distances) if event_distances else 0,
            'std_total_distance': stdev(event_distances) if len(event_distances) > 1 else 0,
            'max_total_distance': max_distance,
            'max_distance_event_id': max_distance_event_id,
            'min_total_distance': min(event_distances) if event_distances else 0,
            'avg_edit_duration_seconds': mean(event_durations) if event_durations else 0,
            'std_edit_duration_seconds': stdev(event_durations) if len(event_durations) > 1 else 0
        }
    
    return overall_stats, per_event_stats


def print_coordinate_statistics(
    overall_stats: Dict, 
    per_event_stats: Dict,
    file_name: str
):
    """Print coordinate change statistics in a formatted table"""
    print(f"\nCoordinate Change Statistics for {file_name}")
    print("=" * 120)
    
    # Print overall statistics
    print("Overall Statistics:")
    print("-" * 120)
    print(f"Total Events with Position Changes: {overall_stats['total_events_with_coords']}")
    print(f"Total Position Edits: {overall_stats['total_position_edits']}")
    print(f"Average Edits per Event: {overall_stats['avg_edits_per_event']:.1f}")
    print(f"Average Distance Change: {overall_stats['avg_distance']:.2f}")
    print(f"Max Distance Change: {overall_stats['max_distance']:.2f}")
    print(f"Min Distance Change: {overall_stats['min_distance']:.2f}")
    print()
    
    # Print per-event type statistics
    print("Per-Event Type Statistics:")
    print("-" * 160)  # Increased width for new column
    header = (
        f"{'Event Type':<30}"
        f"{'Total':>8}"
        f"{'#Edits':>10}"
        f"{'Ratio':>10}"
        f"{'Avg Dist':>12}"
        f"{'Std Dist':>12}"
        f"{'Max Dist':>12}"
        f"{'Max Event ID':>15}"
        f"{'Avg Dur':>12}"
        f"{'Std Dur':>12}"
    )
    print(header)
    print("-" * 160)  # Increased width for new column
    
    for event_type, stat in sorted(
        per_event_stats.items(), 
        key=lambda x: x[1]['events_with_position_changes'], 
        reverse=True
    ):
        name = f"{stat['name']} ({event_type})"
        if len(name) > 29:
            name = name[:26] + "..."
            
        row = (
            f"{name:<30}"
            f"{stat['events_with_position_changes']:>8}"
            f"{stat['total_position_edits']:>10}"
            f"{stat['avg_edits_per_event']:>10.1f}"
            f"{stat['avg_total_distance']:>12.2f}"
            f"{stat['std_total_distance']:>12.2f}"
            f"{stat['max_total_distance']:>12.2f}"
            f"{stat['max_distance_event_id']:>15}"
            f"{stat['avg_edit_duration_seconds']:>11.1f}s"
            f"{stat['std_edit_duration_seconds']:>11.1f}s"
        )
        print(row)
    
    print("-" * 160)  # Increased width for new column


if __name__ == "__main__":
    match_info = get_file_path()
    overall_stats, per_event_stats = analyze_coordinate_changes(match_info.file_path)
    print_coordinate_statistics(
        overall_stats, 
        per_event_stats,
        match_info.match_name
    )