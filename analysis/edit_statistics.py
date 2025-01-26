import json
from pathlib import Path
from typing import Dict, List, DefaultDict
from statistics import mean
from collections import defaultdict
from utils import parse_timestamp, has_meaningful_changes
from backend_streaming.providers.opta.domain.types.sport_event_types import EVENT_TYPE_IDS

from constants import SNAPSHOTS_DIR


def calculate_statistics(file_path: str) -> tuple[Dict, Dict]:
    """Calculate both overall and per-event type statistics"""
    with open(file_path, 'r') as f:
        events = json.load(f)
    
    # First, count total occurrences of each event type
    total_events: DefaultDict[int, int] = defaultdict(int)
    total_global_events = 0
    for event in events:
        if event['event_type'] == 'GlobalEventAdded':
            event_type = event['payload']['type_id']
            total_events[event_type] += 1
            total_global_events += 1
    
    # Then collect edit information
    edited_events: DefaultDict[int, Dict[int, List[str]]] = defaultdict(lambda: defaultdict(list))
    total_edited_events = set()  # Track unique feed_event_ids
    all_edit_durations = []  # Track all edit durations
    
    for event in events:
        if event['event_type'] == 'EventEdited':
            # Skip if no meaningful changes
            if not has_meaningful_changes(set(event['payload']['changed_fields'].keys())):
                continue
                
            feed_event_id = event['payload']['feed_event_id']
            # Find the original event to get its type
            original = next(
                (e for e in events 
                 if e['event_type'] == 'GlobalEventAdded' 
                 and e['payload']['feed_event_id'] == feed_event_id),
                None
            )
            if original:
                event_type = original['payload']['type_id']
                edited_events[event_type][feed_event_id].append(event['occurred_on'])
                total_edited_events.add(feed_event_id)
    
    # Calculate per-event type statistics
    per_event_stats = {}
    for event_type in total_events.keys():
        edited_count = len(edited_events[event_type])
        total_count = total_events[event_type]
        
        # Calculate average edit duration for events with multiple edits
        durations = []
        for edit_times in edited_events[event_type].values():
            if len(edit_times) > 1:
                first_edit = parse_timestamp(min(edit_times))
                last_edit = parse_timestamp(max(edit_times))
                duration = (last_edit - first_edit).total_seconds() / 60
                durations.append(duration)
                all_edit_durations.append(duration)
        
        per_event_stats[event_type] = {
            'name': EVENT_TYPE_IDS[event_type],
            'type_id': event_type,
            'total_events': total_count,
            'edited_events': edited_count,
            'edit_percentage': (edited_count / total_count * 100) if total_count > 0 else 0,
            'average_duration': mean(durations) if durations else 0
        }
    
    # Calculate overall statistics
    overall_stats = {
        'total_events': total_global_events,
        'edited_events': len(total_edited_events),
        'edit_percentage': (len(total_edited_events) / total_global_events * 100) if total_global_events > 0 else 0,
        'average_duration': mean(all_edit_durations) if all_edit_durations else 0
    }
    
    return overall_stats, per_event_stats


def print_statistics(overall_stats: Dict, per_event_stats: Dict, file_name: str):
    """Print both overall and per-event statistics"""
    print(f"\nStatistics for {file_name}")
    print("=" * 100)
    
    # Print overall statistics
    print("Overall Statistics:")
    print("-" * 100)
    print(f"Total Events: {overall_stats['total_events']}")
    print(f"Events Edited: {overall_stats['edited_events']} ({overall_stats['edit_percentage']:.1f}%)")
    if overall_stats['average_duration'] > 0:
        print(f"Average Edit Duration: {overall_stats['average_duration']:.1f} minutes")
    print("\n")
    
    # Print per-event type statistics
    print("Per-Event Type Statistics:")
    print("-" * 100)
    print("Event Type (ID)                Total Events    Edited Events   Edit %          Avg Edit Duration")
    print("-" * 100)
    
    # Sort by edit percentage
    for event_type, stat in sorted(per_event_stats.items(), key=lambda x: x[1]['edit_percentage'], reverse=True):
        name = f"{stat['name']} ({stat['type_id']})"
        if len(name) > 25:
            name = name[:22] + "..."
        else:
            name = name.ljust(25)
            
        total = str(stat['total_events']).ljust(15)
        edited = str(stat['edited_events']).ljust(15)
        percentage = f"{stat['edit_percentage']:.1f}%".ljust(15)
        duration = f"{stat['average_duration']:.1f} min" if stat['average_duration'] > 0 else "N/A"
        
        print(f"{name}{total}{edited}{percentage}{duration}")
    
    print("-" * 100)


if __name__ == "__main__":
    match_name = 'West Ham United-Aston Villa-2025-01-26.json'
    file_path = f"{SNAPSHOTS_DIR}/{match_name}"
    overall_stats, per_event_stats = calculate_statistics(file_path)
    print_statistics(overall_stats, per_event_stats, match_name) 
