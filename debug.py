import json
from pathlib import Path
from typing import List, Dict, Tuple
from src.backend_streaming.providers.opta.domain.types.sport_event_types import EVENT_TYPE_IDS
from datetime import datetime
from collections import defaultdict
from statistics import mean, median, stdev
import matplotlib.pyplot as plt
import numpy as np

def parse_datetime(dt_string: str) -> datetime:
    """Helper function to parse datetime strings that might have varying precision"""
    try:
        return datetime.fromisoformat(dt_string)
    except ValueError:
        # If the string has too many or too few decimal places, normalize it
        # Remove 'Z' or timezone info if present
        dt_string = dt_string.replace('Z', '')
        # Split into main part and microseconds
        main_part, *microseconds = dt_string.split('.')
        if microseconds:
            # Pad or truncate microseconds to 6 digits
            microseconds = f"{microseconds[0][:6]:0<6}"
            dt_string = f"{main_part}.{microseconds}"
        return datetime.fromisoformat(dt_string)

def find_type_changes(events: List[Dict], match_projection: List[Dict]) -> Dict[int, Tuple[str, str, int, int, int, int, datetime, datetime]]:
    """
    Find all EventTypeChanged entries and their original GlobalEventAdded entries.
    Track both timestamps to see the "inconsistency window".
    
    Args:
        events: List of domain event dictionaries
        match_projection: List of match projection event dictionaries
        
    Returns:
        Dictionary mapping feed_event_id to tuple of:
        (old_type_name, new_type_name, old_type_id, new_type_id, time_min, time_sec, added_timestamp, changed_timestamp)
    """
    # Create lookup for match projection events
    projection_lookup = {event['event_id']: event for event in match_projection}
    
    # First, find all GlobalEventAdded timestamps
    added_timestamps = {}
    for event in events:
        if event['event_type'] == 'GlobalEventAdded':
            feed_event_id = event['payload']['feed_event_id']
            added_timestamps[feed_event_id] = parse_datetime(event['occurred_on'])
    
    type_changes = {}
    
    for event in events:
        if event['event_type'] == 'EventTypeChanged':
            payload = event['payload']
            old_type = payload['old_type_id']
            new_type = payload['new_type_id']
            
            # Only track actual changes
            if old_type != new_type:
                feed_event_id = payload['feed_event_id']
                proj_event = projection_lookup.get(feed_event_id)
                added_time = added_timestamps.get(feed_event_id)
                changed_time = parse_datetime(event['occurred_on'])
                
                if proj_event and added_time:
                    type_changes[feed_event_id] = (
                        EVENT_TYPE_IDS[old_type],
                        EVENT_TYPE_IDS[new_type],
                        old_type,
                        new_type,
                        proj_event['time_min'],
                        proj_event['time_sec'],
                        added_time,
                        changed_time
                    )
    
    return type_changes

def analyze_event_stability(events: List[Dict], match_projection: List[Dict]) -> Dict:
    """
    Analyze when events become stable by tracking:
    1. Which event types are likely to change
    2. How long after creation they typically change
    3. Whether they change multiple times
    4. Ratio of changes to total events of each type
    """
    # Create lookup for match projection events
    projection_lookup = {event['event_id']: event for event in match_projection}
    
    # Track original event types and their changes
    event_changes = defaultdict(list)
    event_history = defaultdict(list)
    event_counts = defaultdict(int)  # Track total events of each type
    
    # First, find all GlobalEventAdded timestamps
    for event in events:
        if event['event_type'] == 'GlobalEventAdded':
            feed_event_id = event['payload']['feed_event_id']
            original_type = event['payload']['type_id']
            added_time = parse_datetime(event['occurred_on'])
            event_counts[original_type] += 1
            event_history[feed_event_id].append({
                'type_id': original_type,
                'timestamp': added_time,
                'action': 'added'
            })
    
    # Track all type changes
    for event in events:
        if event['event_type'] == 'EventTypeChanged':
            feed_event_id = event['payload']['feed_event_id']
            old_type = event['payload']['old_type_id']
            new_type = event['payload']['new_type_id']
            if old_type != new_type:
                changed_time = parse_datetime(event['occurred_on'])
                original_time = event_history[feed_event_id][0]['timestamp']
                event_changes[old_type].append((changed_time - original_time).total_seconds())
                event_history[feed_event_id].append({
                    'type_id': new_type,
                    'timestamp': changed_time,
                    'action': 'changed'
                })
    
    # Analyze stability windows
    stability_analysis = {}
    for type_id in set(event_counts.keys()) | set(event_changes.keys()):
        total_events = event_counts[type_id]
        changes = event_changes[type_id]
        stats = {
            'event_type': EVENT_TYPE_IDS[type_id],
            'total_events': total_events,
            'changed_events': len(changes),
            'change_rate': f"{(len(changes) / total_events * 100):.1f}%" if total_events > 0 else "N/A"
        }
        
        if changes:
            stats.update({
                'min_window': min(changes),
                'max_window': max(changes),
                'avg_window': mean(changes),
                'median_window': median(changes),
                'std_dev': stdev(changes) if len(changes) > 1 else 0
            })
        
        stability_analysis[type_id] = stats
    
    return stability_analysis

def plot_change_analysis(analysis: Dict, match_id: str):
    """
    Create two stacked plots:
    1. (Top) Change rates with total occurrences
    2. (Bottom) Window statistics for events that change
    
    Args:
        analysis: Dictionary containing the stability analysis
        match_id: ID of the match for the title
    """
    # Prepare data
    event_types = []
    change_rates = []
    total_events = []
    avg_windows = []
    std_windows = []
    
    # Only include event types that actually occurred
    for type_id, stats in analysis.items():
        if stats['total_events'] > 0:
            event_types.append(type_id)
            rate = (stats['changed_events'] / stats['total_events'] * 100)
            change_rates.append(rate)
            total_events.append(stats['total_events'])
            
            # Add window statistics if events changed
            if stats['changed_events'] > 0:
                avg_windows.append(stats['avg_window'])
                std_windows.append(stats['std_dev'])
            else:
                avg_windows.append(0)
                std_windows.append(0)
    
    # Sort by change rate
    sorted_indices = np.argsort(change_rates)[::-1]
    event_types = [event_types[i] for i in sorted_indices]
    change_rates = [change_rates[i] for i in sorted_indices]
    total_events = [total_events[i] for i in sorted_indices]
    avg_windows = [avg_windows[i] for i in sorted_indices]
    std_windows = [std_windows[i] for i in sorted_indices]
    
    # Create figure with two subplots (stacked)
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 12), height_ratios=[1, 1])
    
    # Plot 1 (Top): Change rates
    bars = ax1.bar(range(len(event_types)), change_rates)
    
    # Customize first plot
    # ax1.set_title('Event Type Change Rates', pad=20)
    ax1.set_xlabel('Event Type ID')
    ax1.set_ylabel('Change Rate (%)')
    ax1.set_xticks(range(len(event_types)))
    ax1.set_xticklabels(event_types, rotation=45)
    ax1.grid(True, axis='y', linestyle='--', alpha=0.7)
    
    # Add total events on top of bars
    for i, bar in enumerate(bars):
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height,
                str(total_events[i]),
                ha='center', va='bottom')
    
    # Plot 2 (Bottom): Window statistics
    # Only include events that actually changed
    changed_indices = [i for i, avg in enumerate(avg_windows) if avg > 0]
    changed_types = [event_types[i] for i in changed_indices]
    changed_avgs = [avg_windows[i] for i in changed_indices]
    changed_stds = [std_windows[i] for i in changed_indices]
    
    # Create error bar plot
    ax2.errorbar(range(len(changed_types)), changed_avgs, yerr=changed_stds, 
                fmt='o', capsize=5, capthick=1, elinewidth=1, markersize=8)
    
    # Customize second plot
    ax2.set_title('Inconsistency Window Statistics', pad=20)
    ax2.set_xlabel('Event Type ID')
    ax2.set_ylabel('Average Window (seconds)')
    ax2.set_xticks(range(len(changed_types)))
    ax2.set_xticklabels(changed_types, rotation=45)
    ax2.grid(True, linestyle='--', alpha=0.7)
    
    # Add main title
    fig.suptitle(f'Event Analysis - Match {match_id}', fontsize=14, y=0.95)
    
    # Adjust layout
    plt.tight_layout()
    
    # Save the plot
    plt.savefig(f'event_analysis_{match_id}.png', bbox_inches='tight', dpi=300)
    plt.close()

if __name__ == "__main__":
    # Read both JSON files
    domain_events_path = Path("src/backend_streaming/static/cbggpny9iygsfce7xf6wycb9w_domain_events.json")
    match_projection_path = Path("src/backend_streaming/static/cbggpny9iygsfce7xf6wycb9w_match_projection.json")
    
    with open(domain_events_path) as f:
        events = json.load(f)
    with open(match_projection_path) as f:
        match_projection = json.load(f)

    # Find type changes
    changes = find_type_changes(events, match_projection)
    # Print results
    print(f"Found {len(changes)} events with type changes:")
    for feed_event_id, (old_type, new_type, old_id, new_id, time_min, time_sec, added_time, changed_time) in changes.items():
        time_diff = changed_time - added_time
        print(f"{feed_event_id} ({time_min}:{time_sec:02d}): {old_type} ({old_id}) -> {new_type} ({new_id})")
        print(f"  Added: {added_time.strftime('%H:%M:%S.%f')}")
        print(f"  Changed: {changed_time.strftime('%H:%M:%S.%f')}")
        print(f"  Time difference: {time_diff.total_seconds():.3f} seconds")
        print()
    
    # # Analyze stability
    # analysis = analyze_event_stability(events, match_projection)
    
    # # Print results sorted by change rate (highest to lowest)
    # print("Event Type Stability Analysis (sorted by change rate):")
    # print("---------------------------------------------------")
    
    # # Calculate change rates for sorting
    # sorted_types = sorted(
    #     analysis.items(),
    #     key=lambda x: (
    #         x[1]['changed_events'] / x[1]['total_events'] if x[1]['total_events'] > 0 else 0,
    #         x[1]['total_events']  # Secondary sort by total events
    #     ),
    #     reverse=True
    # )
    
    # for type_id, stats in sorted_types:
    #     if stats['total_events'] > 0:  # Only show types that actually occurred
    #         print(f"\n{stats['event_type']} (ID: {type_id}):")
    #         print(f"  Total events: {stats['total_events']}")
    #         print(f"  Changed events: {stats['changed_events']}")
    #         print(f"  Change rate: {stats['change_rate']}")
    #         if stats['changed_events'] > 0:
    #             print(f"  Window range: {stats['min_window']:.1f}s - {stats['max_window']:.1f}s")
    #             print(f"  Average window: {stats['avg_window']:.1f}s")
    #             print(f"  Median window: {stats['median_window']:.1f}s")
    #             print(f"  Standard deviation: {stats['std_dev']:.1f}s")

    # # Create visualization
    # match_id = "cbggpny9iygsfce7xf6wycb9w"
    # plot_change_analysis(analysis, match_id)