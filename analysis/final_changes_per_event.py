import json
import os
import matplotlib.pyplot as plt
from collections import defaultdict
from typing import List, Dict, Tuple
from utils import (
    collect_event_histories,
    calculate_euclidean_distance,
    should_skip_coordinate_change,
    parse_timestamp,
    EventHistory
)
from db_alchemy.type_ids import EVENT_TYPE_IDS
from constants import SNAPSHOTS_DIR


def analyze_final_changes(file_path: str) -> Dict[int, Tuple[List[float], List[float]]]:
    """
    Analyze the final state of each edited event compared to its original state.
    Returns dict of event_type -> (distances, durations) for valid coordinate changes.
    """
    with open(file_path, 'r') as f:
        events = json.load(f)
    
    histories = collect_event_histories(events)
    
    # Track distances and durations per event type
    event_stats = defaultdict(lambda: ([], []))  # (distances, durations)
    
    for history in histories:
        if not history.edits:
            continue
            
        event_type = history.original_event['payload']['type_id']
        final_edit = history.edits[-1]
        
        # Check if this is a valid coordinate change
        changed_fields = final_edit.changed_fields
        old_fields = {
            'x': history.original_event['payload']['x'],
            'y': history.original_event['payload']['y']
        }
        
        if not should_skip_coordinate_change(changed_fields, old_fields, history.original_event):
            # Calculate final distance from original position
            distance = calculate_euclidean_distance(
                float(old_fields['x']), float(old_fields['y']),
                float(changed_fields['x']), float(changed_fields['y'])
            )
            
            # Calculate total duration
            duration = final_edit.time_diff * 60  # Convert to seconds
            
            # Add to event type stats
            distances, durations = event_stats[event_type]
            distances.append(distance)
            durations.append(duration)
    
    return event_stats


def combine_game_stats(all_stats: List[Dict[int, Tuple[List[float], List[float]]]]) -> Dict[int, Tuple[List[float], List[float]]]:
    """Combine statistics from multiple games"""
    combined_stats = defaultdict(lambda: ([], []))
    
    for game_stats in all_stats:
        for event_type, (distances, durations) in game_stats.items():
            combined_distances, combined_durations = combined_stats[event_type]
            combined_distances.extend(distances)
            combined_durations.extend(durations)
    
    return combined_stats


def plot_event_histograms(event_stats: Dict[int, Tuple[List[float], List[float]]], title: str):
    """Plot histograms for distances and durations per event type"""
    # Sort event types by number of samples
    sorted_events = sorted(
        event_stats.items(),
        key=lambda x: len(x[1][0]),
        reverse=True
    )
    
    print(f"\nPer-Event Type Statistics (Aggregated across all games):")
    print("=" * 120)
    print(f"{'Event Type':<30} {'Count':>8} {'Avg Dist':>10} {'Max Dist':>10} "
          f"{'Avg Dur':>12} {'Max Dur':>12}")
    print("-" * 120)
    
    for event_type, (distances, durations) in sorted_events:
        if not distances:  # Skip if no samples
            continue
            
        event_name = f"{EVENT_TYPE_IDS[event_type]} ({event_type})"
        if len(event_name) > 29:
            event_name = event_name[:26] + "..."
            
        avg_dist = sum(distances) / len(distances)
        max_dist = max(distances)
        avg_dur = sum(durations) / len(durations)
        max_dur = max(durations)
        
        print(f"{event_name:<30} {len(distances):>8} {avg_dist:>10.2f} {max_dist:>10.2f} "
              f"{avg_dur:>12.1f} {max_dur:>12.1f}")
        
        # Create subplot for this event type
        plt.figure(figsize=(15, 6))
        
        # Distance histogram
        plt.subplot(1, 2, 1)
        plt.hist(distances, bins=min(30, len(distances)), edgecolor='black')
        plt.title(f'Distance Changes - {event_name}')
        plt.xlabel('Euclidean Distance (meters)')
        plt.ylabel('Count')
        
        # Duration histogram
        plt.subplot(1, 2, 2)
        plt.hist(durations, bins=min(30, len(durations)), edgecolor='black')
        plt.title(f'Edit Durations - {event_name}')
        plt.xlabel('Duration (seconds)')
        plt.ylabel('Count')
        
        plt.suptitle(f'Final Edit Analysis - {event_name}\n{title}')
        plt.tight_layout()
        
        # Save the plot
        plt.savefig(f'analysis/plots/{event_type}.png')
        plt.close()
    
    print("-" * 120)


if __name__ == "__main__":
    # Collect stats from all games
    all_game_stats = []
    game_count = 0
    
    for filename in os.listdir(SNAPSHOTS_DIR):
        if filename.endswith('.json'):
            file_path = os.path.join(SNAPSHOTS_DIR, filename)
            try:
                game_stats = analyze_final_changes(file_path)
                all_game_stats.append(game_stats)
                game_count += 1
                print(f"Processed: {filename}")
            except Exception as e:
                print(f"Error processing {filename}: {e}")
    
    # Combine all stats
    combined_stats = combine_game_stats(all_game_stats)
    
    # Plot combined statistics
    plot_event_histograms(combined_stats, f"All Games (n={game_count})")