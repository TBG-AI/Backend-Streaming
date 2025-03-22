import json
import os
import matplotlib.pyplot as plt
from typing import List, Dict, Tuple
from utils import (
    collect_event_histories,
    calculate_euclidean_distance,
    should_skip_coordinate_change,
    parse_timestamp,
    EventHistory
)
from constants import SNAPSHOTS_DIR


def analyze_final_changes(file_path: str) -> Tuple[List[float], List[float]]:
    """
    Analyze the final state of each edited event compared to its original state.
    Returns (distances, durations) for valid coordinate changes.
    """
    with open(file_path, 'r') as f:
        events = json.load(f)
    
    histories = collect_event_histories(events)
    
    distances = []
    durations = []
    
    for history in histories:
        if not history.edits:
            continue
            
        # Get the last edit
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
            distances.append(distance)
            
            # Calculate total duration using time_diff which is in minutes
            duration = final_edit.time_diff * 60  # Convert to seconds
            durations.append(duration)
    
    return distances, durations


def plot_histograms(all_distances: List[float], all_durations: List[float], game_count: int):
    """Plot histograms for distances and durations"""
    plt.figure(figsize=(15, 6))
    
    # Distance histogram
    plt.subplot(1, 2, 1)
    plt.hist(all_distances, bins=50, edgecolor='black')
    plt.title('Distribution of Final Position Changes')
    plt.xlabel('Euclidean Distance (meters)')
    plt.ylabel('Count')
    
    # Duration histogram
    plt.subplot(1, 2, 2)
    plt.hist(all_durations, bins=50, edgecolor='black')
    plt.title('Distribution of Edit Durations')
    plt.xlabel('Duration (seconds)')
    plt.ylabel('Count')
    
    plt.suptitle(f'Final Edit Analysis - All Games (n={game_count})')
    plt.tight_layout()
    
    # Save the plot
    plt.savefig('analysis/plots/all_events.png')
    plt.close()
    
    # Print some basic statistics
    print(f"\nDistance Statistics (meters):")
    print(f"Total samples: {len(all_distances)}")
    print(f"Mean: {sum(all_distances)/len(all_distances):.2f}")
    print(f"Max: {max(all_distances):.2f}")
    print(f"Min: {min(all_distances):.2f}")
    
    print(f"\nDuration Statistics (seconds):")
    print(f"Total samples: {len(all_durations)}")
    print(f"Mean: {sum(all_durations)/len(all_durations):.2f}")
    print(f"Max: {max(all_durations):.2f}")
    print(f"Min: {min(all_durations):.2f}")


if __name__ == "__main__":
    # Collect stats from all games
    all_distances = []
    all_durations = []
    game_count = 0
    
    for filename in os.listdir(SNAPSHOTS_DIR):
        if filename.endswith('.json'):
            file_path = os.path.join(SNAPSHOTS_DIR, filename)
            try:
                distances, durations = analyze_final_changes(file_path)
                all_distances.extend(distances)
                all_durations.extend(durations)
                game_count += 1
                print(f"Processed: {filename}")
            except Exception as e:
                print(f"Error processing {filename}: {e}")
    
    # Plot combined statistics
    plot_histograms(all_distances, all_durations, game_count) 