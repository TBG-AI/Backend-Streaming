import json
import os
from datetime import datetime
from collections import defaultdict
from typing import List, Dict
from utils import (
    collect_event_histories,
    parse_timestamp,
    EventHistory
)
from constants import SNAPSHOTS_DIR
from db_alchemy.type_ids import EVENT_TYPE_IDS


def find_last_event_timestamp(events: List[Dict]) -> str:
    """Find the timestamp of the last GlobalEventAdded"""
    last_timestamp = None
    for event in events:
        if event['event_type'] == 'GlobalEventAdded':
            last_timestamp = event['occurred_on']
    return last_timestamp


def format_match_time(match_time: str) -> str:
    """Format match time nicely"""
    if ':' in str(match_time):
        return match_time
    try:
        minutes = int(float(match_time))
        seconds = int((float(match_time) - minutes) * 60)
        return f"{minutes:02d}:{seconds:02d}"
    except:
        return match_time


def print_post_game_histories(file_path: str):
    """Print event histories for edits that occurred after the last GlobalEventAdded, organized by event type"""
    with open(file_path, 'r') as f:
        events = json.load(f)
    
    last_event_time = find_last_event_timestamp(events)
    if not last_event_time:
        return
        
    last_event_dt = parse_timestamp(last_event_time)
    histories = collect_event_histories(events)
    
    # Group histories by event type
    event_type_histories = defaultdict(list)
    
    for history in histories:
        # Filter edits to only those after the last event
        post_game_edits = [
            edit for edit in history.edits 
            if parse_timestamp(history.original_event['occurred_on']) <= last_event_dt
        ]
        
        if not post_game_edits:
            continue
            
        event_type = history.original_event['payload']['type_id']
        event_type_histories[event_type].append((history, post_game_edits))
    
    print(f"\nLast event added at: {last_event_time}")
    print("=" * 100)
    
    # Sort event types by number of edited events
    sorted_event_types = sorted(
        event_type_histories.items(),
        key=lambda x: len(x[1]),
        reverse=True
    )
    
    for event_type, histories_and_edits in sorted_event_types:
        event_name = EVENT_TYPE_IDS[event_type]
        print(f"\n{event_name} (ID: {event_type}) - {len(histories_and_edits)} events edited")
        print("=" * 100)
        
        # Sort histories by number of edits
        sorted_histories = sorted(
            histories_and_edits,
            key=lambda x: len(x[1]),
            reverse=True
        )
        
        for history, post_game_edits in sorted_histories:
            # Print header with feed event ID and edit count
            print(f"\nFeed Event ID: {history.feed_event_id} ({len(post_game_edits)} edits)")
            print("-" * 80)
            
            # Print original event details
            original = history.original_event['payload']
            event_time = format_match_time(original.get('match_time', 'N/A'))
            print(f"Original Event ({history.original_event['occurred_on']}):")
            print(f"  Match Time: {event_time}")
            if 'player_name' in original:
                print(f"  Player: {original['player_name']}")
            if 'x' in original and 'y' in original:
                print(f"  Position: ({original['x']}, {original['y']})")
            print()
            
            # Print edits
            print("Edits:")
            for edit in post_game_edits:
                print(f"\n  {edit.time_diff:.1f} minutes later:")
                for field, new_value in edit.changed_fields.items():
                    old_value = edit.old_fields.get(field, 'N/A')
                    if field == 'match_time':
                        old_value = format_match_time(old_value)
                        new_value = format_match_time(new_value)
                    print(f"    {field}: {old_value} -> {new_value}")
            
            print("-" * 80)
        
        print("\n")


if __name__ == "__main__":
    # Process the most recent game file
    files = [f for f in os.listdir(SNAPSHOTS_DIR) if f.endswith('.json')]
    if not files:
        print("No game files found")
        exit()
        
    latest_file = max(files, key=lambda f: os.path.getmtime(os.path.join(SNAPSHOTS_DIR, f)))
    file_path = os.path.join(SNAPSHOTS_DIR, latest_file)
    
    print(f"Analyzing: {latest_file}")
    print_post_game_histories(file_path)