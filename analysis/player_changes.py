import json
from typing import Dict, List
from utils import collect_event_histories, get_file_path
from db_alchemy.type_ids import EVENT_TYPE_IDS


def analyze_player_changes(file_path: str):
    """
    Analyze events where player names were changed.
    Prints detailed history of player name changes.
    """
    # Load events from file first
    with open(file_path, 'r') as f:
        events = json.load(f)
    
    # Then collect histories
    histories = collect_event_histories(events)
    
    # Filter histories that have player name changes
    player_changes = []
    for history in histories:
        name_changes = [
            edit for edit in history.edits 
            if 'player_name' in edit.changed_fields
        ]
        if name_changes:
            player_changes.append((history, name_changes))
    
    # Sort by event type
    player_changes.sort(key=lambda x: x[0].original_event['payload']['type_id'])
    
    # Print changes
    print("\nPlayer Name Changes")
    print("=" * 100)
    
    for history, changes in player_changes:
        orig_payload = history.original_event['payload']
        event_type = orig_payload['type_id']
        
        print(f"\nFeed Event ID: {history.feed_event_id} ({len(changes)} edits)")
        print("=" * 80)
        print(f"Original Event ({history.original_event['occurred_on']}):")
        print(f"  Type: {EVENT_TYPE_IDS[event_type]} (ID: {event_type})")
        print(f"  Match Time: {orig_payload['time_min']}:{orig_payload['time_sec']:02d}")
        print(f"  Player: {orig_payload['player_name']}")
        
        print("\nEdits:")
        for edit in changes:
            print(f"\n  {edit.time_diff:.1f} minutes later:")
            print(f"    {edit.old_fields['player_name']} → {edit.changed_fields['player_name']}")
        
        print("-" * 100)


def print_player_changes(stats: Dict, file_name: str):
    """Print statistics about player name changes"""
    print(f"\nPlayer Name Changes for {file_name}")
    print("=" * 100)
    
    # Print per-event type statistics
    print("Event Type                    Total Changes  Changes")
    print("-" * 100)
    
    # Sort by number of changes
    for event_type, stat in sorted(
        stats.items(), 
        key=lambda x: x[1]['total_changes'], 
        reverse=True
    ):
        name = f"{stat['name']} ({event_type})"
        if len(name) > 25:
            name = name[:22] + "..."
        else:
            name = name.ljust(25)
            
        total = str(stat['total_changes']).ljust(14)
        
        print(f"{name}{total}", end="")
        
        # Print the first change as an example
        if stat['changes']:
            first_change = stat['changes'][0]
            print(f"{first_change['old_name']} → {first_change['new_name']}")
            # Print remaining changes indented
            for change in stat['changes'][1:]:
                print(f"{' ' * 39}{change['old_name']} → {change['new_name']}")
        else:
            print()
    
    print("-" * 100)


if __name__ == "__main__":
    match_info = get_file_path()
    analyze_player_changes(match_info.file_path)
