import json
from typing import Dict, List, Tuple, NamedTuple
from datetime import datetime
from statistics import mean
from pathlib import Path
from backend_streaming.providers.opta.domain.types.sport_event_types import EVENT_TYPE_IDS

from utils import collect_event_histories, EventHistory


def print_event_history(history: EventHistory):
    """Print the history of a single event"""
    orig_payload = history.original_event['payload']
    print(f"Feed Event ID: {history.feed_event_id} ({len(history.edits)} edits)")
    print("=" * 80)
    
    print(f"Original Event ({history.original_event['occurred_on']}):")
    print(f"  Type: {EVENT_TYPE_IDS[orig_payload['type_id']]} (ID: {orig_payload['type_id']})")
    print(f"  Match Time: {orig_payload['time_min']}:{orig_payload['time_sec']:02d}")
    if orig_payload.get('player_name'):
        print(f"  Player: {orig_payload['player_name']}")
    
    print("\nEdits:")
    for edit in history.edits:
        print(f"\n  {edit.time_diff:.1f} minutes later:")
        
        # Handle time changes
        if 'time_min' in edit.changed_fields or 'time_sec' in edit.changed_fields:
            old_min = edit.old_fields.get('time_min', orig_payload['time_min'])
            old_sec = edit.old_fields.get('time_sec', orig_payload['time_sec'])
            new_min = edit.changed_fields.get('time_min', old_min)
            new_sec = edit.changed_fields.get('time_sec', old_sec)
            print(f"    Match Time: {old_min}:{old_sec:02d} -> {new_min}:{new_sec:02d}")
        
        # Handle other changes, excluding qualifiers and metadata
        for field in edit.changed_fields:
            if field not in ['last_modified', 'time_stamp', 'time_min', 'time_sec', 'qualifiers']:
                print(f"    {field}: {edit.old_fields[field]} -> {edit.changed_fields[field]}")
        
        #print("\n" + "-" * 80)


def analyze_event_history(file_path: str):
    """Main analysis function"""
    with open(file_path, 'r') as f:
        events = json.load(f)
    
    histories = collect_event_histories(events)
    # Print detailed histories
    for history in sorted(histories, key=lambda h: len(h.edits), reverse=True):
        print_event_history(history)
        print("\n" + "-" * 80)


if __name__ == "__main__":
    file_path = "analysis/domain_events/Liverpool-Ipswich Town-2025-01-25.json"
    analyze_event_history(file_path)