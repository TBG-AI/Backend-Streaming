from backend_streaming.providers.opta.infra.api import get_tournament_schedule
from backend_streaming.providers.opta.constants import EPL_TOURNAMENT_ID
import json
from typing import Dict, List, NamedTuple
from datetime import datetime
from statistics import mean


class EventEdit(NamedTuple):
    """Represents a single edit to an event"""
    time_diff: float
    changed_fields: Dict
    old_fields: Dict


class EventHistory(NamedTuple):
    """Represents the complete history of an event"""
    feed_event_id: int
    original_event: Dict
    edits: List[EventEdit]


def parse_timestamp(ts: str) -> datetime:
    """Parse timestamp with variable precision microseconds and handle 'Z' timezone"""
    ts = ts.replace('Z', '')
    if '.' in ts:
        main, micro = ts.split('.')
        micro = f"{micro:0<6}"
        ts = f"{main}.{micro}"
    return datetime.fromisoformat(ts)


def get_time_diff(original_time: str, edit_time: str) -> float:
    """Calculate time difference between original event and edit in minutes"""
    orig = parse_timestamp(original_time)
    edit = parse_timestamp(edit_time)
    return (edit - orig).total_seconds() / 60


def has_meaningful_changes(changed_fields: set) -> bool:
    """Check if the edit contains meaningful changes, excluding metadata and qualifiers"""
    metadata_fields = {'last_modified', 'time_stamp'}
    qualifier_fields = {'qualifiers'}
    
    # Remove metadata and qualifier fields
    meaningful_fields = changed_fields - metadata_fields - qualifier_fields
    
    # Return True only if there are remaining fields that changed
    return len(meaningful_fields) > 0


def collect_event_histories(events: List[Dict]) -> List[EventHistory]:
    """Collect all events that have edits and their histories"""
    edited_events: Dict[int, Dict] = {}
    
    # First pass: find meaningful edits
    for event in events:
        if event['event_type'] == 'EventEdited':
            changed_fields = set(event['payload']['changed_fields'].keys())
            if not has_meaningful_changes(changed_fields):
                continue
                
            feed_event_id = event['payload']['feed_event_id']
            if feed_event_id not in edited_events:
                edited_events[feed_event_id] = {
                    'original': None,
                    'edits': []
                }
            
            edited_events[feed_event_id]['edits'].append(EventEdit(
                time_diff=0,  # Will be calculated once we have the original event
                changed_fields=event['payload']['changed_fields'],
                old_fields=event['payload']['old_fields']
            ))
    
    # Second pass: find original events and calculate time differences
    histories = []
    for event in events:
        if event['event_type'] == 'GlobalEventAdded':
            feed_event_id = event['payload']['feed_event_id']
            if feed_event_id in edited_events:
                edits = []
                for edit_event in [e for e in events if e['event_type'] == 'EventEdited' 
                                 and e['payload']['feed_event_id'] == feed_event_id
                                 and has_meaningful_changes(set(e['payload']['changed_fields'].keys()))]:  # Add this filter
                    time_diff = get_time_diff(
                        event['occurred_on'],
                        edit_event['occurred_on']
                    )
                    edits.append(EventEdit(
                        time_diff=time_diff,
                        changed_fields=edit_event['payload']['changed_fields'],
                        old_fields=edit_event['payload']['old_fields']
                    ))
                
                histories.append(EventHistory(
                    feed_event_id=feed_event_id,
                    original_event=event,
                    edits=sorted(edits, key=lambda x: x.time_diff)
                ))
    
    return histories


async def create_match_id_mapping(match_ids: list[str]) -> dict[str, str]:
    """
    Creates a mapping of match_id -> 'hometeam-awayteam-date' format
    
    Args:
        match_ids: List of match IDs to map
        
    Returns:
        Dictionary mapping match_id to formatted string
    """
    mapping = {}
    
    for match_id in match_ids:
        try:
            # Get match details from schedule
            schedule = await get_tournament_schedule(EPL_TOURNAMENT_ID)
            
            # Search through schedule for matching ID
            for match_date in schedule.get("matchDate", []):
                for match in match_date.get("match", []):
                    if match["id"] == match_id:
                        # Format: "HomeTeam-AwayTeam-YYYY-MM-DD"
                        formatted_string = (
                            f"{match['homeContestantName']}-"
                            f"{match['awayContestantName']}-"
                            f"{match['date'].replace('Z', '')}"
                        )
                        mapping[match_id] = formatted_string
                        break
                        
        except Exception as e:
            print(f"Error processing match {match_id}: {e}")
            mapping[match_id] = "ERROR"
            
    return mapping


if __name__ == "__main__":
    import asyncio
    
    async def main():
        match_ids = [
            "c0i4chcg41suds6581fj8k7bo",
            "cbggpny9iygsfce7xf6wycb9w",
            "cdvojt8rvxgk077kd9bvyj3f8",
            "ceoracydrstgwdj3jeqfm0aac",
            "cfjmtr9xrz3ydur0k879qbjmc",
            "cgrtk6bfvu2ctp1rjs34g2r6c",
            "ch6opw6zdu0a9z0yopszbd91w",
            "chlesutq3dquxwfvv4ba65hjo",
            "cif7u6dfjijtksln0bq4fvgus",
            'cfy32fjgh4kbey9otbghjfpjo',
        ]
        
        mapping = await create_match_id_mapping(match_ids)
        
        # Print results
        print("\nMatch ID Mappings:")
        for match_id, formatted_name in mapping.items():
            print(f"{match_id}: {formatted_name}")
    
    asyncio.run(main())