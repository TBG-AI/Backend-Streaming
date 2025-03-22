from typing import Dict, List, NamedTuple, Tuple, Callable, Optional
from datetime import datetime
from statistics import mean, stdev
from math import sqrt
from constants import SNAPSHOTS_DIR
from dataclasses import dataclass
from scipy.stats import norm
from db_alchemy.type_ids import EVENT_TYPE_IDS

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

@dataclass
class MatchInfo:
    match_name: str
    file_path: str



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


def calculate_euclidean_distance(old_x: float, old_y: float, new_x: float, new_y: float) -> float:
    """Calculate Euclidean distance between two points"""
    return sqrt((new_x - old_x)**2 + (new_y - old_y)**2)


def find_original_event(events: List[Dict], feed_event_id: int) -> Dict:
    """Find the original GlobalEventAdded event for a given feed_event_id"""
    return next(
        (e for e in events 
         if e['event_type'] == 'GlobalEventAdded' 
         and e['payload']['feed_event_id'] == feed_event_id),
        None
    )

def get_file_path() -> MatchInfo:
    """
    Get the match info including file path and match name.
    This is so that all other analysis scripts can view the same game.
    """
    match_name = "Liverpool-Ipswich Town-2025-01-25"
    file_path = f"{SNAPSHOTS_DIR}/{match_name}.json"
    return MatchInfo(match_name=match_name, file_path=file_path)

def has_valid_coordinates(x: float, y: float) -> bool:
    """
    Check if coordinates are valid (not 0,0).
    """
    return not (x == 0 and y == 0)

def should_skip_coordinate_change(changed_fields: dict, old_fields: dict, original: dict) -> bool:
    """
    Check if this coordinate change should be skipped.
    Returns True if the change should be skipped, False otherwise.
    """
    # Skip if no original event
    if not original:
        return True

    # Skip if no x,y changes
    if not ('x' in changed_fields and 'y' in changed_fields):
        return True
        
    # Skip if either position is (0,0)
    if not (has_valid_coordinates(float(changed_fields['x']), float(changed_fields['y'])) and
           has_valid_coordinates(float(old_fields['x']), float(old_fields['y']))):
        return True
        
    return False

def analyze_distribution(values: List[float], confidence: float = 0.99) -> Tuple[float, float, Tuple[float, float]]:
    """
    Calculate mean, standard deviation, and confidence interval for a list of values.
    Returns (mean, std, (lower_bound, upper_bound))
    """
    if len(values) > 1:
        val_mean = mean(values)
        val_std = stdev(values)
        z_score = norm.ppf((1 + confidence) / 2)  # ~2.576 for 99%
        val_interval = (
            max(0, val_mean - z_score * val_std),  # Can't have negative values
            val_mean + z_score * val_std
        )
        return val_mean, val_std, val_interval
    else:
        max_val = max(values) if values else 0
        return max_val, 0, (0, max_val)


def collect_distribution_stats(events: List[Dict], 
                             value_extractor: Callable[[Dict, Dict, Dict, Dict], Optional[float]],
                             should_skip: Callable[[Dict, Dict, Dict], bool] = lambda *args: False
                             ) -> Dict[int, Dict]:
    """
    Generic function to collect distribution statistics for events.
    
    Args:
        events: List of event dictionaries
        value_extractor: Function that takes (event, changed_fields, old_fields, original) and returns a value to analyze
        should_skip: Optional function that takes (changed_fields, old_fields, original) and returns True if event should be skipped
    
    Returns:
        Dictionary of event type statistics
    """
    event_stats: Dict[int, Dict[str, List]] = {}
    
    for event in events:
        if event['event_type'] == 'EventEdited':
            changed_fields = event['payload']['changed_fields']
            old_fields = event['payload']['old_fields']
            feed_event_id = event['payload']['feed_event_id']
            original = find_original_event(events, feed_event_id)
            
            if not original or should_skip(changed_fields, old_fields, original):
                continue
                
            event_type = original['payload']['type_id']
            
            # Initialize stats for new event type
            if event_type not in event_stats:
                event_stats[event_type] = {
                    'values': [],
                    'name': EVENT_TYPE_IDS[event_type]
                }
            
            value = value_extractor(event, changed_fields, old_fields, original)
            if value is not None:
                event_stats[event_type]['values'].append(value)
    
    # Calculate confidence intervals
    results = {}
    for event_type, stats in event_stats.items():
        values = stats['values']
        mean_val, std_val, interval = analyze_distribution(values)
            
        results[event_type] = {
            'name': stats['name'],
            'sample_size': len(values),
            'mean': mean_val,
            'std': std_val,
            'interval': interval
        }
    
    return results