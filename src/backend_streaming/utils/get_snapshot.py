import psycopg2
import json
from pathlib import Path
from typing import Literal
from backend_streaming.constants import SNAPSHOTS_DIR 

def export_snapshot(
    match_id: str, 
    table_type: Literal['domain_events', 'match_projection']
):
    """
    Export all rows for a match from either domain_events or match_projection table.
    
    Args:
        match_id: The match ID to export data for
        table_type: Either 'domain_events' or 'match_projection'
    """
    # TODO: for now, we are assuming a few things. ADD PROPER TESTS!
    # 1. ordering of 'in-game' events is consistent with feed order (depends on opta provider)
    # 2. insertion order follows feed order (should be the case since we're looping with single transaction per event)
    conn = psycopg2.connect("dbname=opta_test")
    cur = conn.cursor()
    
    if table_type == 'domain_events':
        cur.execute("""
            SELECT row_to_json(de)
            FROM domain_events de
            WHERE aggregate_id = %s
            ORDER BY occurred_on ASC
        """, (match_id,))
    else:  
        cur.execute("""
            SELECT row_to_json(mp)
            FROM match_projection mp
            WHERE match_id = %s
            ORDER BY time_stamp ASC
        """, (match_id,))
    
    results = cur.fetchall()
    # Ensure snapshots directory exists
    Path(SNAPSHOTS_DIR).mkdir(exist_ok=True)
    
    # Save to file with appropriate suffix
    output_file = f'{SNAPSHOTS_DIR}/{match_id}_{table_type}.json'
    with open(output_file, 'w') as f:
        json.dump([row[0] for row in results], f, indent=2)
    
    cur.close()
    conn.close()


if __name__ == "__main__":
    match_id = 'cbggpny9iygsfce7xf6wycb9w'
    # Export both types of snapshots
    export_snapshot(match_id, 'domain_events')
    export_snapshot(match_id, 'match_projection')

