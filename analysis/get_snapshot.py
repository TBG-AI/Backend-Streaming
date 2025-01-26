import psycopg2
import json
from pathlib import Path
from typing import Literal
from constants import SNAPSHOTS_DIR

def get_formatted_name(match_id: str) -> str:
    """Map match IDs to their formatted names"""
    mapping = {
        "ceoracydrstgwdj3jeqfm0aac": "AFC Bournemouth-Nottingham Forest-2025-01-25",
        "cfjmtr9xrz3ydur0k879qbjmc": "Brighton & Hove Albion-Everton-2025-01-25",
        "cgrtk6bfvu2ctp1rjs34g2r6c": "Liverpool-Ipswich Town-2025-01-25",
        "ch6opw6zdu0a9z0yopszbd91w": "Manchester City-Chelsea-2025-01-25",
        "chlesutq3dquxwfvv4ba65hjo": "Southampton-Newcastle United-2025-01-25",
        "cif7u6dfjijtksln0bq4fvgus": "Wolverhampton Wanderers-Arsenal-2025-01-25",
        "cf51smte7w3vb85s7wtnll3is": "West Ham United-Aston Villa-2025-01-26"
    }
    return mapping.get(match_id, match_id)

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
    
    # Get formatted name for the file
    formatted_name = get_formatted_name(match_id)
    
    # Save to file with appropriate suffix
    output_file = f'{SNAPSHOTS_DIR}/{formatted_name}.json'
    with open(output_file, 'w') as f:
        json.dump([row[0] for row in results], f, indent=2)
    
    print(f"Exported {table_type} for {formatted_name}")
    
    cur.close()
    conn.close()


if __name__ == "__main__":
    match_ids = [
        # "ceoracydrstgwdj3jeqfm0aac",
        # "cfjmtr9xrz3ydur0k879qbjmc",
        # "cgrtk6bfvu2ctp1rjs34g2r6c",
        # "ch6opw6zdu0a9z0yopszbd91w",
        # "chlesutq3dquxwfvv4ba65hjo",
        # "cif7u6dfjijtksln0bq4fvgus"
        "cf51smte7w3vb85s7wtnll3is"
    ]
    
    # Export snapshots for all matches
    for match_id in match_ids:
        try:
            export_snapshot(match_id, 'domain_events')
            print(f"Exported domain_events for {get_formatted_name(match_id)}")
        except Exception as e:
            print(f"Error exporting domain_events for {match_id}: {e}")

