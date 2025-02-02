import psycopg2
import json
from pathlib import Path
from typing import Literal
from analysis.constants import SNAPSHOTS_DIR, GAME_TO_ID_MAPPING

def get_formatted_name(match_id: str) -> str:
    """Map match IDs to their formatted names"""
    return GAME_TO_ID_MAPPING.get(match_id, match_id)

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
    output_file = f'{SNAPSHOTS_DIR}/{table_type}/{formatted_name}.json'
    with open(output_file, 'w') as f:
        json.dump([row[0] for row in results], f, indent=2)
    
    print(f"Exported {table_type} for {formatted_name}")
    
    cur.close()
    conn.close()

def get_snapshot_type():
    """Get snapshot type from user input"""
    while True:
        print("\nAvailable snapshot types:")
        print("1. domain_events")
        print("2. match_projections")
        choice = input("Choose snapshot type (1/2): ")
        
        if choice == "1":
            return "domain_events"
        elif choice == "2":
            return "match_projections"
        else:
            print("Invalid choice. Please enter 1 or 2.")

if __name__ == "__main__":
    # games on 1/26
    match_ids = [
        "c0i4chcg41suds6581fj8k7bo",
        "cbggpny9iygsfce7xf6wycb9w",
        "cdvojt8rvxgk077kd9bvyj3f8",
        "ceoracydrstgwdj3jeqfm0aac",
        "cf51smte7w3vb85s7wtnll3is",
        "cfjmtr9xrz3ydur0k879qbjmc",
        "cfy32fjgh4kbey9otbghjfpjo",
        "cgd2x2vbz3uxkuerreo4txo9g",
        "cgrtk6bfvu2ctp1rjs34g2r6c",
        "ch6opw6zdu0a9z0yopszbd91w",
        "chlesutq3dquxwfvv4ba65hjo",
        "ci0mj3nznl2mswxmit5tdiwic",
        "cif7u6dfjijtksln0bq4fvgus",
    ]
    
    # Get snapshot type from user
    snapshot_type = get_snapshot_type()
    
    # Export snapshots for all matches
    for match_id in match_ids:
        try:
            export_snapshot(match_id, snapshot_type)
            print(f"Exported {snapshot_type} for {get_formatted_name(match_id)}")
        except Exception as e:
            print(f"Error exporting {snapshot_type} for {match_id}: {e}")

