from backend_streaming.providers.opta.infra.api import get_tournament_schedule
from backend_streaming.providers.opta.constants import EPL_TOURNAMENT_ID
import asyncio


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
        "cif7u6dfjijtksln0bq4fvgus"
    ]
    
    async def main():
        mapping = await create_match_id_mapping(match_ids)
        for match_id, match_name in mapping.items():
            print(f"{match_id}: {match_name}")
    
    # Run with asyncio
    asyncio.run(main())
