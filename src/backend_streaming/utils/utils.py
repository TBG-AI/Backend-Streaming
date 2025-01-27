from backend_streaming.providers.opta.infra.api import get_tournament_schedule
from backend_streaming.providers.opta.constants import EPL_TOURNAMENT_ID


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