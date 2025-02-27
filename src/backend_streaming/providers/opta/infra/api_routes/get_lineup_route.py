from fastapi import APIRouter, HTTPException
from backend_streaming.providers.whoscored.app.services.get_lineup import GetLineupService

router = APIRouter()

@router.get("/get_lineup/{game_id}")
async def get_lineup(game_id: str):
    """
    Given the game_id, return the lineup for both teams.
    """
    try:
        get_lineup = GetLineupService(game_id)
        # TODO: make this into a param. for now, always convert to opta
        lineups = get_lineup.get_lineup(convert_to_opta=True)
        
        if "error" in lineups:
            raise HTTPException(
                status_code=404, 
                detail=f"Lineup not found: {lineups['error']}"
            )
            
        return lineups
        
    except ValueError:
        raise HTTPException(
            status_code=400, 
            detail="Invalid game_id format"
        )
