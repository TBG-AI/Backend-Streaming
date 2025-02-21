import soccerdata as sd
import pandas as pd
from src.backend_streaming.providers.whoscored.infra.config.config import paths
import json
import random
import string
from backend_streaming.providers.opta.infra.models import PlayerModel
from backend_streaming.providers.opta.infra.db import get_session
from datetime import datetime
import argparse


def check_game_data(loader, game_id):
    try:
        players = loader.players(game_id)
        return players
    except TypeError:
        return None


def get_players_data():
    scraper = sd.WhoScored(leagues="ENG-Premier League", seasons='24-25')
    try:
        loader = scraper.read_events(
            output_fmt="loader", 
            force_cache=True, 
            live=False,
        )
        all_players_data = []
        schedule = scraper.read_schedule(force_cache=True)
        for game_id in schedule['game_id']:
            players_df = check_game_data(loader, game_id)
            if players_df is not None:
                # Check if we already have data for these players
                if all_players_data:
                    existing_player_ids = set(pd.concat(all_players_data)['player_id'])
                    players_df = players_df[~players_df['player_id'].isin(existing_player_ids)]
                
                if not players_df.empty:
                    players_df['game_id'] = game_id
                    all_players_data.append(players_df)
                    print(f"Successfully loaded data for game {game_id} ({len(players_df)} new players)")
                else:
                    print(f"No new players found for game {game_id}")
            else:
                print(f"Skipping game {game_id} - no data available")
        
        if all_players_data:
            final_players_df = pd.concat(all_players_data, ignore_index=True)
            print(f"\nTotal games processed: {len(all_players_data)}")
            print(f"Total unique players: {len(final_players_df)}")
            return final_players_df
        else:
            print("No valid player data found")
            return pd.DataFrame()

    except Exception as e:
        print(f"An error occurred: {e}")
        raise e
    

def get_unmapped_players(players_df):
    """Get players that aren't in the existing mapping"""
    # Load existing player mappings
    with open(paths.mappings_dir / "player_ids.json") as f:
        existing_mappings = json.load(f)
    
    # Convert existing WhoScored IDs to strings for comparison    
    # Filter players_df to only include players not in mapping
    existing_ws_ids = set(existing_mappings.keys())
    unmapped_players = players_df[~players_df['player_id'].astype(str).isin(existing_ws_ids)]

    return unmapped_players


def generate_opta_id():
    """Generate random Opta-like ID"""
    length = random.randint(20, 25)  # Opta IDs vary in length
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))


def insert_unmapped_players(unmapped_players):
    """Insert unmapped players into database and update mappings"""
    # Load existing mappings
    with open(paths.mappings_dir / "player_ids.json") as f:
        player_mappings = json.load(f)
    
    with open(paths.mappings_dir / "team_ids.json") as f:
        team_mappings = json.load(f)
    
    session = get_session()
    try:
        for _, player in unmapped_players.iterrows():
            # Generate new Opta-like ID
            opta_id = generate_opta_id()
            
            # Get team's Opta ID from mappings
            ws_team_id = str(player['team_id'])
            opta_team_id = team_mappings.get(ws_team_id)
            
            if not opta_team_id:
                print(f"Warning: No team mapping found for WhoScored team ID {ws_team_id}")
                continue
            
            # Parse player name
            name_parts = player['player_name'].split(' ')
            first_name = name_parts[0]
            last_name = ' '.join(name_parts[1:]) if len(name_parts) > 1 else "PLACEHOLDER"
            
            # Create player model
            new_player = PlayerModel(
                player_id=opta_id,
                first_name=first_name,
                last_name=last_name,
                short_first_name=first_name,
                short_last_name=last_name,
                gender="PLACEHOLDER",
                match_name=player['player_name'],
                nationality="PLACEHOLDER",
                nationality_id="PLACEHOLDER",
                position=player['starting_position'],
                type="PLACEHOLDER",
                date_of_birth="PLACEHOLDER",
                place_of_birth="PLACEHOLDER",
                country_of_birth="PLACEHOLDER",
                country_of_birth_id="PLACEHOLDER",
                height=0,
                weight=0,
                foot="PLACEHOLDER",
                shirt_number=player['jersey_number'],
                status="active",
                active="true",
                team_id=opta_team_id,
                team_name="PLACEHOLDER",
                last_updated=datetime.utcnow().isoformat()
            )
            
            # Add to database
            session.add(new_player)
            
            # Update mappings
            player_mappings[str(player['player_id'])] = opta_id
            
            print(f"Added player: {player['player_name']} (WS ID: {player['player_id']} -> Opta ID: {opta_id})")
        
        # Commit database changes
        session.commit()
        
        # Save updated mappings
        with open(paths.mappings_dir / "player_ids.json", 'w') as f:
            json.dump(player_mappings, f, indent=2)
            
        print(f"\nSuccessfully added {len(unmapped_players)} new players to database and mappings")
        
    except Exception as e:
        session.rollback()
        print(f"Error inserting players: {e}")
        raise
    finally:
        session.close()


def check_and_create_missing_players(players_df):
    """Check if mapped players exist in database and create if missing"""
    # Load mappings
    with open(paths.mappings_dir / "player_ids.json") as f:
        player_mappings = json.load(f)
    
    with open(paths.mappings_dir / "team_ids.json") as f:
        team_mappings = json.load(f)
    
    # Get all Opta IDs from mappings
    opta_ids = set(player_mappings.values())
    
    session = get_session()
    try:
        # Check which IDs exist in database
        existing_ids = set(id_[0] for id_ in session.query(PlayerModel.player_id).all())
        missing_ids = opta_ids - existing_ids
        
        if not missing_ids:
            print("\nAll mapped players exist in database")
            return
            
        print(f"\nFound {len(missing_ids)} mapped players missing from database")
        
        # Create players for missing IDs
        created_count = 0
        for opta_id in missing_ids:
            # Find WhoScored ID for this Opta ID
            ws_id = next(ws_id for ws_id, opt_id in player_mappings.items() if opt_id == opta_id)
            
            # Find player data in DataFrame
            player_data = players_df[players_df['player_id'].astype(str) == ws_id]
            if player_data.empty:
                print(f"Warning: No data found for player with WhoScored ID {ws_id}")
                continue
                
            player = player_data.iloc[0]
            
            # Get team's Opta ID
            ws_team_id = str(player['team_id'])
            opta_team_id = team_mappings.get(ws_team_id)
            
            if not opta_team_id:
                print(f"Warning: No team mapping found for WhoScored team ID {ws_team_id}")
                continue
            
            # Parse player name
            name_parts = player['player_name'].split(' ')
            first_name = name_parts[0]
            last_name = ' '.join(name_parts[1:]) if len(name_parts) > 1 else "PLACEHOLDER"
            
            # Convert numpy.int64 to Python int
            jersey_number = int(player['jersey_number']) if pd.notna(player['jersey_number']) else None
            
            # Create player model
            new_player = PlayerModel(
                player_id=opta_id,
                first_name=first_name,
                last_name=last_name,
                short_first_name=first_name,
                short_last_name=last_name,
                gender="PLACEHOLDER",
                match_name=player['player_name'],
                nationality="PLACEHOLDER",
                nationality_id="PLACEHOLDER",
                position=player['starting_position'],
                type="PLACEHOLDER",
                date_of_birth="PLACEHOLDER",
                place_of_birth="PLACEHOLDER",
                country_of_birth="PLACEHOLDER",
                country_of_birth_id="PLACEHOLDER",
                height=0,
                weight=0,
                foot="PLACEHOLDER",
                shirt_number=jersey_number,  # Use converted number
                status="active",
                active="true",
                team_id=opta_team_id,
                team_name="PLACEHOLDER",
                last_updated=datetime.utcnow().isoformat()
            )
            
            session.add(new_player)
            created_count += 1
            print(f"Created player: {player['player_name']} (WS ID: {ws_id} -> Opta ID: {opta_id})")
        
        session.commit()
        print(f"\nSuccessfully created {created_count} missing players")
        
    except Exception as e:
        session.rollback()
        print(f"Error creating players: {e}")
        raise
    finally:
        session.close()


def main():
    parser = argparse.ArgumentParser(description='Player data management tools')
    parser.add_argument('--mode', type=str, choices=['update', 'check'],
                       help='Mode to run: "update" to fetch and update mappings, "check" to verify existing mappings')
    
    args = parser.parse_args()

    players_df = get_players_data()
    if not players_df.empty:
        if args.mode == 'update':
            # Original functionality
            unmapped_players = get_unmapped_players(players_df)
            print(f"\nFound {len(unmapped_players)} unmapped players:")
            insert_unmapped_players(unmapped_players)
        elif args.mode == 'check':
            # New functionality
            check_and_create_missing_players(players_df)
        else:
            print("\nPlease specify a mode: --mode update|check")
    else:
        print("\nNo players to process")


if __name__ == "__main__":
    main()