# import argparse
# import os
# import logging
# from typing import List
# from db.core.factory import DatabaseClientFactory as db_factory

# from backend_streaming.config.logging import setup_logging
# from backend_streaming.shared.streamer import SingleGameStreamer
# from backend_streaming.providers.local.local import LocalDataProvider
# from backend_streaming.providers.local.utils import reset_all
# from backend_streaming.constants import GAMES_DIR

# # Configure logging first, before other imports
# setup_logging()
# logger = logging.getLogger(__name__)

# def run_game(game_id: int, provider: str):
#     """Run a single game instance"""
#     if provider == "local":
#         reset_all()
#         provider = LocalDataProvider(game_id)
#         db_client = db_factory.get_sql_client("postgres-local")
#         sqs_client = db_factory.get_sqs_client("sqs-local", group_id=game_id)
#     else:
#         raise ValueError(f"Invalid provider: {provider}")

#     game = SingleGameStreamer(
#         game_id=game_id,
#         provider=provider,
#         db_client=db_client,
#         sqs_client=sqs_client,
#     )
#     game.run()


# def main():
#     parser = argparse.ArgumentParser(description='Process game events')
#     parser.add_argument('--all', action='store_true', help='Run all available games simultaneously')
#     parser.add_argument('game_id', type=int, nargs='?', help='Game ID to process (e.g., 1821102 for Chelsea v Brighton)')
#     parser.add_argument('--provider', type=str, help='Data provider to use. For test, use "local"')
#     args = parser.parse_args()
    
    
#     if args.all:
#         game_ids = [f.split('.')[0] for f in os.listdir(GAMES_DIR)]
#         for game_id in game_ids:
#             run_game(int(game_id), args.provider)
#     else:
#         run_game(args.game_id, args.provider)

# if __name__ == "__main__":
#     main() 

import uvicorn

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from backend_streaming.providers.opta.infra.api_routes import event_query_route

app = FastAPI()

# Add CORS middleware if needed
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Specify your allowed origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# Include your router
app.include_router(event_query_route.router, prefix="/provider")

if __name__ == "__main__":
    uvicorn.run(
        "backend_streaming.main:app",
        host="0.0.0.0",
        port=8001,
        reload=True
    )