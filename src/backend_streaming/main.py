import uvicorn

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
# from backend_streaming.providers.whoscored.infra.api_routes import event_query_route
# from backend_streaming.providers.whoscored.infra.api_routes import get_lineup_route
# from backend_streaming.providers.whoscored.infra.api_routes import admin_route
from backend_streaming.providers.whoscored.infra.api_routes import all_routes
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
app.include_router(all_routes.router, prefix="/streaming")
# app.include_router(event_query_route.router, prefix="/provider")
# app.include_router(get_lineup_route.router, prefix="/provider")
# app.include_router(admin_route.router, prefix="/provider")

if __name__ == "__main__":
    uvicorn.run(
        "backend_streaming.main:app",
        host="0.0.0.0",
        port=8001,
        reload=True
    )