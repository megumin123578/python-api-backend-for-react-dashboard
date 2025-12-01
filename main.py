
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routes.traffic_timeseries import router as ts_router
from routes.geography import router as geo_router

app = FastAPI()


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # hoặc chỉ http://localhost:3000
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(ts_router)
app.include_router(geo_router)
