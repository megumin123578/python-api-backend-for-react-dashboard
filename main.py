
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routes.traffic_timeseries import router as ts_router

app = FastAPI()

# (tuỳ môi trường) bật CORS cho frontend dev
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(ts_router)
