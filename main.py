from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api import admin, public

app = FastAPI(
    title="Football Tournament Simulator API",
    version="0.1.0",
)

# CORS settings (adjust origins for your Next.js dev URL)
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers
app.include_router(admin.router, prefix="/admin", tags=["admin"])
app.include_router(public.router, tags=["public"])


@app.get("/", tags=["health"])
def root():
    return {"message": "Football Tournament Simulator API is running"}