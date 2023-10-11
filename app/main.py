from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.router import files,report


app = FastAPI()

origins = [
    "*",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


app.include_router(files.router, tags=['report'], prefix='/api/file')
app.include_router(report.router, tags=['report'], prefix='/api/report')


@app.get("/api/status")
def root():
    return {"message": "Welcome to FastAPI with MongoDB"}