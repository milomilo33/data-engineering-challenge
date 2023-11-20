from fastapi import Depends, FastAPI, HTTPException
from starlette.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

from . import crud, models
from .db import SessionLocal, engine
from .initialize import initialize

models.Base.metadata.drop_all(bind=engine)
models.Base.metadata.create_all(bind=engine)

app = FastAPI()

origins = [
    "http://localhost",
    "http://localhost:8080",
    "http://localhost:5173",
    "*"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["DELETE", "GET", "POST", "PUT"],
    allow_headers=["*"],
)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def init_db():
    db = SessionLocal()
    try:
        initialize(db)
    finally:
        db.close()

init_db()

@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/user/country", response_model=str)
def read_users(user_id: str, db: Session = Depends(get_db)):
    country = crud.get_country_of_user(db, user_id)
    if not country:
        raise HTTPException(status_code=404, detail="User not found or country is invalid.")
    return country
