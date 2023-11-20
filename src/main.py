from fastapi import Depends, FastAPI, HTTPException
from starlette.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

from . import crud, models
from .db import SessionLocal, engine
from .initialize import initialize

import datetime

# models.Base.metadata.drop_all(bind=engine)
# models.Base.metadata.create_all(bind=engine)

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

# init_db()

@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/user/country", response_model=str)
def get_country_of_user(user_id: str, db: Session = Depends(get_db)):
    country = crud.get_country_of_user(db, user_id)
    if not country:
        raise HTTPException(status_code=404, detail="User not found or country is invalid.")
    return country


@app.get("/user/name", response_model=str)
def get_name_of_user(user_id: str, db: Session = Depends(get_db)):
    name = crud.get_name_of_user(db, user_id)
    if not name:
        raise HTTPException(status_code=404, detail="User not found.")
    return name


@app.get("/user/logins", response_model=int)
def get_number_of_logins(user_id: str, input_date: str = '', db: Session = Depends(get_db)):
    if input_date:
        try:
            datetime.date.fromisoformat(input_date)
        except ValueError:
            raise HTTPException(status_code=400, detail="Incorrect date format, should be YYYY-MM-DD.")
    
    if not input_date:
        input_date = None
    return crud.get_number_of_logins(db, user_id, input_date)


@app.get("/user/days-since-login", response_model=str)
def get_days_since_last_login(user_id: str, input_date: str = '', db: Session = Depends(get_db)):
    if input_date:
        try:
            input_date = datetime.date.fromisoformat(input_date)
        except ValueError:
            raise HTTPException(status_code=400, detail="Incorrect date format, should be YYYY-MM-DD.")
    
    if not input_date:
        input_date = None
    return crud.get_days_since_last_login(db, user_id, input_date)

@app.get("/user/sessions", response_model=int)
def get_number_of_sessions(user_id: str, input_date: str = '', db: Session = Depends(get_db)):
    if input_date:
        try:
            input_date = datetime.date.fromisoformat(input_date)
        except ValueError:
            raise HTTPException(status_code=400, detail="Incorrect date format, should be YYYY-MM-DD.")
    
    if not input_date:
        input_date = None
    return crud.get_number_of_sessions(db, user_id, input_date)

