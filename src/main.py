from fastapi import Depends, FastAPI, HTTPException
from starlette.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

from . import crud, models
from .db import SessionLocal, engine
from .initialize import initialize

import datetime

models.Base.metadata.drop_all(bind=engine)
models.Base.metadata.create_all(bind=engine)

tags_metadata = [
    {
        "name": "user",
        "description": "User level stats endpoints.\n Return value is a string or numeric value."
    },
    {
        "name": "game",
        "description": "Game level stats endpoints.\n Return value is a dict with indexes starting from 0. \
                        If the output is a single numeric value, the value will be stored in dict['0']. \
                        If the output is grouped by country, the values are stored in multiple key-value pairs (as many as there are grouped countries)"
    }
]

app = FastAPI(openapi_tags=tags_metadata)

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

# @app.get("/")
# def read_root():
#     return {"Hello": "World"}


@app.get("/user/country", response_model=str, tags=["user"])
def get_country_of_user(user_id: str, db: Session = Depends(get_db)):
    country = crud.get_country_of_user(db, user_id)
    if not country:
        raise HTTPException(status_code=404, detail="User not found or country is invalid.")
    return country


@app.get("/user/name", response_model=str, tags=["user"])
def get_name_of_user(user_id: str, db: Session = Depends(get_db)):
    name = crud.get_name_of_user(db, user_id)
    if not name:
        raise HTTPException(status_code=404, detail="User not found.")
    return name


@app.get("/user/logins", response_model=int, tags=["user"])
def get_number_of_logins(user_id: str, input_date: str = '', db: Session = Depends(get_db)):
    if input_date:
        try:
            datetime.date.fromisoformat(input_date)
        except ValueError:
            raise HTTPException(status_code=400, detail="Incorrect date format, should be YYYY-MM-DD.")
    
    if not input_date:
        input_date = None
    return crud.get_number_of_logins(db, user_id, input_date)


@app.get("/user/days-since-login", response_model=str, tags=["user"])
def get_days_since_last_login(user_id: str, input_date: str = '', db: Session = Depends(get_db)):
    if input_date:
        try:
            input_date = datetime.date.fromisoformat(input_date)
        except ValueError:
            raise HTTPException(status_code=400, detail="Incorrect date format, should be YYYY-MM-DD.")
    
    if not input_date:
        input_date = None
    return crud.get_days_since_last_login(db, user_id, input_date)


@app.get("/user/sessions", response_model=int, tags=["user"])
def get_number_of_sessions(user_id: str, input_date: str = '', db: Session = Depends(get_db)):
    if input_date:
        try:
            input_date = datetime.date.fromisoformat(input_date)
        except ValueError:
            raise HTTPException(status_code=400, detail="Incorrect date format, should be YYYY-MM-DD.")
    
    if not input_date:
        input_date = None
    return crud.get_number_of_sessions(db, user_id, input_date)


@app.get("/user/time-in-game", response_model=int, tags=["user"])
def get_time_spent_in_game(user_id: str, input_date: str = '', db: Session = Depends(get_db)):
    if input_date:
        try:
            input_date = datetime.date.fromisoformat(input_date)
        except ValueError:
            raise HTTPException(status_code=400, detail="Incorrect date format, should be YYYY-MM-DD.")
    
    if not input_date:
        input_date = None
    return crud.get_time_spent_in_game(db, user_id, input_date)


@app.get("/game/daily-active-users", response_model=dict, tags=["game"])
def get_number_of_daily_active_users(input_date: str = '', country: bool = False, db: Session = Depends(get_db)):
    if input_date:
        try:
            input_date = datetime.date.fromisoformat(input_date)
        except ValueError:
            raise HTTPException(status_code=400, detail="Incorrect date format, should be YYYY-MM-DD.")
    
    if not input_date:
        input_date = None
    return crud.get_number_of_daily_active_users(db, input_date, country)


@app.get("/game/logins", response_model=dict, tags=["game"])
def get_number_of_logins(input_date: str = '', country: bool = False, db: Session = Depends(get_db)):
    if input_date:
        try:
            input_date = datetime.date.fromisoformat(input_date)
        except ValueError:
            raise HTTPException(status_code=400, detail="Incorrect date format, should be YYYY-MM-DD.")
    
    if not input_date:
        input_date = None
    return crud.get_number_of_logins(db, input_date, country)


@app.get("/game/revenue", response_model=dict, tags=["game"])
def get_total_revenue_in_usd(input_date: str = '', country: bool = False, db: Session = Depends(get_db)):
    if input_date:
        try:
            input_date = datetime.date.fromisoformat(input_date)
        except ValueError:
            raise HTTPException(status_code=400, detail="Incorrect date format, should be YYYY-MM-DD.")
    
    if not input_date:
        input_date = None
    return crud.get_total_revenue_in_usd(db, input_date, country)


@app.get("/game/paid-users", response_model=dict, tags=["game"])
def get_number_of_paid_users(input_date: str = '', country: bool = False, db: Session = Depends(get_db)):
    if input_date:
        try:
            input_date = datetime.date.fromisoformat(input_date)
        except ValueError:
            raise HTTPException(status_code=400, detail="Incorrect date format, should be YYYY-MM-DD.")
    
    if not input_date:
        input_date = None
    return crud.get_number_of_paid_users(db, input_date, country)


@app.get("/game/average-number-of-sessions", response_model=dict, tags=["game"])
def get_average_number_of_sessions_for_users_with_sessions(input_date: str = '', country: bool = False, db: Session = Depends(get_db)):
    if input_date:
        try:
            input_date = datetime.date.fromisoformat(input_date)
        except ValueError:
            raise HTTPException(status_code=400, detail="Incorrect date format, should be YYYY-MM-DD.")
    
    if not input_date:
        input_date = None
    return crud.get_average_number_of_sessions_for_users_with_sessions(db, input_date, country)


@app.get("/game/average-total-time-spent", response_model=dict, tags=["game"])
def get_average_total_time_spent_in_game(input_date: str = '', country: bool = False, db: Session = Depends(get_db)):
    if input_date:
        try:
            input_date = datetime.date.fromisoformat(input_date)
        except ValueError:
            raise HTTPException(status_code=400, detail="Incorrect date format, should be YYYY-MM-DD.")
    
    if not input_date:
        input_date = None
    return crud.get_average_total_time_spent_in_game(db, input_date, country)
