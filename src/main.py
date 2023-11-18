from fastapi import Depends, FastAPI, HTTPException
from sqlalchemy.orm import Session

from . import crud, models
from .db import SessionLocal, engine

models.Base.metadata.create_all(bind=engine)

app = FastAPI()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/user/country", response_model=str)
def read_users(user_id: str, db: Session = Depends(get_db)):
    country = crud.get_country_of_user(db, user_id)
    if not country:
        raise HTTPException(status_code=404, detail="User not found or country is invalid.")
    return country
