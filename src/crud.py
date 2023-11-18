from sqlalchemy.orm import Session
from datetime import datetime

from . import models


def get_country_of_user(db: Session, user_id: str):
    return db.query(models.User.country).filter(models.User.id == user_id).first()


def insert_registration_event(db: Session, registration_event):
    db_event = models.Event(id=registration_event.event_id)
    db_user = models.User(id=registration_event.event_data.user_id,
                          country=registration_event.event_data.country,
                          name=registration_event.event_data.name,
                          device_os=registration_event.event_data.device_os,
                          marketing_campaign=registration_event.event_data.marketing_campaign)
    
    event_datetime = datetime.fromtimestamp(registration_event.event_timestamp)
    db_registration_event = models.Registration(id=registration_event.event_id,
                                                event_datetime=event_datetime,
                                                user_id=registration_event.event_data.user_id)
    
    db.add(db_event)
    db.add(db_user)
    db.add(db_registration_event)
    db.commit()
    db.refresh(db_event)
    db.refresh(db_user)
    db.refresh(db_registration_event)
