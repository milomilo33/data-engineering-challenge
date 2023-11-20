from sqlalchemy.orm import Session
from datetime import datetime

from . import models


def get_country_of_user(db: Session, user_id: str):
    return db.query(models.User.country).filter(models.User.id == user_id).first()

def insert_event(db: Session, event):
    match event.event_type:
        case 'registration':
            insert_registration_event(db, event)
        case 'transaction':
            insert_transaction_event(db, event)
        case 'login':
            insert_login_logout_event(db, event, is_login=True)
        case 'logout':
            insert_login_logout_event(db, event, is_login=False)
        case _:
            print("Insert error.")


# def insert_user(db: Session, registration_event):
#     db_user = models.User(id=registration_event.user_id,
#                           country=registration_event.country,
#                           name=registration_event.name,
#                           device_os=registration_event.device_os,
#                           marketing_campaign=registration_event.marketing_campaign)
    
#     db.add([db_event, db_user, db_registration_event])

#     # Commit every 1000 records
#     if len(db.new) % 1000 == 0:
#         db.commit()

def insert_registration_event(db: Session, registration_event):
    db_event = models.Event(id=registration_event.event_id)
    db_user = models.User(id=registration_event.user_id,
                          country=registration_event.country,
                          name=registration_event.name,
                          device_os=registration_event.device_os,
                          marketing_campaign=registration_event.marketing_campaign)
    
    event_datetime = datetime.fromtimestamp(registration_event.event_timestamp)
    db_registration_event = models.Registration(id=registration_event.event_id,
                                                event_datetime=event_datetime,
                                                user_id=registration_event.user_id)
    
    db_registration_event.user = db_user

    # db.add(db_event)
    # db.add(db_user)
    # db.commit()
    # db.add(db_registration_event)
    # db.commit()
    db.add_all([db_event, db_user, db_registration_event])

    # Commit every 1000 records
    if len(db.new) % 1000 == 0:
        db.commit()


def insert_transaction_event(db: Session, transaction_event):
    db_event = models.Event(id=transaction_event.event_id)
    event_datetime = datetime.fromtimestamp(transaction_event.event_timestamp)
    db_transaction_event = models.Transaction(id=transaction_event.event_id,
                                              event_datetime=event_datetime,
                                              user_id=transaction_event.user_id,
                                              transaction_amount=transaction_event.transaction_amount,
                                              transaction_currency=transaction_event.transaction_currency)
    
    db.add(db_event)
    db.add(db_transaction_event)
    db.add_all([db_event, db_transaction_event])

    # Commit every 1000 records
    if len(db.new) % 1000 == 0:
        db.commit()
    # db.commit()


def insert_login_logout_event(db: Session, login_logout_event, is_login: bool):
    db_event = models.Event(id=login_logout_event.event_id)
    event_datetime = datetime.fromtimestamp(login_logout_event.event_timestamp)
    db_login_logout_event = models.LoginLogout(id=login_logout_event.event_id,
                                              event_datetime=event_datetime,
                                              user_id=login_logout_event.user_id,
                                              is_login=is_login)
    
    db.add_all([db_event, db_login_logout_event])
    
    # Commit every 1000 records
    if len(db.new) % 1000 == 0:
        db.commit()
    # db.commit()
