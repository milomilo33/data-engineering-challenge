from sqlalchemy.orm import Session
from sqlalchemy.sql import text
import datetime

import numpy as np

from . import models

END_DATE = datetime.datetime(2010, 5, 22).date()

def get_country_of_user(db: Session, user_id: str):
    result = db.query(models.User.country).filter(models.User.id == user_id).first()
    if result:
        for r in result:
            return r


def get_name_of_user(db: Session, user_id: str):
    result = db.query(models.User.name).filter(models.User.id == user_id).first()
    if result:
        for r in result:
            return r


def get_number_of_logins(db: Session, user_id: str, input_date: datetime):
    optional_part = ' AND event_datetime::date = :input_date' if input_date else ''
    result = db.execute(
        text('SELECT COUNT(*) FROM login_logout WHERE user_id = :user_id AND is_login = true' + optional_part),
        {'user_id': user_id, 'input_date': input_date}
    )
    for arr in result:
        for r in arr:
            return r


def get_days_since_last_login(db: Session, user_id: str, input_date: datetime.date):
    if not input_date:
        input_date = END_DATE
    result = db.execute(
        text(
            'SELECT MAX(event_datetime) FROM login_logout \
            WHERE user_id = :user_id AND is_login = true AND event_datetime::date <= :input_date'),
        {'user_id': user_id, 'input_date': input_date}
    )
    last_login_date, found = None, False
    for arr in result:
        if not arr:
            return 'No login at or prior to this date.'
        for r in arr:
            if not r:
                return 'No login at or prior to this date.'
            last_login_date = r
            found = True
            break
        if found: break
    last_login_date = last_login_date.date()
    return str((input_date - last_login_date).days)


def get_number_of_sessions(db: Session, user_id: str, input_date: datetime.date):
    optional_part = 'l1.event_datetime::date = :input_date AND' if input_date else ''
    result = db.execute(
        text(
            f'SELECT COUNT(*) from login_logout AS l1 \
            INNER JOIN login_logout AS l2 \
            ON l1.matching_login_or_logout_id = l2.id AND \
            l1.user_id = :user_id AND \
            {optional_part} \
            l1.is_login = true AND \
            (l2.event_datetime - l1.event_datetime) >= interval \'1 minute\''),
        {'user_id': user_id, 'input_date': input_date}
    )
    for arr in result:
        for r in arr:
            return r
        

def get_time_spent_in_game(db: Session, user_id: str, input_date: datetime.date):
    optional_part = 'AND login.event_datetime::date = :input_date' if input_date else ''
    optional_part_select = '''
        SUM(
            EXTRACT(
                EPOCH FROM (
                    CASE
                        WHEN logout.event_datetime::date > login.event_datetime::date
                        THEN DATE_TRUNC('day', login.event_datetime) + INTERVAL '1 day'
                        ELSE logout.event_datetime
                    END
                    - login.event_datetime
                )
            )
        )
    ''' if input_date else 'SUM(EXTRACT(EPOCH FROM (logout.event_datetime - login.event_datetime)))'
    result = db.execute(
        # calculating durations of all sessions, cutting off at midnight when there is a date parameter
        text(f'''
            SELECT {optional_part_select}
            FROM login_logout AS login
            JOIN login_logout AS logout
            ON login.matching_login_or_logout_id = logout.id
            AND login.user_id = :user_id
            AND login.is_login = true
            {optional_part}
        '''),
        {'user_id': user_id, 'input_date': input_date}
    )
    # when a date param is set, we should consider logins with no logouts for the time spent that day
    if input_date:
        sum = 0
        for arr in result:
            for r in arr:
                if r:
                    sum = r
        result = db.execute(
            text(f'''
                SELECT SUM(EXTRACT(EPOCH FROM ((DATE_TRUNC('day', event_datetime) + INTERVAL '1 day') - event_datetime)))
                FROM login_logout
                WHERE user_id = :user_id AND is_login = true 
                AND event_datetime::date = :input_date
                AND matching_login_or_logout_id IS NULL
            '''),
            {'user_id': user_id, 'input_date': input_date}
        )
        for arr in result:
            for r in arr:
                if r:
                    sum += r
        return sum
    else:
        for arr in result:
            for r in arr:
                return r if r else 0


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


def insert_registration_event(db: Session, registration_event):
    db_event = models.Event(id=registration_event.event_id)
    db_user = models.User(id=registration_event.user_id,
                          country=registration_event.country,
                          name=registration_event['name'],
                          device_os=registration_event.device_os,
                          marketing_campaign=registration_event.marketing_campaign)
    
    event_datetime = datetime.fromtimestamp(registration_event.event_timestamp)
    db_registration_event = models.Registration(id=registration_event.event_id,
                                                event_datetime=event_datetime,
                                                user_id=registration_event.user_id)
    
    db_registration_event.user = db_user

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

    # commit every 1000 records
    if len(db.new) % 1000 == 0:
        db.commit()


def insert_login_logout_event(db: Session, login_logout_event, is_login: bool):
    db_event = models.Event(id=login_logout_event.event_id)
    event_datetime = datetime.fromtimestamp(login_logout_event.event_timestamp)
    if is_login:
        db_login_logout_event = models.LoginLogout(id=login_logout_event.event_id,
                                              event_datetime=event_datetime,
                                              user_id=login_logout_event.user_id,
                                              is_login=is_login)
    else:
        db_login_logout_event = models.LoginLogout(id=login_logout_event.event_id,
                                                event_datetime=event_datetime,
                                                user_id=login_logout_event.user_id,
                                                is_login=is_login,
                                                matching_login_or_logout_id=login_logout_event.matching_login_or_logout_id)
    
    db.add_all([db_event, db_login_logout_event])

    # Commit every 1000 records
    if len(db.new) % 1000 == 0:
        db.commit()


def add_matching_logout_ids(db: Session, login_event):
    if not np.isnan(login_event.matching_login_or_logout_id):
        db.execute(
            text("UPDATE login_logout SET matching_login_or_logout_id = :logout_id WHERE id = :id"),
            {'id': login_event.event_id, 'logout_id': login_event.matching_login_or_logout_id}
        )
