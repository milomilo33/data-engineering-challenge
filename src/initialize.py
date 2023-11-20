import pandas as pd
import datetime
import pycountry
from sqlalchemy.orm import Session

from .crud import insert_event

import time

START_DATE = datetime.datetime(2010, 5, 8, 0, 0).timestamp()
END_DATE = datetime.datetime(2010, 5, 23, 0, 0).timestamp()

VALID_DEVICE_OS = ['iOS', 'Android', 'Web']
VALID_TRANSACTION_AMOUNT = [0.99, 1.99, 2.99, 4.99, 9.99]
VALID_TRANSACTION_CURRENCY = ['EUR', 'USD']

def initialize(db: Session):
    print("Cleaning data & populating DB...", flush=True)
    start = time.time()
    all_events = pd.read_json(path_or_buf="./src/events.jsonl", lines=True)
    # when running locally
    # all_events = pd.read_json(path_or_buf="events.jsonl", lines=True)

    # flatten
    all_events = pd.concat([all_events.drop(['event_data'], axis=1), all_events['event_data'].apply(pd.Series)], axis=1)

    # data cleaning

    # print(all_events.event_type.unique())
    # print(all_events.event_id.unique())
    # print(all_events.dtypes)
    # print(all_events.head)
    # print(all_events['event_id'].is_unique)
    # print(all_events.sort_values(by='event_timestamp').event_timestamp)
    # duplicate_rows = all_events.duplicated()
    # print("Number of duplicate rows: ", duplicate_rows.sum())
    # all_events.dropna

    # drop rows with no event_id, timestamp or user_id
    all_events = all_events[all_events['event_id'].astype(bool)]
    all_events = all_events[all_events['event_timestamp'].astype(bool)]
    all_events = all_events[all_events['user_id'].astype(bool)]

    # remove duplicates by id, keeping the chronologically first event
    all_events = all_events.sort_values(by='event_timestamp')
    all_events = all_events.drop_duplicates(subset='event_id', keep='first')
    print(str(START_DATE) + " - " + str(END_DATE))
    print("Before dates:")
    print(all_events)
    # dates (filter events that happened before May 8, 2010 or after May 22, 2010)
    all_events = all_events.query('event_timestamp >= @START_DATE and event_timestamp < @END_DATE')
    print("After dates:")
    print(all_events)
    # drop invalid event types
    all_events = all_events.loc[all_events['event_type'].isin(['registration', 'login', 'logout', 'transaction'])]

    # drop multiple registration events, keep only the first one
    all_events = (
        all_events
        .loc[~((all_events['event_type'] == 'registration') & all_events.duplicated(subset=['event_type', 'user_id'], keep=False))]
        .sort_values(by='event_timestamp')
    )

    # filter non-registration events done by nonexistent users
    registered_user_ids = all_events.loc[all_events['event_type'] == 'registration', 'user_id']
    all_events = all_events.loc[~(
        (all_events['event_type'] != 'registration') & 
        ~(all_events['user_id'].isin(registered_user_ids))
    )]

    # filter non-registration events done before user registered 
    registration_timestamps_df = (
        all_events[all_events['event_type'] == 'registration']
            .set_index('user_id')
            .rename(columns={'event_timestamp': 'registration_timestamp'})
    )
    all_events_with_registration_timestamp = ( 
        all_events
            .merge(registration_timestamps_df, left_on='user_id', right_index=True, how='left')
    )
    all_events = all_events.loc[~(
        (all_events['event_type'] != 'registration') & 
        (all_events['event_timestamp'] < 
         all_events_with_registration_timestamp['registration_timestamp'])
    )]

    # drop rows with invalid country (registration events)
    all_countries = list(pycountry.countries)
    alpha_2_codes = [country.alpha_2 for country in all_countries]
    all_events = all_events.loc[~((all_events['event_type'] == 'registration') & 
                                ~(all_events['country'].isin(alpha_2_codes)))]

    # drop rows with invalid device_os (registration events)
    all_events = all_events.loc[~((all_events['event_type'] == 'registration') & 
                                ~(all_events['device_os'].isin(VALID_DEVICE_OS)))]

    # drop rows with invalid transaction amount (transaction events)
    all_events = all_events.loc[~((all_events['event_type'] == 'transaction') & 
                                ~(all_events['transaction_amount'].isin(VALID_TRANSACTION_AMOUNT)))]

    # drop rows with invalid transaction currency (transaction events)
    all_events = all_events.loc[~((all_events['event_type'] == 'transaction') & 
                                ~(all_events['transaction_currency'].isin(VALID_TRANSACTION_CURRENCY)))]


    # populate database
    registration_events = all_events[all_events['event_type'] == 'registration']
    registration_events.apply(lambda x: insert_event(db, x), axis=1)
    db.commit()

    non_registration_events = all_events[all_events['event_type'] != 'registration']
    non_registration_events.apply(lambda x: insert_event(db, x), axis=1)
    db.commit()
    
    # all_events.apply(lambda x: insert_event(db, x), axis=1)

    end = time.time()
    print("Data cleaning and database population took: " + str(end - start) + "s.", flush=True)
