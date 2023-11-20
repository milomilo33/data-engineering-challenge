import numpy as np
import pandas as pd
import datetime
import pycountry
from sqlalchemy.orm import Session

from .crud import insert_event, add_matching_logout_ids

import time

# omitting timezones for clarity
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
    all_events = all_events[all_events['event_id'].notna()]
    all_events = all_events[all_events['event_timestamp'].astype(bool)]
    all_events = all_events[all_events['user_id'].astype(bool)]

    # remove duplicates by id, keeping the chronologically first event
    all_events = all_events.sort_values(by='event_timestamp')
    all_events = all_events.drop_duplicates(subset='event_id', keep='first')
    # print(str(START_DATE) + " - " + str(END_DATE))
    # print("Before dates:")
    # print(all_events)

    # dates (filter events that happened before May 8, 2010 or after May 22, 2010)
    all_events = all_events.query('event_timestamp >= @START_DATE and event_timestamp < @END_DATE')
    # print("After dates:")
    # print(all_events)

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

    # filter invalid login & logout events and add matching logout or login events
    login_logout_events = all_events[all_events['event_type'].isin(['login', 'logout'])].copy()
    login_logout_events.sort_values(by=['user_id', 'event_timestamp'], inplace=True)
    login_logout_events['matching_login_or_logout_id'] = np.nan
    login_logout_events['valid'] = False
    login_logout_events = login_logout_events.reset_index(drop=True)
    pd.set_option('display.max_columns', None)

    for idx, row in enumerate(login_logout_events.itertuples()):
        if row.event_type == 'login':
            if idx == 0:
                login_logout_events.loc[idx, 'valid'] = True
            else:
                for prev_idx in range(idx-1, -1, -1):
                    prev_row = login_logout_events.iloc[prev_idx]
                    if (prev_row['event_type'] == 'logout' and
                        prev_row['user_id'] == row.user_id and
                        prev_row['valid'] == True):
                        login_logout_events.loc[idx, 'valid'] = True
                        break
                    elif prev_row['user_id'] != row.user_id:
                        login_logout_events.loc[idx, 'valid'] = True
                        break
                    elif (prev_row['event_type'] == 'login' and
                          prev_row['user_id'] == row.user_id and
                          prev_row['valid'] == True):
                        break

        # logout
        else:
            if idx != 0:
                for prev_idx in range(idx-1, -1, -1):
                    prev_row = login_logout_events.iloc[prev_idx]
                    if (prev_row['event_type'] == 'login' and
                        prev_row['user_id'] == row.user_id and
                        prev_row['valid'] == True):
                        login_logout_events.loc[idx, 'valid'] = True
                        login_logout_events.loc[idx, 'matching_login_or_logout_id'] = prev_row['event_id']
                        login_logout_events.loc[prev_idx, 'matching_login_or_logout_id'] = row.event_id
                        break
                    elif prev_row['user_id'] != row.user_id:
                        break
                    elif (prev_row['event_type'] == 'logout' and
                          prev_row['user_id'] == row.user_id and
                          prev_row['valid'] == True):
                        break

    login_logout_events = login_logout_events.loc[login_logout_events['valid'] == True]

    # filter transactions that are done outside the user's session
    transaction_events = all_events[all_events['event_type'] == 'transaction'].copy()
    transaction_events['valid'] = False
    transaction_events = transaction_events.reset_index(drop=True)

    for idx, row in enumerate(transaction_events.itertuples()):
        matching_login_logout_for_user = login_logout_events.loc[login_logout_events['user_id'] == row.user_id]
        for matching_idx, matching_login_logout_row in enumerate(matching_login_logout_for_user.itertuples()):
            if matching_login_logout_row.event_type == 'login':
                if row.event_timestamp > matching_login_logout_row.event_timestamp:
                    shifted_timestamp = matching_login_logout_for_user['event_timestamp'].shift(-1).iloc[matching_idx]
                    # means login has no matching logout event
                    if np.isnan(shifted_timestamp):
                        transaction_events.loc[idx, 'valid'] = True
                    # otherwise shifted timestamp is the matching logout event's timestamp
                    elif row.event_timestamp < shifted_timestamp:
                        transaction_events.loc[idx, 'valid'] = True

    transaction_events = transaction_events.loc[transaction_events['valid'] == True]

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

    login_logout_events[login_logout_events['event_type'] == 'login'].apply(lambda x: insert_event(db, x), axis=1)
    db.commit()
    login_logout_events[login_logout_events['event_type'] == 'logout'].apply(lambda x: insert_event(db, x), axis=1)
    db.commit()
    login_logout_events[login_logout_events['event_type'] == 'login'].apply(lambda x: add_matching_logout_ids(db, x), axis=1)

    transaction_events.apply(lambda x: insert_event(db, x), axis=1)
    db.commit()

    end = time.time()
    print("Data cleaning and database population took: " + str(end - start) + "s.", flush=True)
