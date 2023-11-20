# Nordeus Data Engineering Challenge 2023
This project aims to analyze events emitted from a football manager game (such as registrations, logins, transactions etc.) in order to evaluate the employed marketing campaigns.

The specification file is given [here](https://github.com/milomilo33/data-engineering-challenge/blob/main/JobFair%202023-%20Data%20Engineering%20Challenge_.pdf).

## My approach
I used the following technologies for the development of this project: _Python_, _FastAPI_, _PostgreSQL_, _Docker_.

The solution was implemented as a RESTful API, with a data model stored in PostgreSQL (leveraging Python's _SQLAlchemy_ ORM). Data cleaning was implemented with the use of Python's _Pandas_ and _NumPy_ libraries.

## Data model
Diagram of the database schema is given below.

![image](https://github.com/milomilo33/data-engineering-challenge/assets/29868001/e6d31ef6-9ed8-4a89-b8ab-20a818d309db)

I decided to keep the different types of events mostly separate (where possible), with the goal of optimizing the model as most queries require only one type of event.

## Data cleaning
The following data cleaning steps were taken:
1. Dropped rows with no event_id, event_timestamp or user_id.
2. Removed duplicates by event_id, keeping only the chronologically first event.
3. Filtered events that happened before the launch of the game (May 8, 2010) or after the date of analysis (May 22, 2010), according to the specification.
4. Dropped rows with invalid event types.
5. Dropped multiple registration events, keeping only the chronologically first one.
  - Why: A user can only register once.
6. Filtered non-registration events done by nonexistent users.
  - Why: A user can only trigger non-registration events once he has registered.
7. Filtered non-registration events emitted before user registered.
  - Why: Much like the above, a user cannot trigger non-registration events before he is registered.
8. Filtered invalid login & logout events (logout with no prior login, multiple logins without prior logout, etc.) and added matching logout or login's identifier to each login and logout event (although nullable in the case of logins with no later logouts) for easier data analysis.
  - Why: It makes no sense, for example, for multiple logout events to exist at a time when the user has not previously logged in.
9. Filtered transactions that are emitted when the user is not logged in.
  - Why: According to the specification, transactions are only done in-game, meaning the user has to be logged in, in order for the transaction event to be valid.
10. Dropped registration events with invalid country names, by checking if the country name is an ISO 3166-1 alpha-2 code.
11. Dropped registration events with invalid device_os (a.k.a. not one of: iOS, Android or Web).
12. Dropped transaction events with invalid transaction amounts (a.k.a. amounts not in: 0.99, 1.99, 2.99, 4.99, 9.99).
13. Dropped transaction events with invalid transaction currencies (a.k.a. not either of: USD, EUR).

## How to use
To run this project, make sure you have Docker set up. Then simply, from the project's root folder:

```
$ docker compose up
```

Once the containers are up and running, open [http://localhost:8001/docs](http://localhost:8001/docs), where you can see the documented API and easily test the API's endpoints with the required and optional parameters (displayed in the same order as the queries in the specification file):

![image](https://github.com/milomilo33/data-engineering-challenge/assets/29868001/c22b9740-daa5-488b-ba4f-cfe36e9a7603)

Example of querying the number of logins for a specific date, grouped by country:

![image](https://github.com/milomilo33/data-engineering-challenge/assets/29868001/51638591-e866-43fd-8db1-fe8081c0fa15)

From there, we can see that on May 8, 2010 there were a total of 9 logins in Germany, 4 in Spain and 4 in Italy, which amounts to a total of 17 login events on that day. We can get that result by doing the same query, while specifying that we do not want to group the results by country:

![image](https://github.com/milomilo33/data-engineering-challenge/assets/29868001/1c246a9d-8427-4142-8202-0de11cde2674)

