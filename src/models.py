from sqlalchemy import DateTime, Boolean, Float, ForeignKey, Integer, String, CheckConstraint
from sqlalchemy.orm import relationship, mapped_column

from .db import Base

# class Event(Base):
#     __tablename__ = "event"

#     id = mapped_column(Integer, primary_key=True)
#     timestamp = mapped_column(Integer, nullable=False)
#     type = mapped_column(String, nullable=False)

#     __mapper_args__ = {
#         # "polymorphic_identity": "event",
#         "polymorphic_on": "type",
#     }

#     __table_args__ = (
#         CheckConstraint(type.in_(['registration', 'Android', 'Web']), name='valid_device_os'),
#     )


#     def __repr__(self):
#         return f"{self.__class__.__name__}({self.name!r})"


# class Registration(Event):
#     __tablename__ = "registration"

#     id = mapped_column(ForeignKey("event.id"), primary_key=True)
#     user_id = mapped_column(String, nullable=False, unique=True)
#     country = mapped_column(String, nullable=False)
#     device_os = mapped_column(String, nullable=False)
#     marketing_campaign = mapped_column(String, nullable=True)

#     __table_args__ = (
#         CheckConstraint(device_os.in_(['iOS', 'Android', 'Web']), name='valid_device_os'),
#     )

#     __mapper_args__ = {
#         "polymorphic_identity": "registration",
#     }


# class Transaction(Event):
#     __tablename__ = "transaction"

#     id = mapped_column(ForeignKey("event.id"), primary_key=True)
#     user_id = mapped_column(String, nullable=False, unique=True)
#     country = mapped_column(String, nullable=False)
#     device_os = mapped_column(String, nullable=False)
#     marketing_campaign = mapped_column(String, nullable=True)

#     __table_args__ = (
#         CheckConstraint(device_os.in_(['iOS', 'Android', 'Web']), name='valid_device_os'),
#     )

#     __mapper_args__ = {
#         "polymorphic_identity": "registration",
#     }


class Event(Base):
    __tablename__ = "event"

    id = mapped_column(Integer, primary_key=True)


class Registration(Base):
    __tablename__ = "registration"

    id = mapped_column(ForeignKey("event.id"), primary_key=True)
    event_datetime = mapped_column(DateTime, nullable=False)
    user_id = mapped_column(ForeignKey("user.id"), nullable=False, unique=True)

    user = relationship("User", back_populates="registrations")

class User(Base):
    __tablename__ = "user"

    id = mapped_column(String, primary_key=True)
    country = mapped_column(String, nullable=False)
    name = mapped_column(String, nullable=False)
    device_os = mapped_column(String, nullable=False)
    marketing_campaign = mapped_column(String, nullable=True)

    registrations = relationship("Registration", back_populates="user")

    __table_args__ = (
        CheckConstraint(device_os.in_(['iOS', 'Android', 'Web']), name='valid_device_os'),
    )


class Transaction(Base):
    __tablename__ = "transaction"

    id = mapped_column(ForeignKey("event.id"), primary_key=True)
    event_datetime = mapped_column(DateTime, nullable=False)
    user_id = mapped_column(ForeignKey("user.id"), nullable=False)

    transaction_amount = mapped_column(Float, nullable=False)
    transaction_currency = mapped_column(String, nullable=False)

    __table_args__ = (
        CheckConstraint(transaction_amount.in_([0.99, 1.99, 2.99, 4.99, 9.99]), name='valid_transaction_amount'),
        CheckConstraint(transaction_currency.in_(['EUR', 'USD']), name='valid_transaction_currency')
    )


class LoginLogout(Base):
    __tablename__ = "login_logout"

    id = mapped_column(ForeignKey("event.id"), primary_key=True)
    event_datetime = mapped_column(DateTime, nullable=False)
    user_id = mapped_column(ForeignKey("user.id"), nullable=False)

    # if False, then Logout
    is_login = mapped_column(Boolean, nullable=False)

