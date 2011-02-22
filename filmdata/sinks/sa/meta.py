from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import MetaData

__all__ = ['Session', 'engine', 'metadata']

# SQLAlchemy database engine. Updated by model.init_model()
engine = None
#
# # SQLAlchemy session manager. Updated by model.init_model()
Session = scoped_session(sessionmaker())

metadata = MetaData()
