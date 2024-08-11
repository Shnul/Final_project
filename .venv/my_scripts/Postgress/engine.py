from sqlalchemy import create_engine, Column, Integer, String, Sequence
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Define the database URL
db_url = "postgresql://shahar:64478868@localhost:5433/cryptodb"

# Create an engine
engine = create_engine(db_url)

# Define a base class for declarative class definitions
Base = declarative_base()
Base.metadata = Base.metadata = engine

# Define a table schema
class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, Sequence('user_id_seq'), primary_key=True)
    name = Column(String(50))
    email = Column(String(50))

# Create the table in the database
Base.metadata.create_all(engine)

# Create a session
Session = sessionmaker(bind=engine)
session = Session()