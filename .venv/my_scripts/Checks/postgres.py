from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

db_url = "postgresql://shahar:64478868@localhost:5433/cryptodb"

try:
    engine = create_engine(db_url)
    # Test the connection
    with engine.connect() as connection:
        print("Connection successful!")
except OperationalError as e:
    print(f"Connection failed: {e}")