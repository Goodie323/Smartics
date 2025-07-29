import os
from sqlalchemy import create_engine

DATABASE_URL = os.getenv("DATABASE_URL")  # Render provides this automatically

engine = create_engine(DATABASE_URL)