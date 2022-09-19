import os, sys
sys.path.insert(0, os.path.abspath("."))
from utils.database import create_tables

create_tables()