from dotenv import load_dotenv
import os

load_dotenv()  # take environment variables from .env.


SUPABASE_URL=os.getenv("SUPABASE_URL", None)
GOOGLE_API_KEY=os.getenv("GOOGLE_API_KEY", None)
LANGSMITH_API_KEY=os.getenv("LANGSMITH_API_KEY", None)
REDIS_URI=os.getenv("REDIS_URI", None)
DATABASE_URI=os.getenv("DATABASE_URI", None)
SNOWFLAKE_ACCOUNT=os.getenv("SNOWFLAKE_ACCOUNT", None)
SNOWFLAKE_USER=os.getenv("SNOWFLAKE_USER", None)
SNOWFLAKE_PASSWORD=os.getenv("SNOWFLAKE_PASSWORD", None)
SNOWFLAKE_DATABASE=os.getenv("SNOWFLAKE_DATABASE", None)
SNOWFLAKE_SCHEMA=os.getenv("SNOWFLAKE_SCHEMA", None)
SNOWFLAKE_WAREHOUSE=os.getenv("SNOWFLAKE_WAREHOUSE", None)
SNOWFLAKE_ROLE=os.getenv("SNOWFLAKE_ROLE", None)


required_env_vars = [
    "SUPABASE_URL",
    "GOOGLE_API_KEY",
]

for var in required_env_vars:
    if not var:
        raise ValueError(f"Missing required environment variable: {var}")