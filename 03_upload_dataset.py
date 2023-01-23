import logging
from dotenv import dotenv_values
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.sql.expression import text
from sqlalchemy.dialects.postgresql import ARRAY, TEXT, INTEGER

logging.basicConfig(level=logging.INFO,
                    format='%(relativeCreated)d ms: %(message)s')

default_na_value = '(Unspecified/Other)'
na_values = {
    'na_study_type': default_na_value,
    'na_phase': 'N/A',
    'na_recruitment_status': default_na_value,
    'na_medarea': default_na_value,
    'na_country': default_na_value,
}

SQL_DROP_TABLE = "DROP TABLE IF EXISTS ictrp CASCADE"
SQL_ADD_PRIMARY_KEY = text("ALTER TABLE ictrp ADD PRIMARY KEY (trial_id)")
SQL_CREATE_VIEW = text("""
CREATE MATERIALIZED VIEW ictrp_rollout AS
WITH ictrp_nvl AS
(
SELECT 
    trial_id,
    registration_date,
    COALESCE(study_type, :na_study_type) AS study_type,
    COALESCE(phases::TEXT[], ARRAY[:na_phase]) AS phases,
    COALESCE(recruitment_status, :na_recruitment_status) AS recruitment_status,
    COALESCE(medarea, :na_medarea) AS medarea,
    sponsor,
    COALESCE(countries, ARRAY[:na_country]) AS countries
FROM ictrp
),
trial_phase AS
(
SELECT 
    trial_id,
    UNNEST(phases) AS phase
FROM ictrp_nvl
),
trial_country AS
(
SELECT 
    trial_id,
    UNNEST(countries) AS country
FROM ictrp_nvl
)
SELECT
    ictrp_nvl.trial_id,
    ictrp_nvl.registration_date,
    ictrp_nvl.study_type,
    trial_phase.phase,
    ictrp_nvl.recruitment_status,
    ictrp_nvl.medarea,
    ictrp_nvl.sponsor,
    trial_country.country
FROM ictrp_nvl
INNER JOIN trial_phase ON ictrp_nvl.trial_id = trial_phase.trial_id
INNER JOIN trial_country ON ictrp_nvl.trial_id = trial_country.trial_id
""")

SQL_CREATE_INDEX_TRIAL_ID = text(
    "CREATE INDEX IF NOT EXISTS trial_id_idx ON ictrp_rollout(trial_id)")

if __name__ == '__main__':
    # Read public ('config.env') and secret ('.env') configuration file
    config = {}
    config.update(dotenv_values('config.env'))
    config.update(dotenv_values('.env'))
    # Read dataset, prepared on step 2
    selected_df = pd.read_feather(config.get('PATH_DF_SELECTED'))
    # Fix numpy array of int64 type where list of ints is expected
    phases = selected_df['phases'].copy()
    phases[~phases.isna()] = phases[~phases.isna()].apply(lambda x: x.tolist())
    selected_df['phases'] = phases
    logging.info("Cached dataset read.")
    # Upload data to PostgreSQL
    engine = sa.create_engine(config.get('ICTRP_DB_URL'))
    with engine.connect() as db_connection:
        # Clean up everything
        db_connection.execute(SQL_DROP_TABLE)
        logging.info("Existing schema cleaned.")
        # Upload table
        export_df = selected_df  # .head(10)
        export_df.to_sql('ictrp', db_connection, index=False,
                         if_exists='replace',
                         dtype={'phases': ARRAY(INTEGER),
                                'countries': ARRAY(TEXT)})
        db_connection.execute(SQL_ADD_PRIMARY_KEY)  # add primary key
        logging.info("Table created.")
        db_connection.execute(SQL_CREATE_VIEW, na_values)
        db_connection.execute(SQL_CREATE_INDEX_TRIAL_ID)
        logging.info("Materialized View created.")
    logging.info("Data fully loaded to relational database.")
