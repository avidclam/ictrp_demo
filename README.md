# Clinical Trial Activity of Major Pharmaceutical Companies
The purpose of the [demo project](https://demo.avidclam.com) is to provide an example of data analysis on real data from [ICTRP](https://www.who.int/clinical-trials-registry-platform), International Clinical Trials Registry Platform. 

This repository contains Python code used to read, clean, transform, filter, and upload source data into the PostgreSQL database.

Visualization and aggregation is performed by Metabase.

There're are three stages of the ETL process:
- `01_import_data.py` reads ICTRP files downloaded into the source folder, reads the latest Full Export, subsequent update files, and builds raw data frame
- `02_prepare_dataset.ipynb` performs all the cleaning, transformation and classification (sometimes the process required interactivity, thus Jupyter Notebook)
- `03_upload_dataset.py` completes the process with upload

Required but not included into the repository `.env` file contains database URL parameter for the SQLAlchemy's `create_engine()`.

`ICTRP_DB_URL=postgresql+psycopg2://ictrp:<password>@localhost:5432/ictrp`
