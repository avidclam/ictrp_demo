import logging
import re
import pathlib
from dateutil.parser import parse as dateutil_parse
from dotenv import dotenv_values
import pandas as pd

# Patterns
PTN_FULL_EXPORT_GLOB = 'ICTRPFullExport*'
PTN_FULL_EXPORT_DATE = r'ICTRPFullExport-\d+-(.*)\.zip'
PTN_WEEK_GLOB = 'ICTRPWeek*'
PTN_WEEK_DATE = r'ICTRPWeek(\d{1,2})([A-Z][a-z]+)(\d{4})\w?\.zip'
# Local parameters
# IN_NROWS = 100  # None means "All"
IN_NROWS = None  # None means "All"

logging.basicConfig(level=logging.INFO,
                    format='%(relativeCreated)d ms: %(message)s')


def cleanup(series):
    return series.str.strip('"').str.strip()


def series_to_date(series):
    _dt = pd.to_datetime(series, dayfirst=True, infer_datetime_format=True)
    return _dt.dt.date


if __name__ == '__main__':
    config = dotenv_values('config.env')
    data_frames = []
    data_dir = pathlib.Path(config.get('PATH_DIR_DATA'))
    colnames_path = pathlib.Path(config.get('PATH_COLNAMES'))
    colnames = colnames_path.read_text().splitlines()
    # Find the latest full export (base)
    full_export_paths = {}
    for path in data_dir.glob(PTN_FULL_EXPORT_GLOB):
        m = re.match(PTN_FULL_EXPORT_DATE, path.name)
        full_export_date = dateutil_parse(m.group(1), dayfirst=True)
        full_export_paths[path] = full_export_date
    full_export_base = max(full_export_paths,
                           key=lambda x: full_export_paths[x])
    full_export_base_date = full_export_paths[full_export_base]
    logging.info(f"Ready to read base: {full_export_base.name}")
    # Read base data file
    raw_df = pd.read_csv(full_export_base, sep=',', header=0, names=colnames,
                         dtype='string', na_filter=False, memory_map=True,
                         nrows=IN_NROWS)
    data_frames.append(raw_df.apply(cleanup))
    logging.info(f"Read {raw_df.shape[0]} rows from base.")
    # Get the list of weekly update data files
    week_paths = {}
    for path in data_dir.glob(PTN_WEEK_GLOB):
        m = re.match(PTN_WEEK_DATE, path.name)
        week_date = dateutil_parse(' '.join(m.groups()))
        week_paths[path] = week_date
    update_paths = [path for path in
                    sorted(week_paths, key=lambda x: week_paths[x])
                    if week_paths[path] >= full_export_base_date]
    # Read weekly update data files
    for path in update_paths:
        raw_df = pd.read_csv(path, sep=',', header=0, names=colnames,
                             dtype='string', na_filter=False, memory_map=True,
                             nrows=IN_NROWS)
        data_frames.append(raw_df.apply(cleanup))
        logging.info(f"Read {raw_df.shape[0]} rows from {path.name}")
    # Build resulting dataframe
    logging.info("Ready to build resulting dataframe.")
    joint_frames = pd.concat(data_frames)
    result_df = (
        joint_frames
        .assign(ictrp_date=lambda x: series_to_date(x['ictrp_date_str']))
        .sort_values('ictrp_date')
        .groupby('trial_id')
        .tail(1)
        .drop('ictrp_date', axis='columns')
        .reset_index(drop=True)
    )
    # Save for future use
    n_updated = joint_frames.shape[0] - result_df.shape[0]
    n_total = result_df.shape[0]
    logging.info(f"Updated records: {n_updated}, total: {n_total}, saving...")
    result_df.to_feather(config.get('PATH_DF_FULL'))
    logging.info("All done.")
