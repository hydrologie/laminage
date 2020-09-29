from pydsstools.heclib.dss import HecDss
from pydsstools.core import TimeSeriesContainer, UNDEFINED
import os
import pandas as pd
import errno
import shutil


def _csv_to_dss(csv_filename: str,
                output_path: str = None,
                sim_name: str = None,
                start_date: str = "01JAN2001 24:00:00"):
    """
    Converts csv file to equivalent file in hec format

    Parameters
    ----------
    csv_filename : str
        Complete or relative path of csv filename
    output_path : str, default None
        Folder directory to ouput converted hec files
    sim_name : str, default None
        Alternative name of output file
    start_date : str, default "01JAN2001 00:00:00"
        Start date associated with first row of data in csv filename
    Returns
    -------
    None

    """

    if output_path is None:
        output_path = os.path.dirname(csv_filename)

    if sim_name is None:
        sim_name = os.path.splitext(os.path.basename(csv_filename))[0]

    if not os.path.isdir(output_path):
        try:
            os.makedirs(output_path)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

    dss_filename = os.path.join(output_path,
                                sim_name + '.dss')

    # copy empty.dss to dss_filename
    shutil.copy2(os.path.join(os.path.dirname(__file__),
                              'templates',
                              'empty.dss'),
                 dss_filename)

    # Prepare time-series data
    df = pd.read_csv(csv_filename)
    if df.shape[0] > 0:
        while df.shape[0] < 365:
            df = df.append(df.iloc[-1, :])
        df = df.reset_index().drop(columns=['index'])
        df = df.round(decimals=2)

    tsc = TimeSeriesContainer()
    tsc.startDateTime = start_date
    tsc.numberValues = df.shape[0]
    tsc.units = "cms"
    tsc.type = "INST-VAL"
    tsc.interval = 24 * 60
    fid = HecDss.Open(dss_filename)
    # add each column time-series from dataframe to hec
    for column in df:
        pathname = "/{}/{}///1DAY/{}/".format(sim_name, column, sim_name)
        tsc.pathname = pathname
        tsc.values = df[column].values
        fid.deletePathname(tsc.pathname)
        fid.put_ts(tsc)
    fid.close()
