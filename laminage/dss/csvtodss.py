from datetime import datetime
from pydsstools.heclib.dss import HecDss
from pydsstools.core import TimeSeriesContainer, UNDEFINED
import os
import pandas as pd
import errno
import shutil


def csv_to_dss(csv_filename: str,
               output_path: str = None,
               nom_sim: str = None):
    """

    :param csv_filename:
    :param nom_sim:
    :param output_path:
    :return:
    """

    if output_path is None:
        output_path = os.path.dirname(csv_filename)

    if nom_sim is None:
        nom_sim = os.path.splitext(os.path.basename(csv_filename))[0]

    if not os.path.isdir(output_path):
        try:
            os.makedirs(output_path)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

    dss_filename = os.path.join(output_path,
                                nom_sim + '.dss')
    print(dss_filename)

    # copy empty.dss to dss_filename
    shutil.copy2(os.path.join(os.path.dirname(__file__),
                 'empty.dss'),
                 dss_filename)

    # Prepare time-series data
    df = pd.read_csv(csv_filename)
    tsc = TimeSeriesContainer()
    tsc.startDateTime = "01JAN2001 00:00:00"
    tsc.numberValues = df.shape[0]
    tsc.units = "cfs"
    tsc.type = "INST"
    tsc.interval = 1 * 60
    fid = HecDss.Open(dss_filename)
    # add each column time-series from dataframe to dss
    for column in df:
        pathname = "/{}/{}///1DAY/{}/".format(nom_sim, column, nom_sim)
        tsc.pathname = pathname
        tsc.values = df[column].values
        fid.deletePathname(tsc.pathname)
        fid.put_ts(tsc)
    fid.close()
