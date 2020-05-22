from pydsstools.heclib.dss import HecDss
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
from time import strptime


def _read_simulation_values(alternative_name: str,
                            reservoir_id: str,
                            variable_type: str,
                            base_dir: str,
                            start_date: str = "01JAN2001 00:00:00",
                            end_date: str = "30JUL2001 00:00:00"):
    """

    Parameters
    ----------
    alternative_name
    reservoir_id
    variable_type
    base_dir
    start_date
    end_date

    Returns
    -------

    """
    base_name = os.path.basename(base_dir)
    # TODO : don't hardcode
    dss_file = os.path.join(base_dir, 'base/Outaouais_long/rss/simulation/simulation.dss')

    pathname = "//{}-POOL/{}//1DAY/{}/".format(reservoir_id,
                                               variable_type,
                                               alternative_name)
    startDate = start_date
    endDate = end_date

    fid = HecDss.Open(dss_file)
    ts = fid.read_ts(pathname, window=(startDate, endDate), trim_missing=True)
    start_date_num = np.datetime64('{}-{:02d}-{}'.format(start_date[5:9],
                                                         strptime(start_date[2:5], '%b').tm_mon, start_date[0:2]))
    end_date_num = np.datetime64('{}-{:02d}-{}'.format(end_date[5:9],
                                                       strptime(end_date[2:5], '%b').tm_mon, end_date[0:2]))
    times = np.arange(start_date_num, end_date_num + np.timedelta64(1,'D'),
                      np.timedelta64(1, 'D'), dtype='datetime64')
    values = ts.values
    fid.close()

    member_id = [int(alternative_name[1:-1]) + int(base_name[1:]) * 100 - 100] * len(values)
    d = {'date': times, 'reservoir_id': [reservoir_id] * len(values), 'member_id': member_id, 'value': values}

    return pd.concat([pd.Series(v, name=k) for k, v in d.items()], axis=1)


def _save_simulation_values(alternative_names: list,
                            variable_type_list: list,
                            reservoir_list: list,
                            base_dir: str,
                            csv_output_path: str):
    """

    Parameters
    ----------
    alternative_names
    variable_type_list
    reservoir_list
    base_dir
    output_path

    Returns
    -------

    """
    alternatives_chunks = [alternative_names[x:x + 10] for x in range(0, len(alternative_names), 10)]
    for alternative_chunk in alternatives_chunks:
        df = pd.concat([_read_simulation_values(alternative_name,
                                                reservoir_id,
                                                variable_type,
                                                base_dir)
                        for alternative_name in alternative_chunk
                        for variable_type in variable_type_list
                        for reservoir_id in reservoir_list])
        df.to_csv(os.path.join(csv_output_path, 'simulations_' + "{:07d}".format(int(df['member_id'].min()))
                               + '_' + "{:07d}".format(int(df['member_id'].max())) + '.csv'),
                  index=False)

