import os
import errno
import dask
from shutil import copytree
from dask import compute, persist, delayed
from dask.distributed import Client, progress
import numpy as np
import glob
import shutil
from distutils.dir_util import copy_tree
import subprocess
from pathlib import Path
from send2trash import send2trash
import pandas as pd
from pydsstools.heclib.dss import HecDss
from pydsstools.core import PairedDataContainer

from .alternatives import CreationAlternative as ca
from .simulations import _read_dss_values, _save_simulation_values
from .csvtodss import _csv_to_dss


class BaseManager:
    """
    Handles all required steps to create a distributed
    HEC ResSim base

    Attributes
    ----------
    model_base_folder : str
        Complete or relative path model base
    project_path : str, default None
        Project directory

    Examples
        --------
        from dask.distributed import Client, progress
        Constructing CreationAlternative object.
        >> project_path = 'PATH TO PROJECT DIRECTORY'
        >> csv_directory = 'PATH TO CSV DIRECTORY'
        >> source_config_file = 'PATH TO reference HEC ResSim rss.conf file'
        >> type_series = 'STO'
        >> ralt_file = 'PATH TO RALT FILE'
        >> nom_BV_hec=['Inflow Mitchinamecus','Inflow Kiamika','Inflow Mont-Laurier',
            'Inflow Cedar','Inflow High Falls','Inflow Petite Nation','Inflow Masson',
            'Inflow Rideau','Inflow South Nation','Inflow Cabonga','Inflow Baskatong',
            'Inflow Paugan','Inflow Arnprior','Inflow Mountain Chute','Inflow Kamaniskeg',
            'Inflow Bark Lake','Inflow Dumoine','Inflow Mattawa',
            'Inflow Joachims','Inflow Otto Holden','Inflow Kipawa','Inflow Dozois',
            'Inflow Victoria','Inflow Rapide 7','Inflow Rapide 2','Inflow Rabbit Lake',
            'Inflow Lower Notch','Inflow Lady Evelyn','Inflow Mistinikon','Inflow Blanche',
            'Inflow Temiscamingue','Inflow Maniwaki','Inflow Chelsea','Inflow Carillon',
            'Inflow Kinojevis','Inflow des Quinze (Anglier)','Inflow Petawawa','Inflow Chenaux et Noire',
            'Inflow Coulonge','Inflow Bonnechere','Inflow Chat Falls','Inflow Mississippi',
            'Inflow Rouge']
        >> nom_BV_dss = ['MITCHINAMECUS','KIAMIKA','MONT-LAURIER','LAC DU POISSON BLANC',
            'HIGH FALLS','RIVIERE PETITE NATION','MASSON','RIVIERE RIDEAU',
            'RIVIERE SOUTH NATION','CABONGA','BASKATONG','PAUGAN',
            'MADAWASKA-ARNPRIOR','MOUNTAIN CHUTE','KAMANISKEG','BARK LAKE',
            'RIVIERE DUMOINE','RIVIERE MATTAWA','DES JOACHIMS','OTTO HOLDEN',
            'KIPAWA','DOZOIS','LAC VICTORIA ET LAC GRANET','RAPIDE 7',
            'RAPIDE 2','RABBIT LAKE','LOWER NOTCH ET INDIAN CHUTE','LADY EVELYN',
            'MISTINIKON','RIVIERE BLANCHE','LAC TEMISCAMINGUE A ANGLIERS','MANIWAKI',
            'CHELSEA','CARILLON ET HULL','RIVIERE KINOJEVIS','LAC DES QUINZE',
            'RIVIERE PETAWAWA','CHENAUX ET NOIRE','RIVIERE COULONGE','RIVIERE BONNECHERE',
            'CHUTE-DES-CHATS','RIVIERE MISSISSIPPI','RIVIERE ROUGE']
        >> keys_link = dict(zip(nom_BV_hec, nom_BV_dss))
        >> routing_config = {'type_series':type_series,
                             'keys_link':keys_link,
                             'source_ralt_file':ralt_file,
                             'source_config_file':source_config_file}
        >> bm = lm.BaseManager(project_path=project_path)

        >> results = bm.csv_to_dss(csv_directory=csv_directory,
                        client=client)
           progress(results)
        >> results = bm.create_bases(routing_config=routing_config,
                          client=client)
           progress(results)
        """

    def __init__(self,
                 project_path: str,
                 model_base_folder: str = None,
                 ):
        """

        Parameters
        ----------
        model_base_folder : str
            Complete or relative path model base
        project_path : str, default None
            Project directory

        """
        self.project_path: str = project_path
        if not os.path.isdir(self.project_path):
            try:
                os.makedirs(self.project_path)
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise

        if model_base_folder is None:
            model_base_folder = os.path.join(self.project_path,
                                             '02_Calculs',
                                             'Laminage_STO',
                                             '01_Modele_ResSIM_reference')

        self.model_base_folder: str = model_base_folder

    def csv_to_dss(self,
                   csv_directory: str,
                   client):
        """
        Convert all csv files in directory to dss files using the dask distributed client for parallel processing

        Parameters
        ----------
        csv_directory : str
            Folder where all .csv alternatives files are held
        client : Client
            Dask client that owns the dask.delayed() objects

        Returns
        -------
        List of Futures

        """
        lazy_results = [dask.delayed(_csv_to_dss)(filename, os.path.join(self.project_path,
                                                                         '01_Intrants',
                                                                         'Series_stochastiques',
                                                                         'dss'))
                        for filename in glob.glob(csv_directory)]
        return client.compute(lazy_results)

    def update_storage(self,
                       dss_network_filename: str,
                       client,
                       filepath: str = None,
                       df: pd.DataFrame = None):
        """

        """

        if df is not None:  # priority to the passed dataframe
            filepath = None

        if filepath is not None:
            filepath = Path(filepath)
            if not filepath.is_file():
                raise FileNotFoundError(
                    errno.ENOENT, os.strerror(errno.ENOENT), filepath)
            df = pd.read_csv(filepath, header=[0, 1])

        # verify that all series are monotonic
        not_monotonic_series_names = verify_monotonic_values(df)
        if not_monotonic_series_names.any():
            raise ValueError('Reservoirs {} values are not monotonic.'.format(', '.join(not_monotonic_series_names)) +
                             ' Please validate your input files.')

        dss_simulation_files = glob.glob(os.path.join(self.model_base_folder, 'base', '*', 'rss', '*', 'rss',
                                                      '*' + os.path.basename(dss_network_filename)))

        dss_simulation_files.append(dss_network_filename)
        print(dss_simulation_files)

        lazy_results = [dask.delayed(self._update_storage)(filename, df)
                        for filename in dss_simulation_files]
        return client.compute(lazy_results)

    @staticmethod
    def _update_storage(dss_filename: str,
                        df: pd.DataFrame = None):
        """

        """
        # Dataframe to dss network (Storage)
        param = "ELEV-STOR-AREA"
        variable = 'POOL-AREA CAPACITY'

        fid = HecDss.Open(dss_filename)

        variable_types = df.columns.get_level_values(1).unique()  # (Elevation, Storage)

        for watershed, variable_type in df:
            if variable_type == variable_types[1]:
                pdc = PairedDataContainer()
                size = df[watershed].iloc[:, 1].dropna().values.shape[0]

                pdc.pathname = '/%s/%s/%s////' % (watershed, variable, param)
                pdc.curve_no = 2
                pdc.data_no = size

                pdc.independent_units = 'm'
                pdc.independent_type = 'Elev'
                pdc.independent_axis = df[watershed].iloc[:, 0].dropna().values

                pdc.labels_list = ["0", "1"]
                pdc.dependent_units = 'undef'
                pdc.dependent_type = 'undef'
                pdc.curves = np.vstack((df[watershed].iloc[:, 1].dropna().values.T,
                                        -np.inf * np.ones(shape=(size)))).astype(dtype=np.float32)
                fid.put_pd(pdc)
        fid.close()

    def read_storage(self, reservoir_list, dss_filename):
        return pd.concat([self._read_pd_storage(reservoir, dss_filename) for reservoir in reservoir_list], axis=1)

    @staticmethod
    def _read_pd_storage(reservoir, dss_filename):
        """

        """
        param = "ELEV-STOR-AREA"
        variable = 'POOL-AREA CAPACITY'
        with HecDss.Open(dss_filename) as fid:
            pathname = '/%s/%s/%s////' % (reservoir, variable, param)
            df = fid.read_pd(pathname).reset_index().iloc[:, 0:2]
            df.columns = pd.MultiIndex.from_tuples(zip([reservoir, reservoir], ['Elevation', 'Volume']))
        return df

    def run_partial_base(self,
                         dss_list: list,
                         output_path: str,
                         routing_config: dict,
                         csv_output_path: str = None):
        """
        Creates a HEC ResSim base from reference base with limited number of dss alternatives (for performance)

        Parameters
        ----------
        dss_list : list
            List of all dss alternatives to add to the current base
        output_path : str
            Output path where the new base should be created
        routing_config : dict
            Dictionary should contain the following keys:
                type_series : str
                    Options available : FREQ (frequential analysis study),
                                        PMF (probable maximum flood study),
                                        HIST (historical time-series study),
                                        STO (stochastical analysis study)
                keys_link : dict
                    Dictionary to link dss inflows with Hec ResSim's nomenclature
                    Keys correspond to inflow names in Hec ResSim's model
                    while values correspond to dss inflow names
                source_ralt_file : str
                    Path of a reference HEC ResSim model .ralt file
                source_config_file : str
                    Path of the reference HEC ResSim model rss.conf file
        csv_output_path : str
            Directory to store csv results in

        """
        if not os.path.isdir(output_path):
            try:
                os.makedirs(output_path)
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise

        copytree(self.model_base_folder,
                 output_path,
                 dirs_exist_ok=True)

        result = list(Path(output_path).rglob("study"))
        complete_output_path = os.path.realpath(str(result[0]).split('study')[0])

        [os.remove(f) for f in glob.glob(os.path.join(complete_output_path, 'shared', '*.dss'))]

        # Add .dss files to shared and renumber from 000001 to match reference HEC ResSim base
        min_sim_number = int(os.path.basename(dss_list[0]).split('.')[0]) - 1
        [shutil.copy2(dss_filename, os.path.join(complete_output_path, 'shared',
                                                 "{:07d}".format(int(os.path.basename(dss_filename).split('.')[
                                                                         0]) - min_sim_number) + '.dss'))
         for dss_filename in dss_list]

        # TODO : update simulation.dss with all dss in shared
        dss_filename_output = os.path.join(output_path, 'base/Outaouais_long/rss/simulation/simulation.dss')
        shutil.copy2(os.path.join(os.path.dirname(__file__),
                    'templates',
                    'empty.dss'),
                     dss_filename_output)
        alternative_names = ["%07d.dss" % (i,) for i in range(1, 101)]
        [_read_dss_values(alternative_basename=alternative_name,
                          reservoir_id=nom_BV,
                          base_dir=output_path,
                          start_date="01JAN2001 24:00:00",
                          end_date="31DEC2001 24:00:00")
         for nom_BV in [*routing_config['keys_link'].values()]
         for alternative_name in alternative_names]

        # Run all alternatives in simulation for specific base
        output_path_windows = ('C:' + complete_output_path.split('drive_c')[1]).replace('/', '\\\\')
        self._run_sim(output_path_windows)

        alternative_names = ['M' + "%09d0" % (i,) for i in range(1, 101)]
        _save_simulation_values(alternative_names=alternative_names,
                                variable_type_list=routing_config['variable_type_list'],
                                reservoir_list=routing_config['reservoir_list'],
                                base_dir=output_path,
                                csv_output_path=csv_output_path)

        # shutil.rmtree(output_path, ignore_errors=True)
        send2trash(output_path)
        os.system('rm -rf ~/.local/share/Trash/*')

    def _run_sim(self,
                 base_path: str,
                 hec_res_sim_exe_path: str = None):
        """

        Parameters
        ----------
        base_path : str
        hec_res_sim_exe_path : str, default None

        Returns
        -------

        """

        if hec_res_sim_exe_path is None:
            hec_res_sim_exe_path = os.path.join(os.environ['HOME'],
                                                '.wine/drive_c/Program Files/HEC/HEC-ResSim/3.1/HEC-ResSim.exe')

        try:
            Path(hec_res_sim_exe_path).resolve(strict=True)
        except FileNotFoundError:
            print('HEC-ResSim.exe not found automatically. Please provide the hec_res_sim_path argument')
        else:
            shutil.copy2(os.path.join(os.path.dirname(__file__), 'templates', 'run_sim.py'),
                         os.path.join(self.project_path, '02_Calculs', '01_Programmes'))

            script_path = ('C:' + os.path.join(self.project_path, '02_Calculs',
                                               '01_Programmes', 'run_sim.py').split('drive_c')[1]).replace('/', '\\\\')

            command = "wine '%s' %s %s" % (hec_res_sim_exe_path, script_path, base_path)

            subprocess.call(command, shell=True)

    def run_distributed_simulations(self,
                                    routing_config: dict,
                                    client,
                                    output_path: str = None,
                                    dss_path: str = None,
                                    csv_output_path: str = None):
        """
        Creates a distributed base to scale HEC ResSim simulations using the dask distributed client

        Parameters
        ----------
        routing_config : dict
            Dictionary should contain the following keys:
                type_series : str
                    Options available : FREQ (frequential analysis study),
                                        PMF (probable maximum flood study),
                                        HIST (historical time-series study),
                                        STO (stochastical analysis study)
                keys_link : dict
                    Dictionary to link dss inflows with Hec ResSim's nomenclature
                    Keys correspond to inflow names in Hec ResSim's model
                    while values correspond to dss inflow names
                source_ralt_file : str
                    Path of a reference HEC ResSim model .ralt file
                source_config_file : str
                    Path of the reference HEC ResSim model rss.conf file
        client : Client
            Dask client that owns the dask.delayed() objects
        output_path : str, default None
            Directory where to create distributed base
        dss_path : str, default None
            Directory where all .dss alternatives are held
        csv_output_path : str
            Directory to store csv results in

        Returns
        -------
        List of Futures
        """
        if output_path is None:
            output_path = os.path.join(self.project_path,
                                       '02_Calculs',
                                       'Laminage_STO',
                                       '02_Bases')
            if not os.path.isdir(output_path):
                try:
                    os.makedirs(output_path)
                except OSError as e:
                    if e.errno != errno.EEXIST:
                        raise

        if dss_path is None:
            dss_path = os.path.join(self.project_path,
                                    '01_Intrants',
                                    'Series_stochastiques',
                                    'dss')
            if not os.path.isdir(dss_path):
                try:
                    os.makedirs(dss_path)
                except OSError as e:
                    if e.errno != errno.EEXIST:
                        raise
        if csv_output_path is None:
            csv_output_path = os.path.join(self.project_path,
                                           '02_Calculs',
                                           'Laminage_STO',
                                           '03_Resultats')
        if not os.path.isdir(csv_output_path):
            try:
                os.makedirs(csv_output_path)
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise

        dss_list = sorted(glob.glob(os.path.join(dss_path, '*.dss')))

        chunks = [dss_list[x:x + 100] for x in range(0, len(dss_list), 100)]

        lazy_results = [dask.delayed(self.run_partial_base)(chunk,
                                                            os.path.join(output_path,
                                                                         "b{:06d}".format(idx + 1)),
                                                            routing_config,
                                                            csv_output_path)
                        for idx, chunk in enumerate(chunks)]

        return client.compute(lazy_results)

    def run_distributed_simulations_ext(self,
                                       routing_config: dict,
                                       client,
                                       output_path: str = None,
                                       dss_path: str = None):
        """
        Creates a distributed base to scale HEC ResSim simulations using the dask distributed client

        Parameters
        ----------
        routing_config : dict
            Dictionary should contain the following keys:
                type_series : str
                    Options available : FREQ (frequential analysis study),
                                        PMF (probable maximum flood study),
                                        HIST (historical time-series study),
                                        STO (stochastical analysis study)
                keys_link : dict
                    Dictionary to link dss inflows with Hec ResSim's nomenclature
                    Keys correspond to inflow names in Hec ResSim's model
                    while values correspond to dss inflow names
                source_ralt_file : str
                    Path of a reference HEC ResSim model .ralt file
                source_config_file : str
                    Path of the reference HEC ResSim model rss.conf file
        client : Client
            Dask client that owns the dask.delayed() objects
        output_path : str, default None
            Directory where to create distributed base
        dss_path : str, default None
            Directory where all .dss alternatives are held

        Returns
        -------
        List of Futures
        """
        if output_path is None:
            output_path = os.path.join(self.project_path,
                                       '02_Calculs',
                                       'Laminage_STO',
                                       '02_Bases')
            if not os.path.isdir(output_path):
                try:
                    os.makedirs(output_path)
                except OSError as e:
                    if e.errno != errno.EEXIST:
                        raise

        if dss_path is None:
            dss_path = os.path.join(self.project_path,
                                    '01_Intrants',
                                    'Series_stochastiques',
                                    'dss')
            if not os.path.isdir(dss_path):
                try:
                    os.makedirs(dss_path)
                except OSError as e:
                    if e.errno != errno.EEXIST:
                        raise

        dss_list = sorted(glob.glob(os.path.join(dss_path, '*.dss')))

        chunks = [dss_list[x:x + 100] for x in range(0, len(dss_list), 100)]
        chunks = [dss_list[0:100]]

        lazy_results = [dask.delayed(self.run_partial_base)(chunk,
                                                            os.path.join(output_path,
                                                                         "b{:06d}".format(idx + 1)),
                                                            routing_config)
                        for idx, chunk in enumerate(chunks)]

        return lazy_results


def verify_monotonic_values(df):
    df_is_monotonic = df.apply(lambda serie: serie.dropna().is_monotonic)
    return df_is_monotonic.loc[df_is_monotonic == False].index.get_level_values(0)