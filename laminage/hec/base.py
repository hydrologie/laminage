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
import panel as pn
import hvplot.pandas

from .alternatives import CreationAlternative as ca
from .simulations import _read_dss_values, _save_simulation_values
from .csvtodss import _csv_to_dss
from .verifications import *
from .alternatives import CreationAlternative


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
                 model_base_folder: str,
                 routing_config: dict = None
                 ):
        """

        Parameters
        ----------
        model_base_folder : str
            Complete or relative path model base
        project_path : str, default None
            Project directory

        """

        if not os.path.isdir(project_path):
            try:
                os.makedirs(project_path)
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise
        self.project_path: str = project_path

        verify_base_folder_exist(model_base_folder)
        self.model_base_folder: str = model_base_folder

        _routing_config = {}

        source_config_file = os.path.join(directory_find('rss', root=model_base_folder), 'rss.conf')
        # TODO : verify source_config_file
        _routing_config['source_config_file'] = source_config_file

        # TODO : automatically figure out if PMF, frequency analysis or stochastical analysis
        _routing_config['type_series'] = 'STO'

        # variables to extract after post-processing
        _routing_config['variable_type_list'] = ['FLOW-IN', 'FLOW-OUT', 'ELEV']

        rsys_list = glob.glob(os.path.join(model_base_folder, '*', '*', '*', '*.rsys'))
        # TODO : verify that only one .rsys exist. Ask user to either delete or directly specify the file
        # for now, we assume there is only one file  :
        rsys_filename = rsys_list[0]
        _routing_config['rsys_filename'] = rsys_filename

        _routing_config['dss_capacity_filename'] = os.path.splitext(rsys_filename)[0] + '.dss'

        if routing_config is None:
            routing_config = _routing_config
        else:
            routing_config = {**routing_config, **_routing_config}
        self.routing_config = routing_config

    def csv_to_dss(self,
                   csv_directory: str,
                   client,
                   dss_directory: str = None,
                   limit_nb_csv: int = None):
        """
        Convert all csv files in directory to dss files using the dask distributed client for parallel processing

        Parameters
        ----------
        csv_directory : str
            Folder where all .csv alternatives files are held
        client : Client
            Dask client that owns the dask.delayed() objects
        dss_directory : str
            Folder where all .dss alternatives files are held
        limit_nb_csv : float

        Returns
        -------
        List of Futures

        """
        if dss_directory is None:
            dss_directory = os.path.join(os.path.dirname(os.path.dirname(csv_directory)),
                                         'dss')
        if not os.path.isdir(dss_directory):
            try:
                os.makedirs(dss_directory)
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise

        lazy_results = [dask.delayed(_csv_to_dss)(csv_filename=filename,
                                                  output_path=dss_directory,
                                                  start_date=self.routing_config['lookup_date'])
                        for filename in sorted(glob.glob(csv_directory))[:limit_nb_csv]]
        print('Dss files stored in path : {}'.format(dss_directory))
        return client.compute(lazy_results)

    def create_alternatives(self,
                            dss_filename_list):
        """

        """
        rss_directory = directory_find('rss', root=os.path.dirname(os.path.dirname(dss_filename_list[0])))

        for idx, alt_filename in enumerate(dss_filename_list):
            alt = CreationAlternative(dss_filename=alt_filename,
                                      output_path=rss_directory,
                                      type_series=self.routing_config['type_series'],
                                      keys_link=self.routing_config['keys_link'],
                                      dss_capacity_filename=self.routing_config['dss_capacity_filename'],
                                      rsys_filename=self.routing_config['rsys_filename'])

            if idx == 0:
                alt.create_config_from(self.routing_config['source_config_file'])

            alt.add_alternative(dataframe=self.create_connectivity_table(self.routing_config['dss_capacity_filename'],
                                                                         self.routing_config['rsys_filename']),
                                dataframe_initial_conditions=pd.DataFrame(self.routing_config['level_init_conditions']
                                                                          .items(), columns=['name', 'value']))

    def update_storage(self,
                       client,
                       filepath: str = None,
                       df: pd.DataFrame = None,
                       part_b="POOL-AREA CAPACITY",
                       part_c="ELEV-STOR-AREA"
                       ):
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

        # verify that all provided reservoir names exist in the model
        verify_reservoir_names_exist(df_model=self.read_storage(),
                                     df_provided=df)
        # verify that all series are monotonic
        verify_monotonic_df(df_provided=df)

        dss_simulation_files = glob.glob(os.path.join(self.model_base_folder, 'base', '*', 'rss', '*', 'rss',
                                                      '*' + os.path.basename(self.routing_config['dss_capacity_filename'])))

        dss_simulation_files.append(self.routing_config['dss_capacity_filename'])

        lazy_results = [dask.delayed(self._update_storage)(filename, df, part_b, part_c)
                        for filename in dss_simulation_files]
        return client.compute(lazy_results)

    @staticmethod
    def _update_storage(dss_filename: str,
                        df: pd.DataFrame = None,
                        part_b="POOL-AREA CAPACITY",
                        part_c="ELEV-STOR-AREA"
                        ):
        """

        """

        with HecDss.Open(dss_filename) as fid:

            variable_types = df.columns.get_level_values(1).unique()  # (Elevation, Storage)

            for watershed, variable_type in df:
                if variable_type == variable_types[1]:
                    pdc = PairedDataContainer()
                    size = df[watershed].iloc[:, 1].dropna().values.shape[0]

                    pdc.pathname = '/%s/%s/%s////' % (watershed, part_b, part_c)
                    pdc.curve_no = 2
                    pdc.data_no = size

                    pdc.independent_units = 'm'
                    pdc.independent_type = 'Elev'
                    pdc.independent_axis = df[watershed].iloc[:, 0].dropna().values

                    pdc.labels_list = ["0", "1"]
                    pdc.dependent_units = 'undef'
                    pdc.dependent_type = 'undef'
                    pdc.curves = np.vstack((df[watershed].iloc[:, 1].dropna().values.T,
                                            -np.inf * np.ones(shape=size))).astype(dtype=np.float32)
                    fid.put_pd(pdc)

    def read_storage(self,
                     part_b="POOL-AREA CAPACITY",
                     part_c="ELEV-STOR-AREA"):

        pathname_pattern = '/*/%s/%s////' % (part_b, part_c)

        with HecDss.Open(self.routing_config['dss_capacity_filename']) as fid:
            path_list = fid.getPathnameList(pathname_pattern, sort=1)

        return pd.concat([self._read_pd_storage(pathname,
                                                self.routing_config['dss_capacity_filename'])
                          for pathname in path_list], axis=1)

    @staticmethod
    def _read_pd_storage(pathname, dss_filename):
        """

        """

        with HecDss.Open(dss_filename) as fid:
            reservoir = pathname.split('/')[1]
            df = fid.read_pd(pathname).reset_index().iloc[:, 0:2]
            df.columns = pd.MultiIndex.from_tuples(zip([reservoir, reservoir], ['Elevation', 'Volume']))
        return df

    def update_capacity(self,
                        client,
                        filepath: str = None,
                        df: pd.DataFrame = None,
                        part_c="ELEV-FLOW"):
        """

        """

        if df is not None:  # priority to the passed dataframe
            filepath = None

        if filepath is not None:
            filepath = Path(filepath)
            if not filepath.is_file():
                raise FileNotFoundError(
                    errno.ENOENT, os.strerror(errno.ENOENT), filepath)
            df = pd.read_csv(filepath, header=[0, 1, 2])

        # verify that all provided reservoir names exist in the model
        verify_reservoir_names_exist(df_model=self.read_capacity(),
                                     df_provided=df)

        dss_simulation_files = glob.glob(os.path.join(self.model_base_folder, 'base', '*', 'rss', '*', 'rss',
                                                      '*' + os.path.basename(self.routing_config['dss_capacity_filename'])))

        dss_simulation_files.append(self.routing_config['dss_capacity_filename'])
        lazy_results = [dask.delayed(self._update_capacity)(filename, df.astype(float).round(3), part_c)
                        for filename in dss_simulation_files]
        return client.compute(lazy_results)

    @staticmethod
    def _update_capacity(dss_filename: str,
                         df: pd.DataFrame = None,
                         part_c="ELEV-FLOW"):
        """

        """
        # Dataframe to dss network (Storage)

        with HecDss.Open(dss_filename) as fid:

            variable_types = df.columns.get_level_values(1).unique()  # (Elevation, Storage)
            watersheds = df.columns.get_level_values(0).unique()
            for watershed in watersheds:
                for outlet_type in df[watershed].columns.get_level_values(0).unique():

                    pdc = PairedDataContainer()
                    size = df[watershed][outlet_type].iloc[:, 1].dropna().values.shape[0]

                    pdc.pathname = '/%s/%s/%s////' % (watershed, outlet_type, part_c)
                    pdc.curve_no = 1
                    pdc.data_no = size

                    pdc.independent_units = 'm'
                    pdc.independent_type = 'Elev'
                    pdc.independent_axis = df[watershed][outlet_type].iloc[:, 0].dropna().values

                    pdc.labels_list = [""]
                    pdc.dependent_units = 'cms'
                    pdc.dependent_type = 'Flow'
                    pdc.curves = df[watershed][outlet_type].iloc[:, 1].dropna().values.reshape(-1, 1).T.astype(dtype=np.float32)
                    fid.put_pd(pdc)

    def read_capacity(self,
                      part_c="ELEV-FLOW"):
        """

        """
        pathname_pattern = '/*/*/%s////' % part_c

        with HecDss.Open(self.routing_config['dss_capacity_filename']) as fid:
            path_list = fid.getPathnameList(pathname_pattern, sort=1)

        return pd.concat([self._read_capacity(pathname,
                                              self.routing_config['dss_capacity_filename'])
                          for pathname in path_list], axis=1)

    @staticmethod
    def _read_capacity(pathname, dss_filename):
        """

        """

        with HecDss.Open(dss_filename) as fid:
            reservoir = pathname.split('/')[1]
            outlet_name = pathname.split('/')[2]
            df = fid.read_pd(pathname).reset_index().iloc[:, 0:2]
            df.columns = pd.MultiIndex.from_tuples(zip([reservoir, reservoir],
                                                       [outlet_name, outlet_name],
                                                       ['Elevation', 'Flow']))
        return df

    def plot_capacity(self):
        """

        """
        df_capacity = self.read_capacity()

        list_reservoirs = df_capacity \
            .columns \
            .get_level_values(0) \
            .unique() \
            .to_list()
        reservoir_widget = pn.widgets.Select(name='reservoir',
                                             value=list_reservoirs[0],
                                             options=list_reservoirs)

        list_outlet = df_capacity[list_reservoirs[0]] \
            .columns.get_level_values(0) \
            .unique() \
            .to_list()
        outlet_widget = pn.widgets.Select(name='outlet',
                                          value=list_outlet[0],
                                          options=list_outlet)

        plot_fig = \
            df_capacity[reservoir_widget.value][outlet_widget.value] \
            .hvplot(x='Elevation',
                    title=reservoir_widget,
                    width=600,
                    grid=True)
        plot_table = \
            df_capacity[reservoir_widget.value][outlet_widget.value] \
            .dropna(how='all') \
            .hvplot.table(x='Elevation',
                          width=300)

        layout = pn.Column(reservoir_widget, outlet_widget, pn.Row(plot_fig, plot_table))

        def update(event):
            layout[1].options = df_capacity[reservoir_widget.value] \
                .columns \
                .get_level_values(0) \
                .unique() \
                .to_list()
            layout[2][0] = \
                df_capacity[reservoir_widget.value][outlet_widget.value] \
                .hvplot(x='Elevation',
                        title=reservoir_widget,
                        width=600,
                        grid=True)
            layout[2][1] = \
                df_capacity[reservoir_widget.value][outlet_widget.value] \
                .dropna(how='all') \
                .hvplot.table(x='Elevation',
                              width=300)

        reservoir_widget.param.watch(update, 'value')
        outlet_widget.param.watch(update, 'value')

        return layout

    def create_connectivity_table(self,
                                  dss_filename,
                                  rsys_filename):
        """

        """

        matches = ['STR=' + i.split('-RATING')[0] for i in
                   list(self.read_capacity() \
                        .columns \
                        .get_level_values(1) \
                        .unique()) + ['POOL']]

        d = get_line_numbers_reservoir_subelement(rsys_filename, matches)
        df = pd.DataFrame(d)
        df.columns = ['line_number', 'name', 'object_parent_id', 'object_id', 'type']
        return df

    def run_partial_base(self,
                         dss_list: list,
                         output_path: str,
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

        if csv_output_path is None:
            csv_output_path = os.path.join(self.project_path,
                                           '02_Calculs',
                                           'Laminage_STO',
                                           '03_Resultats')

        copytree(self.model_base_folder,
                 output_path,
                 dirs_exist_ok=True)

        result = list(Path(output_path).rglob("study"))
        complete_output_path = os.path.realpath(str(result[0]).split('study')[0])

        [os.remove(f) for f in glob.glob(os.path.join(complete_output_path, 'shared', '*.dss'))]

        # Add .dss files to shared
        [shutil.copy2(dss_filename, os.path.join(complete_output_path, 'shared', os.path.basename(dss_filename)))
         for dss_filename in dss_list]

        # Create alternatives
        self.create_alternatives(glob.glob(os.path.join(complete_output_path, 'shared','*.dss')))

        # Pass alternatives list to compute
        with open(os.path.join(complete_output_path,
                               'alternatives.txt'), 'w') as text_file:

            #TODO : Verify that dates are available
            print(self.routing_config['lookup_date'], file=text_file)
            print(self.routing_config['start_date'], file=text_file)
            print(self.routing_config['end_date'], file=text_file)

            alternative_list = [CreationAlternative.get_alternative_name(
                dss_name=os.path.splitext(os.path.basename(dss_filename))[0],
                type_series=self.routing_config['type_series'])
                for dss_filename in dss_list]
            for i, line in enumerate(alternative_list):
                print(line, file=text_file)

        # Run all alternatives in simulation for specific base
        output_path_windows = ('C:' + complete_output_path.split('drive_c')[1]).replace('/', '\\\\')
        self._run_sim(output_path_windows)

        _save_simulation_values(alternative_names=alternative_list,
                                variable_type_list=self.routing_config['variable_type_list'],
                                reservoir_list=list(self.read_capacity().columns.get_level_values(0).unique()),
                                base_dir=output_path,
                                csv_output_path=csv_output_path,
                                start_date=self.routing_config['start_date'],
                                end_date=self.routing_config['end_date'])

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
                                    client,
                                    dss_path,
                                    output_path: str = None,
                                    csv_output_path: str = None,
                                    n=100):
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
        n : int
            Number of alternatives per simulation

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

        # if dss_path is None:
        #     dss_path = os.path.join(self.project_path,
        #                             '01_Intrants',
        #                             'Series_stochastiques',
        #                             'dss')
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

        lazy_results = [dask.delayed(self.run_partial_base)(chunk,
                                                            os.path.join(output_path,
                                                                         "b{:06d}".format(idx + 1)),
                                                            csv_output_path)
                        for idx, chunk in enumerate(chunks(dss_list, n))]

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


def get_line_numbers_reservoir_element(filename):
    numbers = []
    lookup = 'hec.rss.model.ReservoirElement'
    with open(filename, encoding='latin-1') as myFile:
        for num, line in enumerate(myFile, 1):
            if lookup in line:
                numbers.append(num)
        return numbers


def get_reservoir_element_names(filename, numbers, diff_num):
    names = []
    with open(filename, encoding='latin-1') as myFile:
        for num, line in enumerate(myFile, 1):
            if (num - diff_num) in numbers:
                names.append(line.split('=')[-1].split('\n')[0])
        return names


def get_line_numbers_reservoir_subelement(filename, matches):
    numbers = []
    lines = []
    with open(filename, encoding='latin-1') as myFile:
        for num, line in enumerate(myFile, 1):
            if any(x in line.upper() for x in matches):
                numbers.append(num)
                lines.append(line)
    with open(filename, encoding='latin-1') as f:
        text = f.readlines()
    idx = ['{=hec.rss.model.Element' in x for x in (np.array(text)[tuple([np.array(list(numbers)) - 4])])]
    final_numbers = np.array(list(numbers))[tuple([idx])]
    final_index = [i.split('=')[-1].split('\n')[0] for i in np.array(list(text))[tuple([final_numbers + 3])]]
    final_names = [i.split('=')[-1].split('\n')[0] for i in np.array(list(lines))[tuple([idx])]]

    numbers = get_line_numbers_reservoir_element(filename)
    names = get_reservoir_element_names(filename, numbers, 3)
    ids = get_reservoir_element_names(filename, numbers, 7)
    reservoirs_index = dict(zip(names, numbers))

    arr_list = []
    ids_list = []
    for idx, value in enumerate(final_numbers):
        array = (value - np.array(list(reservoirs_index.values())))
        array[array < 0] = 100000
        index = np.argmin(array)
        bv_name = np.array(list(reservoirs_index.keys()))[[index]][0]
        arr_list.append(bv_name)
        ids_list.append(ids[names.index(bv_name)])

    return list(zip(final_numbers, arr_list, ids_list, final_index, final_names))


def directory_find(atom, root='.'):
    for path, dirs, files in os.walk(root):
        if atom in dirs:
            return os.path.join(path, atom)


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]