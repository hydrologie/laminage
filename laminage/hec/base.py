import os
import errno
import dask
from dask_jobqueue import SLURMCluster
from dask import compute, persist, delayed
from dask.distributed import Client, progress
import glob

from .alternatives import CreationAlternative as ca
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
        Convert all csv files in directory to dss files using the dask distributed client

        Parameters
        ----------
        csv_directory : str
        client : Client
            Dask client that owns the dask.delayed() objects

        Returns
        -------

        """
        lazy_results = [dask.delayed(_csv_to_dss)(filename, os.path.join(self.project_path,
                                                                         '01_Intrants',
                                                                         'Series_stochastiques',
                                                                         'dss'))
                        for filename in glob.glob(csv_directory)]
        return client.compute(lazy_results)

    @staticmethod
    def create_partial_base(dss_list: list,
                            output_path: str,
                            routing_config: dict):
        """
        Creates a HEC ResSim base from reference base with limited number of dss alternatives (for performance)

        Parameters
        ----------
        dss_list : list
            List of all dss alternatives to add to the current base
        output_path : str
            Output bath where the new base should be created
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

        Returns
        -------

        """
        if not os.path.isdir(output_path):
            try:
                os.makedirs(output_path)
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise
        alternatives_obj_list = [ca(dss_filename,
                                    output_path,
                                    routing_config['type_series'],
                                    routing_config['keys_link'])
                                 for dss_filename in dss_list]

        alternatives_obj_list[0].create_config_from(routing_config['source_config_file'])
        for alt in alternatives_obj_list:
            alt.add_alternative(routing_config['source_ralt_file'])

    def create_distributed_base(self,
                                routing_config: dict,
                                client,
                                output_path: str = None,
                                dss_path: str = None):
        """
        Creates a distributed base to scale HEC ResSim simulations using the dask distributed client

        Parameters
        ----------
        routing_config
        client
        output_path
        dss_path

        Returns
        -------

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

        dss_list = glob.glob(os.path.join(dss_path, '*.dss'))

        chunks = [dss_list[x:x + 100] for x in range(0, len(dss_list), 100)]

        lazy_results = [dask.delayed(self.create_partial_base)(chunk,
                                                               os.path.join(output_path,
                                                                            "b{:06d}".format(idx + 1)),
                                                               routing_config)
                        for idx, chunk in enumerate(chunks)]

        return client.compute(lazy_results)
