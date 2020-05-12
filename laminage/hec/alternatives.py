import os
import errno


class CreationAlternative:
    """
    Handles all required steps to create an alternative
    in order to compute simulations in HEC ResSim

    Attributes
    ----------
    dss_filename : str
        Complete or relative path of dss filename
    output_path : str
        Folder directory to output alternatives files
    dss_name : str
        Specific dss name of dss filename
    type_series : str
        Options available : FREQ (frequential analysis study),
                            PMF (probable maximum flood study),
                            HIST (historical time-series study),
                            STO (stochastical analysis study)
    keys_link : dict
        Dictionary to link dss inflows with Hec ResSim's nomenclature
        Keys correspond to inflow names in Hec ResSim's model
        while values correspond to dss inflow names
    alternative_name : str
        Equivalent alternative name in HEC ResSim nomenclature (10 caracters)

    Examples
        --------
        Constructing CreationAlternative object.
        >> dss_filename = '0000001.dss'
        >> output_path = 'output_path'
        >> type_series = 'STO'
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
        >> alt = CreationAlternative(dss_filename=dss_filename,
                          output_path=output_path,
                          type_series=type_series,
                          keys_link=keys_link)
        """

    def __init__(self,
                 dss_filename: str,
                 output_path: str = None,
                 type_series: str = None,
                 keys_link: dict = None,
                 ):
        """

        Parameters
        ----------
        dss_filename : str
            Complete or relative path of dss filename
        output_path : str, default None
            Folder directory to output alternatives files
        type_series : str, default None
            Options available : FREQ (frequential analysis study),
                                CMP (maximum probable flood study),
                                HIST (historical time-series study),
                                STO (stochastic analysis study)
        keys_link : dict, default {}
            Dictionary to link dss inflows with Hec ResSim's nomenclature
            Keys correspond to inflow names in Hec ResSim's model
            while values correspond to dss inflow names

        """
        if keys_link is None:
            keys_link = {}

        self.dss_filename: str = dss_filename
        self.dss_name: str = os.path.splitext(os.path.basename(self.dss_filename))[0]
        self.output_path: str = output_path
        self.type_series: str = type_series
        self.keys_link: dict = keys_link
        self.alternative_name = self.get_alternative_name()

        if not os.path.isdir(self.output_path):
            try:
                os.makedirs(self.output_path)
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise

    def get_alternative_name(self):
        """
        Transforms name to HEC DSSVue nomenclature (10 characters)

        """
        name = None

        if self.type_series == 'STO':
            name = "M" + "{:09d}".format(int(self.dss_name))
        return name

    def create_config(self,
                      config_file) -> str:
        """

        Parameters
        ----------
        config_file (str)
            Path of config file

        Returns
        -------
        idx_sim_run (str)
            Index of sim. run (file) in .conf file for this alternative
        idx_malt (str)
            Index of malt (file) in .conf file for this alternative
        idx_ralt (str)
            Index of ralt (file) in .conf file for this alternative
        mod_time (str)
            Time at creation of file in seconds
        """

        # create a clean copy from original config file
        with open(os.path.join(config_file), 'r') as f:
            lines = f.readlines()

        end_header_position = [idx for idx, s in enumerate(lines) if "ManagerProxyEnd" in s][0]
        new_config_lines = lines[0:end_header_position + 1]

        with open(os.path.join(self.output_path,
                               'rss.conf'), "w") as output:
            for row in new_config_lines:
                output.write(str(row))
        return os.path.join(self.output_path, 'rss.conf')

    def update_config(self) -> list:
        """

        Parameters
        ----------
        config_tmp_file (str)
            Path of config file

        Returns
        -------
        idx_sim_run (str)
            Index of sim. run (file) in .conf file for this alternative
        idx_malt (str)
            Index of malt (file) in .conf file for this alternative
        idx_ralt (str)
            Index of ralt (file) in .conf file for this alternative
        mod_time (str)
            Time at creation of file in seconds
        """

        # create a clean copy from original config file
        with open(os.path.join(self.output_path,
                               'rss.conf'), 'r') as f:
            lines = f.readlines()

        modified_time = int([s for idx, s in enumerate(lines) if "ModifiedTime" in s][-1].split('=')[-1].rstrip())
        index = int([s for idx, s in enumerate(lines) if "Index" in s][-1].split('=')[-1].rstrip())

        idx_ralt = index + 1
        idx_fits = index + 2
        idx_obs = index + 3
        idx_malt = index + 4
        idx_simrun = index + 5
        mod_time = modified_time + 250

        ralt_lines = ['\nManagerProxyBegin',
                      'Name={}'.format(self.alternative_name),
                      'Description=',
                      'Path=rss/{}.ralt'.format(self.alternative_name),
                      'Class=hec.model.RssAlt',
                      'Index={}'.format(idx_ralt),
                      'ModifiedTime={}'.format(str(modified_time + 50)),
                      'ManagerProxyEnd\n']

        fits_lines = ['ManagerProxyBegin',
                      'Name={}'.format(self.alternative_name),
                      'Description=',
                      'Path=rss/{}.fits'.format(self.alternative_name),
                      'Class=hec.model.TSDataSet',
                      'Index={}'.format(idx_fits),
                      'ModifiedTime={}'.format(str(modified_time + 100)),
                      'ManagerProxyEnd\n']

        obs_lines = ['ManagerProxyBegin',
                     'Name={}Obs'.format(self.alternative_name),
                     'Description=',
                     'Path=rss/{}Obs.fits'.format(self.alternative_name),
                     'Class=hec.model.TSDataSet',
                     'Index={}'.format(idx_obs),
                     'ModifiedTime={}'.format(str(modified_time + 150)),
                     'ManagerProxyEnd\n']

        malt_lines = ['ManagerProxyBegin',
                      'Name={}'.format(self.alternative_name),
                      'Description=(Simulation Run)',
                      'Path=rss/{}.malt'.format(self.alternative_name),
                      'Class=hec.model.ModelAlt',
                      'Index={}'.format(idx_malt),
                      'ModifiedTime={}'.format(str(modified_time + 100)),
                      'ManagerProxyEnd\n']

        sim_run_lines = ['ManagerProxyBegin',
                         'Name={}'.format(self.alternative_name),
                         'Description=(Simulation Run)',
                         'Path=rss/{}.simrun'.format(self.alternative_name),
                         'Class=hec.model.RssSimRun',
                         'Index={}'.format(idx_simrun),
                         'ModifiedTime={}'.format(str(modified_time + 150)),
                         'ManagerProxyEnd']

        new_config_lines = ralt_lines + fits_lines + obs_lines + malt_lines + sim_run_lines

        with open(os.path.join(self.output_path,
                               'rss.conf'), "a") as output:
            for row in new_config_lines:
                output.write(str(row) + '\n')
        return [idx_simrun, idx_malt, idx_ralt, mod_time]

    def creation_fits(self):
        """
        Creates .fits file required as part as an alternative

        """
        output_file = os.path.join(self.output_path,
                                   self.alternative_name + '.fits')
        with open(output_file, "w") as text_file:
            print("TSDataSet Name={}".format(self.alternative_name), file=text_file)
            print("Description=(TS Data Set)\n", file=text_file)
            print("_type=0\nParentPath=null\nParentClass=null", file=text_file)
            for idx, (key, value) in enumerate(self.keys_link.items()):
                print("TSRecord={}".format(idx), file=text_file)
                print("TSRecord Name={}".format(key), file=text_file)
                print("TSRecord VariableId=4", file=text_file)
                print("TSRecord ParamName=Flow", file=text_file)
                print("TSRecord DssFilename=shared/{}".format(self.dss_name + '.hec'),
                      file=text_file)
                print("TSRecord DssPathname=/{}/CANIAPISCAU///1DAY/{}/".format(self.dss_name,
                                                                               value,
                                                                               self.dss_name),
                      file=text_file)
                print("TSRecord InputPosition=0", file=text_file)
                print("TSRecord End=\n", file=text_file)

    def creation_obs(self):
        """
        Creates .obs file required as part as an alternative

        """
        output_file = os.path.join(self.output_path,
                                   self.alternative_name + 'Obs.fits')
        with open(output_file, "w") as text_file:
            print('TSDataSet Name={}'.format(self.alternative_name + 'Obs'), file=text_file)
            print('Description=(Observed TS Data Set)\n', file=text_file)
            print('_type=0\nParentPath=null\nParentClass=null', file=text_file)

    def creation_simrun(self,
                        idx_sim_run: str,
                        idx_malt: str,
                        mod_time: str
                        ):
        """
        Creates .simrun file required as part as an alternative

        Parameters
        ----------
        idx_sim_run (str)
            Index of sim. run (file) in .conf file for this alternative
        idx_malt (str)
            Index of malt (file) in .conf file for this alternative
        mod_time (str)
            Time at creation of file in seconds

        """
        output_file = os.path.join(self.output_path,
                                   self.alternative_name + '.simrun')

        with open(os.path.join(os.path.dirname(__file__),
                               'templates',
                               'simrun_template.txt'), 'r') as f:
            lines = f.readlines()

        with open(output_file, 'w') as text_file:
            for i, line in enumerate(lines, 1):  # numbering starts at 1
                if i == 5:
                    print("  STR={}".format(self.alternative_name), file=text_file)
                elif i == 9:
                    print('  I={}'.format(idx_sim_run), file=text_file)
                elif i == 15:
                    print('  J='.format(mod_time), file=text_file)
                elif i == 17:
                    print('  STR={}'.format(self.alternative_name), file=text_file)
                elif i == 27:
                    print('        STR={}'.format(self.alternative_name), file=text_file)
                elif i == 33:
                    print('        I={}'.format(idx_malt), file=text_file)
                else:
                    print('{}'.format(line), file=text_file)

    def creation_malt(self,
                      idx_ralt: str,
                      idx_malt: str,
                      mod_time: str
                      ):
        """
        Creates .malt file required as part as an alternative

        Parameters
        ----------
        idx_ralt (str)
            Index of ralt (file) in .conf file for this alternative
        idx_malt (str)
            Index of malt (file) in .conf file for this alternative
        mod_time (str)
            Time at creation of file in seconds

        """
        output_file = os.path.join(self.output_path,
                                   self.alternative_name + '.simrun')

        with open(os.path.join(os.path.dirname(__file__),
                               'templates',
                               'malt_template.txt'), 'r') as f:
            lines = f.readlines()

        with open(output_file, 'w') as text_file:
            for i, line in enumerate(lines, 1):  # numbering starts at 1
                if i == 5:
                    print("  STR={}".format(self.alternative_name), file=text_file)
                elif i == 9:
                    print('  I={}'.format(idx_malt), file=text_file)
                elif i == 15:
                    print('  J='.format(mod_time), file=text_file)
                elif i == 34:
                    print('  STR={}'.format(self.alternative_name), file=text_file)
                elif i == 39:
                    print('    I={}'.format(idx_ralt), file=text_file)
                elif i == 45:
                    print('    I={}'.format(idx_ralt), file=text_file)
                else:
                    print('{}'.format(line), file=text_file)

    def creation_ralt(self,
                      ralt_file: str,
                      flow_compute_type: str = "2",
                      ):
        """
        Creates .malt file required as part as an alternative

        Parameters
        ----------
        ralt_file (str)
            Path of ralt file
        flow_compute_type (str), default "2"
            HEC ResSim computing method : 0-Program determined 1-Per average 2-Instataneous
        """

        if not isinstance(flow_compute_type, str):
            try:
                str(flow_compute_type)
            except TypeError:
                print("flow_compute_type argument should be an integer")

        output_file = os.path.join(self.output_path,
                                   self.alternative_name + '.ralt')

        with open(ralt_file, 'r') as f:
            lines = f.readlines()

        with open(output_file, 'w') as text_file:
            for i, line in enumerate(lines, 1):  # numbering starts at 1
                if i == 1:
                    print("RssAlt Name={}".format(self.alternative_name), file=text_file)
                elif i == 8:
                    print('InputTSData Path=rss/{}.fits'.format(self.alternative_name), file=text_file)
                elif i == 10:
                    print('FlowComputeType={}'.format(flow_compute_type), file=text_file)
                elif i == 16:
                    print('ObservedTSData Path=rss/{}Obs.fits'.format(self.alternative_name), file=text_file)
                else:
                    print('{}'.format(line), file=text_file)

    def add_alternative(self,
                        ralt_file: str ):
        """
        Add alternative files to output path and update configuration file

        Parameters
        ----------
        ralt_file (str)
            Path of ralt file

        Returns
        -------

        """
        [idx_simrun, idx_malt, idx_ralt, mod_time] = self.update_config()

        self.creation_fits()
        self.creation_obs()
        self.creation_ralt(ralt_file)
        self.creation_malt(idx_ralt=idx_ralt,
                           idx_malt=idx_malt,
                           mod_time=mod_time)
        self.creation_simrun(idx_sim_run=idx_simrun,
                             idx_malt=idx_malt,
                             mod_time=mod_time)
