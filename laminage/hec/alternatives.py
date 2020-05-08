import os


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
        Constructing DataFrame from a dictionary.
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
                 keys_link: dict = {},
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
                                STO (stochastical analysis study)
        keys_link : dict, default {}
            Dictionary to link dss inflows with Hec ResSim's nomenclature
            Keys correspond to inflow names in Hec ResSim's model
            while values correspond to dss inflow names

        """
        self.dss_filename: str = dss_filename
        self.dss_name: str = os.path.splitext(os.path.basename(self.dss_filename))[0]
        self.output_path: str = output_path
        self.type_series:  str = type_series
        self.keys_link: dict = keys_link
        self.alternative_name = self.get_alternative_name()

    def get_alternative_name(self):
        """

        Returns
        -------

        """
        if self.type_series == 'STO':
            name = "M" + "{:09d}".format(int(self.dss_name))
        return name

    def creation_fits(self):
        """

        Returns
        -------

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

        Returns
        -------

        """
        output_file = os.path.join(self.output_path,
                                   self.alternative_name + 'Obs.fits')
        with open(output_file, "w") as text_file:
            print('TSDataSet Name={}'.format(self.alternative_name + 'Obs'), file=text_file)
            print('Description=(Observed TS Data Set)\n', file=text_file)
            print('_type=0\nParentPath=null\nParentClass=null', file=text_file)

    def creation_simrun(self):
        """

        Returns
        -------

        """
