class CreationAlternative:
    """

    """
    def __init__(self,
                 dss_filename: str,
                 output_path: str = None,
                 type_series: str = None,
                 keys_link: dict = {},
                 ):
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

        :return:
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
        """
        output_file = os.path.join(self.output_path,
                                   self.alternative_name + 'Obs.fits')
        with open(output_file, "w") as text_file:
            print('TSDataSet Name={}'.format(self.alternative_name + 'Obs'), file=text_file)
            print('Description=(Observed TS Data Set)\n', file=text_file)
            print('_type=0\nParentPath=null\nParentClass=null', file=text_file)
    def creation_simrun(self):
        """
        """
AsciiSerializer=4
{=hec.rss.model.RssSimRun
  ID=0
  FLD=_name
  STR=10000P01DJ
  FLD=_description
  STR=
  FLD=_index
  I=327
  FLD=_path
  STR=
  FLD=_nextTSRecordProxyIndex
  I=0
  FLD=_lastModifiedTime
  J=1415737385957
  FLD=_userName
  STR=10000P01DJ
  FLD=_altList
  {=java.util.Vector
    ID=1
    {COLLECT
      {=hec.model.AltItem
        ID=2
        FLD=_program
        STR=rss
        FLD=_altName
        STR=10000P01DJ
        FLD=_inputPos
        I=0
        FLD=_modelPos
        I=-1
        FLD=_altIndex
        I=326
      }
    }COLLECT
  }
  FLD=_trialList
  {=java.util.Vector
    ID=3
  }
  FLD=_version
  I=3
  FLD=_computeInundation
  Z=false
  FLD=_lastComputeTime
  J=0
  FLD=_configurationId
  J=3
  FLD=_activeRun
  Z=false
  FLD=_selectedRun
  Z=false
  FLD=_expanded
  Z=false
}