import glob
import os


# verify that all provided reservoir names exist in the model
def verify_reservoir_names_exist(df_model,
                                 df_provided
                                 ):
    reservoir_names_from_model = df_model \
        .columns \
        .get_level_values(0) \
        .unique()
    reservoir_names_from_file = df_provided.columns \
        .get_level_values(0) \
        .unique()
    error_in_column_names = reservoir_names_from_file \
        .to_frame() \
        .loc[~reservoir_names_from_file.isin(reservoir_names_from_model)] \
        .values \
        .ravel()
    if error_in_column_names.any():
        raise ValueError('Reservoirs {} are not in the model.'.format(', '.join(error_in_column_names)) +
                         ' Please validate your input files.')


def verify_monotonic_df(df_provided):
    # verify that all series are monotonic
    not_monotonic_series_names = verify_monotonic_values(df_provided)
    if not_monotonic_series_names.any():
        raise ValueError('Reservoirs {} values are not monotonic.'.format(', '.join(not_monotonic_series_names)) +
                         ' Please validate your input files.')


def verify_monotonic_values(df):
    df_is_monotonic = df.apply(lambda serie: serie.dropna().is_monotonic)
    return df_is_monotonic.loc[df_is_monotonic == False].index.get_level_values(0)


def verify_base_folder_exist(model_base_folder):

    try:
        assert 'base' in [os.path.basename(filename)
                          for filename in glob.glob(os.path.join(model_base_folder,'*'))]

    except AssertionError as e:
        raise AssertionError("""Folder "base" not found in provided path : {}. \
    Please change your input to an appropriate base """.format(model_base_folder)) from e