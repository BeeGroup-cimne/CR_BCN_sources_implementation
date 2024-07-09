import pandas as pd
import ftfy
from fuzzywuzzy import process

# def merge_dataframes(df1, df2):
# def READ_dataframes(df1, df2):
def funcion_keep(dup_rows):
    return dup_rows.loc[dup_rows['Nº CUPS'] == dup_rows['Núm. CUPS']]

def fuzzy_merge(df_1, df_2, key1, key2, threshold=90, limit=1):
    """
    :param df_1: the left table to join
    :param df_2: the right table to join
    :param key1: key column of the left table
    :param key2: key column of the right table
    :param threshold: how close the matches should be to return a match, based on Levenshtein distance
    :param limit: the amount of matches that will get returned, these are sorted high to low
    :return: dataframe with boths keys and matches
    """
    s = df_2[key2].tolist()

    m = df_1[key1].apply(lambda x: process.extract(x, s, limit=limit))
    df_1['matches'] = m

    m2 = df_1['matches'].apply(lambda x: ', '.join([i[0] for i in x if i[1] >= threshold]))
    df_1['matches'] = m2

    return df_1


if __name__ == '__main__':
    # 08030-08033(2022)
    # consumptions_df = pd.read_excel('data/Endesa/2022_Agrupado_Ayunt_Barcelona_Cod_Postal_08030_08033_con Num Calle.xlsx', engine='openpyxl', dtype=str)
    # consumptions_df['address'] = consumptions_df['STREET_DESCRIPTION__C'] + ' ' + consumptions_df['STREET_NUMBER__C']
    # consumptions_df['address'] = consumptions_df['address'].apply(lambda x: x.lower())

    # consumptions_df["address_2022"] = consumptions_df['direccion'].apply(lambda x: ftfy.fix_text(x))
    # consumptions_df['address_2022'] = consumptions_df['address_2022'].str.upper().str.replace(' ', '')
    # consumptions_df["cp"] = consumptions_df['cp'].apply(lambda x: x.zfill(5))

    # 08030-08033(2023)
    # consumptions_df = pd.read_excel('data/Endesa/2023_Agrupado_Ayunt_BARCELONA_Cod_Postal_08030_08033.xlsx', engine='openpyxl', dtype=str)
    # consumptions_df['address'] = consumptions_df['STREET_DESCRIPTION__C'] + ' ' + consumptions_df['STREET_NUMBER__C']
    # consumptions_df['address'] = consumptions_df['address'].astype(str)
    # consumptions_df['address'] = consumptions_df['address'].apply(lambda x: x.lower())

    # address_df = pd.read_csv('sources/Endesa/harmonizer/a.csv', dtype=str)
    # address_df['address'] = address_df['street_name'] + ' ' + address_df['street_number']
    # address_df['address'] = address_df['address'].apply(lambda x: ' '.join(x.split(' ')[1:]).lower())
    # address_filtered_df = address_df[address_df.CODPOS.isin(['08030', '08033'])]

    # join 08033-08030(2022/2023)
    # fuzzy_consumptions_df = fuzzy_merge(consumptions_df, address_filtered_df, 'address', 'address', threshold=95)
    # consumption_merge_df = pd.merge(fuzzy_consumptions_df, address_filtered_df, left_on='matches',
    #                                 right_on='address', how='left')
    # consumption_merge_df.drop(['localId_y', 'namespace_y',
    #                            'horizontalGeometryEstimatedAccuracy',
    #                            'horizontalGeometryEstimatedAccuracy_uom', 'currentUse',
    #                            'numberOfBuildingUnits', 'numberOfDwellings',
    #                            'numberOfFloorsAboveGround', 'officialAreaReference', 'value',
    #                            'value_uom', 'cadastral_zonning_reference', 'gml_id',
    #                            'estimatedAccuracy', 'estimatedAccuracy_uom', 'localId', 'namespace',
    #                            'LocalisedCharacterString', 'nationalCadastalZoningReference',
    #                            'originalMapScaleDenominator', 'height_above_sea_level', 'CUSEC',
    #                            'CUMUN', 'CUDIS', 'codi_districte', 'nom_districte',
    #                            'codi_barri', 'nom_barri', 'gml_id_x', 'localId_x',
    #                            'namespace_x', 'specification', 'street_number', 'geometry',
    #                            'street_name', 'cat_ref', 'gml_id_y', 'conditionOfConstruction', 'address_y'],
    #                           inplace=True, axis=1)
    #
    # consumption_merge_df.to_csv('sources/Endesa/harmonizer/Electricity2023-95.csv')

    # 08019-08022(2022-2023)
    address_df = pd.read_csv('sources/Endesa/harmonizer/a.csv', dtype=str)
    address_df['address'] = address_df['street_name'] + ' ' + address_df['street_number']
    address_df['address'] = address_df['address'].apply(lambda x: ' '.join(x.split(' ')[1:]).lower())
    address_filtered_df = address_df[address_df.CODPOS.isin(['08019', '08022'])]

    for sheet_name, consumptions_df in pd.read_excel('data/Endesa/Agrupado_Ayunt_Codigos_Postales_08019-08022_Barcelona.xlsx', sheet_name=None).items():
        consumptions_df['STREET_NUMBER__C'] = consumptions_df['STREET_NUMBER__C'].astype(str)
        consumptions_df['address'] = consumptions_df['STREET_DESCRIPTION__C'] + ' ' + consumptions_df[
            'STREET_NUMBER__C']
        consumptions_df['address'] = consumptions_df['address'].astype(str)
        consumptions_df['address'] = consumptions_df['address'].apply(lambda x: x.lower())


        fuzzy_consumptions_df = fuzzy_merge(consumptions_df, address_filtered_df, 'address', 'address', threshold=95)
        consumption_merge_df = pd.merge(fuzzy_consumptions_df, address_filtered_df, left_on='matches',
                                    right_on='address', how='left')
        consumption_merge_df.drop(['localId_y', 'namespace_y',
           'horizontalGeometryEstimatedAccuracy',
           'horizontalGeometryEstimatedAccuracy_uom', 'currentUse',
           'numberOfBuildingUnits', 'numberOfDwellings',
           'numberOfFloorsAboveGround', 'officialAreaReference', 'value',
           'value_uom', 'cadastral_zonning_reference', 'gml_id',
           'estimatedAccuracy', 'estimatedAccuracy_uom', 'localId', 'namespace',
           'LocalisedCharacterString', 'nationalCadastalZoningReference',
           'originalMapScaleDenominator', 'height_above_sea_level', 'CUSEC',
           'CUMUN', 'CUDIS', 'codi_districte', 'nom_districte',
           'codi_barri', 'nom_barri', 'gml_id_x', 'localId_x',
           'namespace_x', 'specification', 'street_number', 'geometry',
           'street_name', 'cat_ref', 'gml_id_y', 'conditionOfConstruction', 'address_y'], inplace=True, axis=1)

        consumption_merge_df.to_csv(f"sources/Endesa/harmonizer/Electricity08019-08022-{sheet_name}-95.csv")

