
from fuzzywuzzy import process, fuzz
import pandas as pd
from rapidfuzz import fuzz, process
from scipy.optimize import linear_sum_assignment
import numpy as np

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

    # 08019-08022(2022-2023)
    address_df = pd.read_csv('sources/Endesa/harmonizer/a.csv', dtype=str)

    # address_df = pd.read_csv('sources/Endesa/harmonizer/a.csv', dtype=str)
    # address_df['address'] = address_df['street_name'] + ' ' + address_df['street_number']
    # address_df['address'] = address_df['address'].apply(lambda x: ' '.join(x.split(' ')[1:]).lower())
    # address_df = address_df[address_df.CODPOS.isin(['08019', '08022'])]

    for sheet_name, consumptions_df in pd.read_excel('data/Endesa/Agrupado_Ayunt_Codigos_Postales_08019-08022_Barcelona.xlsx', sheet_name=None).items():
        # remove street_number and num_contract 0
        consumptions_df = consumptions_df[(consumptions_df['STREET_NUMBER__C'] != 0) & (consumptions_df['NUM_CONTRATOS'] != 0)].reset_index(drop=True)
        # fuzzy de los valores unicos del nombre de la calle respecto a calle de catastro unicos para hacer un diccionario

        consumptions_df['street_name'] = consumptions_df['STREET_TYPE__C'] + ' ' + consumptions_df['STREET_DESCRIPTION__C']
        consumptions_street_names_uniques = consumptions_df['street_name'].unique()
        address_street_names_uniques = address_df['street_name'].unique()
        street_name_dict = {}
        for item in consumptions_street_names_uniques:
            match = process.extractOne(item, address_street_names_uniques, scorer=fuzz.ratio)
            street_name_dict[item] = match[0]
        # Map street name
        consumptions_df['street_name_mapped'] = consumptions_df['street_name'].map(street_name_dict)

        # match perfecto entre nombre de calle mapeado y numero
        consumptions_df['STREET_NUMBER__C'] = consumptions_df['STREET_NUMBER__C'].astype(str)
        consumptions_df['address'] = consumptions_df['street_name_mapped'] + ' ' + consumptions_df[
            'STREET_NUMBER__C']
        consumptions_df['address'] = consumptions_df['address'].astype(str)
        consumptions_df['address'] = consumptions_df['address'].apply(lambda x: x.lower())

        address_df['address'] = address_df['street_name'] + ' ' + address_df['street_number']
        address_df['address'] = address_df['address'].apply(lambda x: x.lower())

        # difuse matrix
        score_matrix = np.zeros((len(consumptions_df), len(address_df)))
        for i, a in enumerate(consumptions_df['address']):
            for j, b in enumerate(address_df['address']):
                score_matrix[i, j] = fuzz.ratio(a, b)
        # convert to cost matrix
        cost_matrix = 100 - score_matrix
        # hungarian algorithm
        row_ind, col_ind = linear_sum_assignment(cost_matrix)
        # threshold
        score_threshold = 97
        matches = [None] * len(consumptions_df)
        for row, col in zip(row_ind, col_ind):
            score = 100 - cost_matrix[row, col]
            if score >= score_threshold:
                matches[row] = address_df['address'][col]
        consumptions_df['match'] = matches
        # merge entre consumptions_df address_df
        merged_df = pd.merge(consumptions_df, address_df, left_on='match', right_on='address', how='left')
        merged_df.drop(['localId_y', 'namespace_y',
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
                                   'namespace_x', 'specification', 'geometry',
                                    'cat_ref', 'gml_id_y', 'conditionOfConstruction', 'address_y'],
                                  inplace=True, axis=1)

        merged_df.to_csv(f"sources/Endesa/harmonizer/Electricity08019-08022-{sheet_name}-97.csv")




