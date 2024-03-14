import pandas as pd
import datetime
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from catboost import CatBoostRegressor
from category_encoders import CatBoostEncoder
import yaml
import os
import joblib

def fit_model():
    with open('params.yaml', 'r') as fd:
        params = yaml.safe_load(fd)

    data = pd.read_csv('data/initial_data.csv')

    binary_features = data.select_dtypes(include='bool')

    cat_features = data[['building_type_int']].astype(str)

    data['build_years_old'] = datetime.datetime.now().year - data['build_year']

    num_features = data[['floor', 'kitchen_area', 'living_area', 'rooms',
        'total_area', 'build_years_old', 'ceiling_height',
        'flats_count', 'floors_total']]

    preprocessor = ColumnTransformer(
        [
        ('binary', OneHotEncoder(drop=params['one_hot_drop']), binary_features.columns.tolist()),
        ('cat', OneHotEncoder(drop='first'), cat_features.columns.tolist()),
        ('num', StandardScaler(), num_features.columns.tolist())
        ],
        remainder='drop',
        verbose_feature_names_out=False
    )

    model = CatBoostRegressor(iterations=params['iterations'], 
                                depth=params['depth'], 
                                loss_function=params['loss_function'])

    pipeline = Pipeline(
        [
            ('preprocessor', preprocessor),
            ('model', model)
        ]
    )
    pipeline.fit(data, data[params['target_col']]) 
    
    data.to_csv('data/prepared_data.csv', index=None)
    os.makedirs('models', exist_ok=True)
    with open('models/fitted_model.pkl', 'wb') as fd:
        joblib.dump(pipeline, fd) 


if __name__ == '__main__':
	fit_model()
