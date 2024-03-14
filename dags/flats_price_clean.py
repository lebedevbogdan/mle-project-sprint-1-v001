import pendulum
from airflow.decorators import dag, task

@dag(
    schedule='@once',
    start_date=pendulum.datetime(2024, 3, 13, tz="UTC"),
    tags=["clean", 'flats', 'price']
)
def clean_flats_price():
    import pandas as pd
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    @task()
    def create_table():
        from sqlalchemy import inspect, MetaData, Table, Column, Integer, Float, UniqueConstraint, BigInteger, String, Boolean
        hook = PostgresHook('destination_db')
        db_engine = hook.get_sqlalchemy_engine()

        metadata = MetaData()

        price_table = Table('flats_price_clean', metadata,
                                Column('id', Integer, primary_key=True, autoincrement=True),
                                Column('flat_id', String),
                                Column('floor', Integer),
                                Column('is_apartment', Boolean),
                                Column('kitchen_area', Float),
                                Column('living_area', Float),
                                Column('rooms', Integer),
                                Column('studio', Boolean),
                                Column('total_area', Float),
                                Column('building_id', String),
                                Column('build_year', Integer),
                                Column('building_type_int', Integer),
                                Column('latitude', Float),
                                Column('longitude', Float),
                                Column('ceiling_height', Float),
                                Column('flats_count', Integer),
                                Column('floors_total', Integer),
                                Column('has_elevator', Boolean),
                                Column('price', BigInteger),
                                UniqueConstraint('flat_id'))
        if not inspect(db_engine).has_table(price_table.name):
            metadata.create_all(db_engine)
    
    @task()
    def extract():
        hook = PostgresHook('destination_db')
        conn = hook.get_conn()
        sql = f"""
        select * from flats_price
        """
        data = pd.read_sql(sql, conn).drop(columns=['id'])
        conn.close()
        return data

    @task()
    def transform(data: pd.DataFrame) -> pd.DataFrame:
        
        def int_to_str(data: pd.DataFrame) -> pd.DataFrame:
            columns = ['flat_id', 'building_id']
            for col in columns:
                data[col] = data[col].astype(str)
            return data
        
        def remove_features_target_duplicates(data: pd.DataFrame) -> pd.DataFrame:
            """
            Удаление дубликатов по фичам и таргету. Оставляем одно из значений
            """
            feature_cols = data.drop(columns = ['flat_id', 'building_id']).columns.to_list()
            is_duplicated_features = data.duplicated(subset=feature_cols, keep='first')
            data = data[~is_duplicated_features].reset_index(drop=True)
            return data
        
        def remove_features_duplicates(data: pd.DataFrame) -> pd.DataFrame:
            """
            Удаление дубликатов только по фичам, так как при одинаковых наборах фичей могут быть разные таргеты. Не оставляем дубли.
            """
            feature_cols = data.drop(columns = ['flat_id', 'building_id', 'price']).columns.to_list()
            is_duplicated_features = data.duplicated(subset=feature_cols, keep=False)
            data = data[~is_duplicated_features].reset_index(drop=True)
            return data
        
        def fill_missing_values(data: pd.DataFrame) -> pd.DataFrame:
            cols_with_nans = data.isnull().sum()
            cols_with_nans = cols_with_nans[cols_with_nans > 0]
            for col in cols_with_nans:
                if data[col].dtype in [float, int]:
                    fill_value = data[col].mean()
                data[col] = data[col].fillna(fill_value)
            return data
        
        def remove_outliers(data: pd.DataFrame, threshold = 1.5) -> pd.DataFrame:
            columns = ['price', 'flats_count', 'ceiling_height', 'total_area', 'living_area', 'kitchen_area']
            potential_outliers = pd.DataFrame()

            for col in columns:
                Q1 = data[col].quantile(0.25)
                Q3 = data[col].quantile(0.75)
                IQR = Q3 - Q1
                margin = IQR * threshold
                lower = Q1 - margin
                upper = Q3 + margin
                potential_outliers[col] = ~data[col].between(lower, upper)

            outliers = potential_outliers.any(axis=1)
            return data[~outliers]
        
        data = int_to_str(data)
        data = remove_features_target_duplicates(data)
        data = remove_features_duplicates(data)
        data = fill_missing_values(data)
        data = remove_outliers(data)
        return data

    @task()
    def load(data: pd.DataFrame):
        hook = PostgresHook('destination_db')
        hook.insert_rows(
            table= 'flats_price_clean',
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=['flat_id'],
            rows=data.values.tolist()
    )
    
    create_table()
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)

clean_flats_price()
