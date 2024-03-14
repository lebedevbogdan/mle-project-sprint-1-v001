from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

def create_table():
    from sqlalchemy import inspect, MetaData, Table, Column, Integer, Float, UniqueConstraint, Boolean, BigInteger
    metadata = MetaData()
    price_table = Table('flats_price', metadata,
                            Column('id', Integer, primary_key=True, autoincrement=True),
                            Column('flat_id', Integer),
                            Column('floor', Integer),
                            Column('is_apartment', Boolean),
                            Column('kitchen_area', Float),
                            Column('living_area', Float),
                            Column('rooms', Integer),
                            Column('studio', Boolean),
                            Column('total_area', Float),
                            Column('building_id', Integer),
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
    hook = PostgresHook('destination_db')
    db_conn = hook.get_sqlalchemy_engine()
    if not inspect(db_conn).has_table('flats_price'): 
        metadata.create_all(db_conn)

def extract(**kwargs):
    hook = PostgresHook('destination_db')
    conn = hook.get_conn()
    sql = f'''select 
            f.id
            , floor
            , is_apartment
            , kitchen_area
            , living_area
            , rooms
            , studio
            , total_area
            , building_id
            , build_year
            , building_type_int
            , latitude
            , longitude
            , ceiling_height
            , flats_count
            , floors_total
            , has_elevator
            , price
            from flats f 
            left join buildings b on f.building_id = b.id
            '''
    extracted_data = pd.read_sql(sql, conn)
    conn.close()
    kwargs['ti'].xcom_push('extracted_data',extracted_data)

def transform(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract', key='extracted_data')
    data.rename(columns={'id': 'flat_id'}, inplace=True)
    ti.xcom_push('transformed_data', data)

def load(**kwargs):
    hook = PostgresHook('destination_db')
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='transform', key='transformed_data')
    hook.insert_rows(
            table="flats_price",
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=['flat_id'],
            rows=data.values.tolist()
    )