import dask.dataframe as dd
import os

def read_and_process_csv(csv_file_path):
    column_names = [
        'transaction_id',
        'price',
        'date_of_transfer',
        'postcode',
        'property_type',
        'new_build',
        'tenure',
        'paon',
        'saon',
        'street',
        'locality',
        'town',
        'district',
        'county',
        'ppd_category_type',
        'record_status',
    ]

    ddf = dd.read_csv(
        csv_file_path,
        names=column_names,
        sep=',',
        quotechar='"',
        dtype={'price': 'int32'},
        assume_missing=True,
        blocksize='64MB',
    )
    ddf['date_of_transfer'] = dd.to_datetime(ddf['date_of_transfer'], format="%Y-%m-%d %H:%M")
    ddf = ddf.categorize(columns=['property_type', 'new_build', 'tenure', 'record_status'])

    # Add a new column with the transfer month
    ddf['transfer_month'] = ddf['date_of_transfer'].dt.to_period('M')
    return ddf
    
def append_new_data_to_parquet(parquet_file_path, new_data, engine='pyarrow'):
    if os.path.exists(parquet_file_path):
        existing_data = dd.read_parquet(parquet_file_path)
    else:
        existing_data = None

    if existing_data is not None:
        updated_data = existing_data.append(new_data)
    else:
        updated_data = new_data

    updated_data.to_parquet(
        parquet_file_path,
        engine=engine,
        partition_on=['transfer_month'],
        compression='snappy',
        append=False,
        ignore_divisions=True,
        write_index=False,
    )