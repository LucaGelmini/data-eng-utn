from deltalake import DeltaTable, write_deltalake
import pandas as pd
import pyarrow as pa
import src.config as cfg

class DeltaLakeLoader:
    def __init__(self, table_root_path: str, storage_options: dict[str, str] | None = None):
        self.table_root_path = f"s3://{cfg.BUCKET_NAME}/{table_root_path}" if storage_options else f"./out/{table_root_path}"
        self.storage_options = storage_options
        
        print("Loader initialized for: ", self.table_root_path)
        
    def merge_upsert(
        self, 
        location:str,
        data: pd.DataFrame, 
        predicate: str,
        partition_by: list[str] | None = None):
        path = f"{self.table_root_path}/{location}"
        
        try:
            dt = DeltaTable(path, storage_options=self.storage_options)
            (
                dt.merge(
                source=pa.Table.from_pandas(data),
                source_alias="src",
                target_alias="tgt",
                predicate=predicate
                )
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute()
            )
        except Exception as e:
            print(f"Delta table does not exist or merge failed: {e}. Creating new Delta table.")
            print(dict(path=path,
                data=data.columns.tolist(),
                storage_options=self.storage_options,
                partition_by=partition_by))
            write_deltalake(
                path,
                data,
                storage_options=self.storage_options,
                partition_by=partition_by
            )
            
    def delete_insert(
        self,
        location:str,
        data: pd.DataFrame,
        filter_col: str,
        partition_by: list[str] | None = None):
        """ Deletes existing partition and inserts new data."""
        
        path = f"{self.table_root_path}/{location}"
        filter_value = data[filter_col].iloc[0]
        
        if DeltaTable.is_deltatable(path, storage_options=self.storage_options):
            dt = DeltaTable(path, storage_options=self.storage_options)
            existing_data = dt.to_pandas()
            exists_partition: bool = (existing_data[filter_col] == filter_value).any()
            if exists_partition:
                dt.delete(f"{filter_col} = '{filter_value}'")
                print(f"Deleted partition: {filter_col} = {filter_value}")
            else:
                print(f"No partition found with {filter_col} = {filter_value}. No deletion performed.")
        else:
            print(f"Delta table does not exist at {path}. Creating.")
        write_deltalake(
            path,
            data,
            mode="append",
            partition_by=partition_by,
            storage_options=self.storage_options
        )
        print(f"Inserted {len(data)} registers into {path} with partition {filter_col} = {filter_value}.")
        
    def insert_overwrite(
        self,
        location: str,
        data: pd.DataFrame,
        partition_by: list[str],
        filter_col: str):
        
        path = f"{self.table_root_path}/{location}"
        if set(partition_by) ==  set(data.columns):
            raise ValueError("partition_by columns are not present in data columns.")
        if filter_col not in partition_by:
            raise ValueError("filter_column must be a valid partition")

        partition_value = data[filter_col].iloc[0]

        predicate = f"{filter_col} = '{partition_value}'"

        write_deltalake(
            path,
            data,
            mode="overwrite",
            partition_by=partition_by,
            predicate=predicate,
            storage_options=self.storage_options
        )
        print(f"Succesfuly overwritten {filter_col} = '{partition_value}' partition.")
