import pygeohash as gh
import pandas as pd
from deltalake import DeltaTable, write_deltalake
from typing import ClassVar
from abc import ABC, abstractmethod

class BaseTransformer:
    datalake_layer: ClassVar[str]
    
    def __init__(self, start_date, end_date, storage_options: dict | None):
        self.start_date = start_date
        self.end_date = end_date
        self.storage_options = storage_options
    
    @property
    @abstractmethod
    def datasets(self)->dict[str, DeltaTable]:
        """Read the deltalake to extract the datasets to use in the process.
        
        Returns a dict with the name of the dataset and its delta table object.
        """        
    
    @abstractmethod
    def transform(self)->pd.DataFrame:
        """Loads and transforms the delta tables.
        
        Returns a pandas dataframe"""
        
        
    