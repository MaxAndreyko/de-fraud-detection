import yaml
from pydantic import BaseModel, Field

class Schema(BaseModel):
    @classmethod
    def from_yaml(cls, file_path: str) -> "Schema":
        with open(file_path, "r") as file:
            config_data = yaml.safe_load(file)["tables"]
            return cls(**config_data)

class BankSchema(Schema):
    """Stores bank database table names
    """
    accounts: str
    cards: str
    clients: str

class DIMTableNames(BaseModel):
    accounts: str = Field(...)
    cards: str = Field(...)
    clients: str = Field(...)
    terminals: str = Field(...)

class FACTTableNames(BaseModel):
    blacklist: str = Field(...)
    transactions: str = Field(...)

class STGTableNames(BaseModel):
    accounts: str = Field(...)
    blacklist: str = Field(...)
    cards: str = Field(...)
    clients: str = Field(...)
    terminals: str = Field(...)
    transactions: str = Field(...)

class REPTableNames(BaseModel):
    fraud: str = Field(...)

class DWHSchema(Schema):
    DIM: DIMTableNames   # Dimension table names
    FACT: FACTTableNames # Fact table names
    STG: STGTableNames   # Staging table names
    REP: REPTableNames   # Report table names
