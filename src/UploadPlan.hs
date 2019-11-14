module UploadPlan where

import Data.Text (Text)
import GHC.Generics
import Control.Newtype.Generics (Newtype)
import Data.Aeson (ToJSON)

data ColumnType
  = StringType
  | DoubleType
  | IntType
  | IdType
  | DecimalType
  | DateType Text
  deriving (Generic, Show, Eq)

instance ToJSON ColumnType

newtype WorkbenchId = WorkbenchId Int deriving (Generic, Show, Eq)
instance Newtype WorkbenchId
instance ToJSON WorkbenchId

newtype TemplateId = TemplateId Int deriving (Generic, Show, Eq)
instance Newtype TemplateId
instance ToJSON TemplateId

data UploadPlan = UploadPlan
  { workbenchId :: WorkbenchId
  , templateId :: TemplateId
  , uploadTable :: UploadTable
  }
  deriving (Generic, Show)

instance ToJSON UploadPlan

data NamedValue = NamedValue { column :: Text, value :: Text }
  deriving (Generic, Show)

instance ToJSON NamedValue

data MappingItem = MappingItem {columnName :: Text, columnType :: ColumnType, id :: Int}
  deriving (Generic, Show)

instance ToJSON MappingItem

data UploadTable = UploadTable
  { tableName :: Text
  , idColumn :: Text
  , strategy :: UploadStrategy
  , mappingItems :: [MappingItem]
  , staticValues :: [NamedValue]
  , toOneTables :: [ToOne]
  , toManyTables :: [ToMany]
  }
  deriving (Generic, Show)

instance ToJSON UploadTable

data UploadStrategy
  = AlwaysCreate
  | AlwaysMatch [NamedValue]
  | MatchOrCreate [NamedValue]
  deriving (Generic, Show)
instance ToJSON UploadStrategy

data ToMany = ToMany { toManyFK :: Text, toManyTable :: Text, records :: [ToManyRecord] }
  deriving (Generic, Show)

instance ToJSON ToMany

data ToOne = ToOne { toOneFK :: Text, toOneTable :: UploadTable }
  deriving (Generic, Show)

instance ToJSON ToOne

data ToManyRecord = ToManyRecord
  { filters :: [NamedValue]
  , staticValues :: [NamedValue]
  , toOneTables :: [ToOne]
  , mappingItems :: [MappingItem]
  }
  deriving (Generic, Show)

instance ToJSON ToManyRecord
