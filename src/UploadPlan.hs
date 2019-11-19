module UploadPlan where

import Data.Text (Text)
import GHC.Generics
import Control.Newtype.Generics (Newtype)
import Data.Aeson (FromJSON, ToJSON)

data ColumnType
  = StringType
  | DoubleType
  | IntType
  | IdType
  | DecimalType
  | DateType Text
  deriving (Generic, Show, Eq)

instance ToJSON ColumnType
instance FromJSON ColumnType

newtype WorkbenchId = WorkbenchId Int
  deriving (Generic, Show, Eq)

instance Newtype WorkbenchId
instance ToJSON WorkbenchId
instance FromJSON WorkbenchId

newtype TemplateId = TemplateId Int
  deriving (Generic, Show, Eq)

instance Newtype TemplateId
instance ToJSON TemplateId
instance FromJSON TemplateId

data UploadPlan = UploadPlan
  { workbenchId :: WorkbenchId
  , templateId :: TemplateId
  , uploadTable :: UploadTable
  }
  deriving (Generic, Show)

instance ToJSON UploadPlan
instance FromJSON UploadPlan

data NamedValue = NamedValue
  { column :: Text
  , value :: Text
  }
  deriving (Generic, Show)

instance ToJSON NamedValue
instance FromJSON NamedValue

data MappingItem = MappingItem
  { columnName :: Text
  , columnType :: ColumnType
  , id :: Int
  }
  deriving (Generic, Show)

instance ToJSON MappingItem
instance FromJSON MappingItem

data UploadTable = UploadTable
  { tableName :: Text
  , idColumn :: Text
  , idMapping :: Maybe Int
  , strategy :: UploadStrategy
  , mappingItems :: [MappingItem]
  , staticValues :: [NamedValue]
  , toOneTables :: [ToOne]
  , toManyTables :: [ToMany]
  }
  deriving (Generic, Show)

instance ToJSON UploadTable
instance FromJSON UploadTable

data UploadStrategy
  = AlwaysCreate
  | AlwaysMatch [NamedValue]
  | MatchOrCreate [NamedValue]
  deriving (Generic, Show)
instance ToJSON UploadStrategy
instance FromJSON UploadStrategy

data ToMany = ToMany
  { toManyFK :: Text
  , toManyTable :: Text
  , records :: [ToManyRecord]
  }
  deriving (Generic, Show)

instance ToJSON ToMany
instance FromJSON ToMany

data ToOne = ToOne
  { toOneFK :: Text
  , toOneTable :: UploadTable
  }
  deriving (Generic, Show)

instance ToJSON ToOne
instance FromJSON ToOne

data ToManyRecord = ToManyRecord
  { filters :: [NamedValue]
  , staticValues :: [NamedValue]
  , toOneTables :: [ToOne]
  , mappingItems :: [MappingItem]
  }
  deriving (Generic, Show)

instance ToJSON ToManyRecord
instance FromJSON ToManyRecord
