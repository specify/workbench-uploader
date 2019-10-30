module UploadPlan where

import Data.Text (Text)
import GHC.Generics
import Control.Newtype.Generics (Newtype)

data ColumnType
  = StringType
  | DoubleType
  | IntType
  | DecimalType
  | DateType Text
  deriving (Show, Eq)

newtype WorkbenchId = WorkbenchId Int deriving (Generic, Show, Eq)
instance Newtype WorkbenchId

newtype TemplateId = TemplateId Int deriving (Generic, Show, Eq)
instance Newtype TemplateId

data UploadPlan = UploadPlan
  { workbenchId :: WorkbenchId
  , templateId :: TemplateId
  , uploadTable :: UploadTable
  }
  deriving (Show)

data NamedValue = NamedValue { column :: Text, value :: Text }
  deriving (Show)

data MappingItem = MappingItem {columnName :: Text, columnType :: ColumnType, id :: Int}
  deriving (Show)

data UploadTable = UploadTable
  { tableName :: Text
  , idColumn :: Text
  , strategy :: UploadStrategy
  , mappingItems :: [MappingItem]
  , staticValues :: [NamedValue]
  , toOneTables :: [ToOne]
  , toManyTables :: [ToMany]
  }
  deriving (Show)

data UploadStrategy
  = AlwaysCreate
  | AlwaysMatch [NamedValue]
  | MatchOrCreate [NamedValue]
  deriving (Show)

data ToMany = ToMany { toManyFK :: Text, toManyTable :: Text, records :: [ToManyRecord] }
  deriving (Show)

data ToOne = ToOne { toOneFK :: Text, toOneTable :: UploadTable }
  deriving (Show)

data ToManyRecord = ToManyRecord
  { filters :: [NamedValue]
  , staticValues :: [NamedValue]
  , toOneTables :: [ToOne]
  , mappingItems :: [MappingItem]
  }
  deriving (Show)
