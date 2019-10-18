module UploadPlan where

import Data.Text (Text)

data ColumnType = StringType
                | DoubleType
                | IntType
                | DecimalType
                | DateType Text

newtype WorkbenchId = WorkbenchId Int
newtype TemplateId = TemplateId Int

data UploadPlan = UploadPlan
  { workbenchId :: WorkbenchId
  , templateId :: TemplateId
  , uploadTable :: UploadTable
  }

data NamedValue = NamedValue { column :: Text, value :: Text }

data MappingItem = MappingItem {columnName :: Text, columnType :: ColumnType, id :: Int}

data UploadTable = UploadTable
  { tableName :: Text
  , idColumn :: Text
  , strategy :: UploadStrategy
  , mappingItems :: [MappingItem]
  , staticValues :: [NamedValue]
  , toOneTables :: [ToOne]
  , toManyTables :: [ToMany]
  }

data UploadStrategy = AlwaysCreate
                    | AlwaysMatch [NamedValue]
                    | MatchOrCreate [NamedValue]

data ToMany = ToMany { toManyFK :: Text, toManyTable :: Text, records :: [ToManyRecord] }

data ToOne = ToOne { toOneFK :: Text, toOneTable :: UploadTable }

data ToManyRecord = ToManyRecord
  { filters :: [NamedValue]
  , staticValues :: [NamedValue]
  , toOneTables :: [ToOne]
  , mappingItems :: [MappingItem]
  }
