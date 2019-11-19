module ExamplePlan where

import Data.Text (pack)
import UploadPlan (TemplateId(..), WorkbenchId(..), MappingItem(..), UploadPlan(..), UploadStrategy(..), ToOne(..), UploadTable(..), ToMany(..), NamedValue(..), ToManyRecord(..), ColumnType(..))

uploadPlan :: UploadPlan
uploadPlan = UploadPlan
  { workbenchId = WorkbenchId 25
  , templateId = TemplateId 25
  , uploadTable = UploadTable
    { tableName = "collectingevent"
    , idColumn = "collectingeventid"
    , idMapping = Nothing
    , strategy = AlwaysCreate
    , mappingItems =
      [ MappingItem {columnName = "remarks", id = 1349, columnType = StringType}
      -- , {columnName = "endDate", id = 1352, columnType = DateType "%d %b %Y"}
      , MappingItem {columnName = "method", id = 1343, columnType = StringType}
      -- , {columnName = "startDate", id = 1346, columnType = DateType "%d %b %Y"}
      , MappingItem {columnName = "verbatimDate", id = 1366, columnType = StringType}
      ]
    , staticValues =
      [ NamedValue { column = "disciplineid", value = "32768" }
      , NamedValue { column = "timestampcreated", value = "now()" }
      , NamedValue { column = "guid", value = "uuid()" }
      ]
    , toOneTables =
      [ ToOne
        { toOneFK = "localityid"
        , toOneTable = UploadTable
          { tableName = "locality"
          , idColumn = "localityid"
          , idMapping = Nothing
          , strategy = MatchOrCreate [NamedValue {column = "disciplineid", value = "32768"}]
          , mappingItems =
              [ MappingItem {columnName = "text1", id = 1359, columnType = StringType}
              , MappingItem {columnName = "lat1text", id = 1340, columnType = StringType}
              , MappingItem {columnName = "localityName", id = 1358, columnType = StringType}
              , MappingItem {columnName = "long1text", id = 1338, columnType = StringType}
              , MappingItem {columnName = "maxElevation", id = 1357, columnType = DoubleType}
              , MappingItem {columnName = "minElevation", id = 1361, columnType = DoubleType}
              , MappingItem {columnName = "originalElevationUnit", id = 1348, columnType = StringType}
              ]
          , staticValues =
              [ NamedValue {column = "srclatlongunit", value = "0"}
              , NamedValue {column = "disciplineid", value = "32768"}
              , NamedValue {column = "timestampcreated", value = "now()"}
              , NamedValue {column = "guid", value = "uuid()" }
              ]
          , toOneTables = []
          , toManyTables = []
          }
        }
      ]
    , toManyTables =
      [ ToMany
        { toManyFK = "collectingeventid"
        , toManyTable = "collector"
        , records = [collectorRecord 0 1351, collectorRecord 1 1339, collectorRecord 2 1353]
        }
      ]
    }
  }

collectorRecord :: Int -> Int -> ToManyRecord
collectorRecord ordernumber mappingId = ToManyRecord
  { filters = [ NamedValue {column = "ordernumber", value = pack $ show ordernumber} ]
  , staticValues =
    [ NamedValue {column = "divisionid", value = "2"}
    , NamedValue {column = "ordernumber", value = pack $ show ordernumber}
    ]
  , mappingItems = []
  , toOneTables =
    [ ToOne
      { toOneFK = "agentid"
      , toOneTable = UploadTable
        { tableName = "agent"
        , idColumn = "agentid"
        , idMapping = Nothing
        , strategy = MatchOrCreate [NamedValue {column = "divisionid", value = "2"}]
        , mappingItems = [MappingItem {columnName = "lastname", id = mappingId, columnType = StringType} ]
        , staticValues =
            [ NamedValue {column = "divisionid", value = "2"}
            , NamedValue {column = "timestampcreated", value = "now()"}
            , NamedValue {column = "guid", value = "uuid()"}
            , NamedValue {column = "agentType", value = "2"}
            ]
        , toOneTables = []
        , toManyTables = []
        }
      }
    ]
  }
