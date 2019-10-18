module Upload where

import Data.Text (pack, Text)
import Data.List.Index (ifoldl, imap)

import SQL (Statement, Alias, Alias, TableRef, Alias, Alias, Expr, QueryExpr, Script(..))
import SQLSmart ((@=), project, asc, orderBy, queryDistinct, stringLit, strToDate, nullIf, floatLit, leftJoin, notInSubQuery, suchThat, row, (<=>), userVar, subqueryAs, starFrom, plus, selectAs, insertFrom, setUserVar, rawExpr, alias, and, as, equal, (@@), on, table, join, from, select, query, intLit)
import qualified UploadPlan as UP
import UploadPlan (UploadStrategy(..), ToOne(..), UploadTable(..), ToMany(..), ToManyRecord, NamedValue(..), ToManyRecord(..), ColumnType(..))

import Prelude (foldl, fmap, Int, (<>), (.), Maybe(..), ($))
import qualified Prelude

show = pack . Prelude.show

data MappingItem = MappingItem
  { tableColumn :: Text
  , tableAlias :: Alias
  , selectFromWBas :: Text
  , columnType :: UP.ColumnType
  , mappingId :: Expr
  }


parseMappingItem :: Alias -> UP.MappingItem -> MappingItem
parseMappingItem t (UP.MappingItem {id, columnName, columnType}) = MappingItem
  { mappingId = intLit $ id
  , tableAlias = t
  , columnType = columnType
  , selectFromWBas = columnName
  , tableColumn = columnName
  }


rowsWithValuesFor :: Expr -> QueryExpr
rowsWithValuesFor workbenchtemplatemappingitemid =
  query [select $ r @@ "workbenchrowid"]
  `from`
  [ table "workbenchrow" `as` r
    `join` (table "workbenchdataitem" `as` d)
    `on`
    (((d @@ "workbenchrowid") `equal` (r @@ "workbenchrowid"))
     `and`
     ((d @@ "workbenchtemplatemappingitemid") `equal` workbenchtemplatemappingitemid)
    )
  ]
  where
    r = alias "r"
    d = alias "d"


maybeApply :: forall a b. (a -> b -> a) -> a -> Maybe b -> a
maybeApply f a mb =
  case mb of
    Just b -> f a b
    Nothing -> a

findNewRecords :: Expr -> UploadTable -> Script
findNewRecords wbTemplateMappingItemId table@(UploadTable {tableName, idColumn, strategy, mappingItems}) = Script $
  [ setUserVar "new_id" $ rawExpr "last_insert_id()"
  , setUserVar "row_number" $ intLit 0
  , insertFrom "workbenchdataitem" ["workbenchrowid", "celldata", "rownumber", "workbenchtemplatemappingitemid"] $
    query
    [ select $ wbRow @@ "workbenchrowid"
    , select $ (valuesWithId @@ idColumn)
    , select $ wbRow @@ "rownumber"
    , select $ wbTemplateMappingItemId
    ] `from`
    [ (subqueryAs wbRow $ rowsFromWB (userVar "workbenchid") mappingItems' excludeRows)
      `join`
      (subqueryAs valuesWithId $
       query
       [ selectAs idColumn $ (userVar "row_number") `plus` (userVar "new_id")
       , select $ "row_number" @= ((userVar "row_number") `plus` (intLit 1))
       , starFrom newValues
       ] `from`
       [ subqueryAs newValues $ valuesFromWB (userVar "workbenchid") mappingItems' excludeRows ]
      ) `on` ( newVals <=> wbVals )
    ]
  ]
  where
    t = alias "t"
    wbRow = alias "wbrow"
    newValues = alias "newvalues"
    valuesWithId = alias "valueswithid"
    mappingItems' = fmap (parseMappingItem t) mappingItems <> toOneMappingItems table t <> toManyMappingItems table
    newVals = row $ fmap (\(MappingItem {selectFromWBas}) -> valuesWithId @@ selectFromWBas) mappingItems'
    wbVals = row $ fmap (\(MappingItem {selectFromWBas}) -> wbRow @@ selectFromWBas) mappingItems'
    excludeRows = rowsWithValuesFor wbTemplateMappingItemId

findExistingRecords :: Expr -> UploadTable -> Statement
findExistingRecords wbTemplateMappingItemId ut@(UploadTable {tableName, idColumn, strategy, mappingItems}) =
  insertFrom "workbenchdataitem" ["workbenchrowid", "celldata", "rownumber", "workbenchtemplatemappingitemid"] $
  query
  [ selectAs "rowid" $ wb @@ "workbenchrowid"
  , selectAs "id" $ t @@ idColumn
  , select $ wb @@ "rownumber"
  , select $ wbTemplateMappingItemId
  ] `from`
  [ ( joinToManys t ut (table tableName `as` t) ) `join` wbSubQuery `on` (valuesFromWB <=> valuesFromTable)]
  `st` strategyToWhereClause strategy t
  where
    t = alias "t"
    wb = alias "wb"
    st = maybeApply suchThat
    mappingItems' = fmap (parseMappingItem t) mappingItems <> toOneMappingItems ut t <> toManyMappingItems ut
    valuesFromWB = row $ fmap (\(MappingItem {selectFromWBas}) -> wb @@ selectFromWBas) mappingItems'
    valuesFromTable = row $ fmap (\(MappingItem {tableAlias, tableColumn}) -> tableAlias @@ tableColumn) mappingItems'
    wbSubQuery = subqueryAs wb $ rowsFromWB (userVar "workbenchid") mappingItems' excludeRows
    excludeRows = rowsWithValuesFor wbTemplateMappingItemId

strategyToWhereClause :: UploadStrategy -> Alias -> Maybe Expr
strategyToWhereClause strategy t = case strategy of
  AlwaysCreate -> Nothing
  AlwaysMatch values -> Just $ matchAll values
  MatchOrCreate values -> Just $ matchAll values
  where
    matchAll values = (row $ fmap (\v -> t @@ (column v)) values) <=> (row $ fmap (\v -> rawExpr (value v)) values)


toManyMappingItems :: UploadTable -> [MappingItem]
toManyMappingItems (UploadTable {toManyTables}) = do
  (ToMany {toManyFK, toManyTable, records}) <- toManyTables
  mappingItems <- imap (toManyRecordMappingItems toManyTable) records
  mappingItems

toManyRecordMappingItems :: Text -> Int -> ToManyRecord -> [MappingItem]
toManyRecordMappingItems tableName index (ToManyRecord {mappingItems, toOneTables}) =
  fmap (toManyToOneMappingItems tableName index) toOneTables
  <> fmap (parseToManyMappingItem tableName index) mappingItems


toManyToOneMappingItems :: Text -> Int -> ToOne -> MappingItem
toManyToOneMappingItems tableName index (ToOne {toOneFK, toOneTable}) = MappingItem
   { selectFromWBas = tableName <> (show index) <> toOneFK
   , columnType = IntType
   , mappingId = userVar $ toManyIdColumnVar tableName index toOneFK
   , tableAlias = alias $ tableName <> (show index)
   , tableColumn = toOneFK
   }

parseToManyMappingItem :: Text -> Int -> UP.MappingItem -> MappingItem
parseToManyMappingItem tableName index (UP.MappingItem {columnName, columnType, id}) = MappingItem
  { selectFromWBas = tableName <> ( show index) <> columnName
  , columnType = columnType
  , mappingId = intLit id
  , tableAlias = alias $ tableName <> ( show index)
  , tableColumn = columnName
  }

toOneMappingItems :: UploadTable -> Alias -> [MappingItem]
toOneMappingItems (UploadTable {toOneTables, tableName}) t = fmap (\(ToOne {toOneFK, toOneTable}) -> MappingItem
  { tableColumn = toOneFK
  , columnType = IntType
  , mappingId = userVar $ toOneIdColumnVar tableName toOneFK
  , tableAlias = t
  , selectFromWBas = toOneFK
  }) toOneTables

toOneIdColumnVar :: Text -> Text -> Text
toOneIdColumnVar tableName foreignKey = tableName <> "_" <> foreignKey

toManyIdColumnVar :: Text -> Int -> Text -> Text
toManyIdColumnVar tableName index foreignKey = tableName <> ( show index) <> "_" <> foreignKey

joinToManys :: Alias -> UploadTable -> TableRef -> TableRef
joinToManys t ut tr =
  foldl (joinToManyTable t) tr (toManyTables ut)

joinToManyTable :: Alias -> TableRef -> ToMany -> TableRef
joinToManyTable t tr (ToMany {toManyFK, toManyTable, records}) =
  ifoldl (joinToMany t toManyTable toManyFK) tr records

joinToMany :: Alias -> Text -> Text -> TableRef -> Int -> ToManyRecord -> TableRef
joinToMany t tableName foreignKey tr index (ToManyRecord {filters}) =
  tr `leftJoin` (table tableName `as` a) `on` foldl and usingForeignKey filterExprs
  where
    a = alias $ tableName <> ( show index)
    usingForeignKey = (a @@ foreignKey) `equal` (t @@ foreignKey)
    filterExprs = fmap (\(NamedValue {column, value}) -> (a @@ column) `equal` (rawExpr value)) filters


rowsFromWB :: Expr -> [MappingItem] -> QueryExpr -> QueryExpr
rowsFromWB wbId mappingItems excludeRows =
  query (imap selectWBVal mappingItems <> [select $ r @@ "workbenchrowid", select $ r @@ "rownumber"])
  `from`
  [ ifoldl joinWBCell (table "workbenchrow" `as` r) mappingItems ]
  `suchThat`
  (((r @@ "workbenchid") `equal` wbId) `and` ((r @@ "workbenchrowid") `notInSubQuery` excludeRows))
  where
    r = alias "r"
    c i = alias $ "c" <> ( show i)
    selectWBVal i item = selectAs (selectFromWBas item) $ parseValue (columnType item) ((c i) @@ "celldata")
    joinWBCell :: TableRef -> Int -> MappingItem -> TableRef
    joinWBCell left i item =
      left `leftJoin` (table "workbenchdataitem" `as` c i)
      `on`
      ( ((c i @@ "workbenchrowid") `equal` (r @@ "workbenchrowid"))
        `and` (((c i) @@ "workbenchtemplatemappingitemid") `equal` (mappingId item))
      )

valuesFromWB :: Expr -> [MappingItem] -> QueryExpr -> QueryExpr
valuesFromWB wbId mappingItems excludeRows =
  queryDistinct (imap selectWBVal mappingItems)
  `from`
  [ ifoldl joinWBCell (table "workbenchrow" `as` r) mappingItems ]
  `suchThat`
  (((r @@ "workbenchid") `equal` wbId) `and` ((r @@ "workbenchrowid") `notInSubQuery` excludeRows))
  `orderBy`
  (fmap (\i -> asc $ project $ selectFromWBas i) mappingItems )
  where
    r = alias "r"
    c i = alias $ "c" <> ( show i)
    selectWBVal i item = selectAs (selectFromWBas item) $ parseValue (columnType item) $ c i @@ "celldata"
    joinWBCell :: TableRef -> Int -> MappingItem -> TableRef
    joinWBCell left i item =
      left `leftJoin` (table "workbenchdataitem" `as` c i)
      `on`
      ( ((c i @@ "workbenchrowid") `equal` (r @@ "workbenchrowid"))
        `and` (((c i) @@ "workbenchtemplatemappingitemid") `equal` (mappingId item))
      )



parseValue :: ColumnType -> Expr -> Expr
parseValue StringType value = nullIf value (stringLit "")
parseValue DoubleType value = nullIf value (stringLit "") `plus` (floatLit 0.0)
parseValue IntType value = nullIf value (stringLit "") `plus` (intLit 0)
parseValue DecimalType value = nullIf value (stringLit "")
parseValue (DateType format) value = strToDate value $ stringLit format
