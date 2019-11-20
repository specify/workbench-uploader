module Common where

import Data.String (fromString)
import Data.Text (pack, unpack, Text)
import Data.List.Index (ifoldl, imap)
import Data.Maybe (fromMaybe)
import Database.MySQL.Simple.QueryResults (QueryResults)
import Database.MySQL.Simple (Connection)
import qualified Database.MySQL.Simple as MySQL
import Control.Monad (forM_)

import SQLRender (renderQuery, renderSQL, renderStatement)
import SQL (Statement(..), Alias, Alias, TableRef, Alias, Alias, Expr, QueryExpr)
import SQLSmart
 ( (<=>)
 , (@@)
 , alias
 , and
 , as
 , asc
 , equal
 , floatLit
 , from
 , groupBy
 , having
 , intLit
 , join
 , leftJoin
 , not
 , notInSubQuery
 , null
 , nullIf
 , on
 , orderBy
 , plus
 , project
 , query
 , queryDistinct
 , rawExpr
 , row
 , select
 , selectAs
 , strToDate
 , stringLit
 , suchThat
 , table
 , using
 )
import qualified UploadPlan as UP
import UploadPlan (columnName, UploadStrategy(..), ToOne(..), UploadTable(..), ToMany(..), ToManyRecord, NamedValue(..), ToManyRecord(..), ColumnType(..))

import Prelude ((<$>), IO, const, Show, foldl, fmap, Int, (<>), (.), Maybe(..), ($))
import qualified Prelude


show :: forall a. Show a => a -> Text
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


remark :: Text -> Statement
remark message = QueryStatement $ query [selectAs "Message" $ stringLit message]


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
    r = alias "exclude"
    d = alias "d"


maybeApply :: forall a b. (a -> b -> a) -> a -> Maybe b -> a
maybeApply f a mb =
  case mb of
    Just b -> f a b
    Nothing -> a


strategyToWhereClause :: UploadStrategy -> Alias -> Maybe Expr
strategyToWhereClause strategy t = case strategy of
  AlwaysCreate -> Nothing
  AlwaysMatch values -> Just $ matchAll values
  MatchOrCreate values -> Just $ matchAll values
  where
    matchAll values = (row $ fmap (\v -> t @@ (column v)) values) <=> (row $ fmap (\v -> rawExpr (value v)) values)


toManyMappingItems :: UploadTable -> [MappingItem]
toManyMappingItems (UploadTable {toManyTables}) = do
  (ToMany {toManyTable, records}) <- toManyTables
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
   , mappingId = fromMaybe null $ intLit <$> idMapping toOneTable
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
toOneMappingItems (UploadTable {toOneTables}) t = fmap (\(ToOne {toOneFK, toOneTable}) -> MappingItem
  { tableColumn = toOneFK
  , columnType = IntType
  , mappingId = fromMaybe null $ intLit <$> idMapping toOneTable
  , tableAlias = t
  , selectFromWBas = toOneFK
  }) toOneTables

-- toOneIdColumnVar :: Text -> Text -> Text
-- toOneIdColumnVar tableName foreignKey = tableName <> "_" <> foreignKey

-- toManyIdColumnVar :: Text -> Int -> Text -> Text
-- toManyIdColumnVar tableName index foreignKey = tableName <> ( show index) <> "_" <> foreignKey

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
  (((r @@ "workbenchid") `equal` wbId)
   `and` (r @@ "uploadstatus" `equal` intLit 0)
   `and` ((r @@ "workbenchrowid") `notInSubQuery` excludeRows)
  )
  `having`
  (not $ (row $ fmap (\i -> project $ selectFromWBas i) mappingItems) <=> (row $ fmap (const null) mappingItems))
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
  (((r @@ "workbenchid") `equal` wbId)
   `and` (r @@ "uploadstatus" `equal` intLit 0)
   `and` ((r @@ "workbenchrowid") `notInSubQuery` excludeRows)
  )
  `orderBy`
  (fmap (\i -> asc $ project $ selectFromWBas i) mappingItems )
  `having`
  (not $ (row $ fmap (\i -> project $ selectFromWBas i) mappingItems) <=> (row $ fmap (const null) mappingItems))
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

showWB :: Expr -> QueryExpr
showWB wbId =
  query [select $ r @@ "uploadstatus", select $ rawExpr "group_concat(coalesce(json_quote(celldata), 'null') order by vieworder separator ' | ')"]
  `from`
  [(table "workbenchrow" `as` r)
   `join` (table "workbench") `using` ["workbenchid"]
   `join` (table "workbenchtemplatemappingitem" `as` m) `using` ["workbenchtemplateid"]
   `leftJoin` (table "workbenchdataitem" `as` i) `using` ["workbenchrowid", "workbenchtemplatemappingitemid"]
  ]
  `suchThat`
  (r @@ "workbenchid" `equal` wbId)
  `groupBy` [r @@ "workbenchrowid"]
  `orderBy` [asc $ r @@ "rownumber"]
  where
    r = alias "r"
    i = alias "i"
    m = alias "m"

parseValue :: ColumnType -> Expr -> Expr
parseValue StringType value = nullIf value (stringLit "")
parseValue DoubleType value = nullIf value (stringLit "") `plus` (floatLit 0.0)
parseValue IntType value = nullIf value (stringLit "") `plus` (intLit 0)
parseValue IdType value = nullIf value (stringLit "") `plus` (intLit 0)
parseValue DecimalType value = nullIf value (stringLit "")
parseValue (DateType format) value = strToDate value $ stringLit format


execute :: Connection -> [Statement] -> IO ()
execute conn statements =
  forM_ statements $ \s -> MySQL.execute_ conn $ fromString $ unpack $ renderSQL $ renderStatement s


runQuery :: (QueryResults r) => Connection -> QueryExpr -> IO [r]
runQuery conn q = MySQL.query_ conn $ fromString $ unpack $ renderSQL $ renderQuery q

