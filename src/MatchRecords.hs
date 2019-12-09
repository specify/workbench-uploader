module MatchRecords where

import Data.Text (Text)
import Prelude ((<$), fst, (.), const, Int, head, foldl1, (<$>), Maybe(..), pure, ($), (==), undefined, (<>), fmap, maybe)
import Control.Monad (forM_)
import Control.Newtype.Generics (unpack)
import Data.Maybe (isJust, catMaybes, fromMaybe)
import Data.List.Index (ifoldl, imap)
import qualified Data.List as L
import qualified Data.List.Extra as L
import Data.Tuple.Extra (fst3)
import Database.MySQL.Simple.Types (Only(..))

import UploadPlan (NamedValue(..), ColumnType, WorkbenchId(..), UploadPlan(..), MappingItem(..), UploadStrategy(..), ToManyRecord(..), ToMany(..), ToOne(..), UploadTable(..))
import SQL (Statement, ColumnName(..), Alias, SelectTerm, TableRef, Expr, QueryExpr)
import MonadSQL (MonadSQL(..))
import SQLSmart
 (insertFrom, rollback, union, createTempTable, startTransaction,  (<=>)
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
import MatchExistingRecords (flagNewRecords, useFirst, findExistingRecords)
import Common (tableColumn, mappingId, parseValue, selectFromWBas, parseMappingItem)
import qualified Common


data ColumnDescriptor = ColumnDescriptor
  { colName :: Text
  , colSource :: ColumnSource
  , position :: Int
  }

data ColumnSource = WBValue (Int, ColumnType) | StaticValue Expr


columnDescriptorsForTable :: [Text] -> UploadTable -> [ColumnDescriptor]
columnDescriptorsForTable columns (UploadTable {mappingItems, staticValues}) = do
  (i, c) <- L.zip [0 ..] columns
  case L.find ((==) c . columnName) mappingItems of
    Just (MappingItem {id, columnType}) ->
      pure $ ColumnDescriptor {colName = c, colSource = WBValue (id, columnType), position = i}
    Nothing ->
      case L.find ((==) c . column) staticValues of
        Just (NamedValue {value}) ->
          pure $ ColumnDescriptor {colName = c, colSource = StaticValue $ rawExpr value, position = i}
        Nothing ->
          pure $ ColumnDescriptor {colName = c, colSource = StaticValue null, position = i}

columnsForTableGroup :: [UploadTable] -> [Text]
columnsForTableGroup uploadTables = L.nub $ L.sort (staticColumns <> mappingColumns)
  where
    staticColumns = do
      UploadTable {staticValues} <- uploadTables
      NamedValue {column} <- staticValues
      pure column

    mappingColumns = do
      UploadTable {mappingItems} <- uploadTables
      MappingItem {columnName} <- mappingItems
      pure columnName

uploadGroups :: UploadTable -> [[UploadTable]]
uploadGroups ut =
  L.groupSortOn (\UploadTable {tableName} -> tableName) $ leafTables ut

leafTables :: UploadTable -> [UploadTable]
leafTables uploadTable@(UploadTable {toOneTables, toManyTables}) =
  case (toOneTables, toManyTables) of
    ([], []) -> [uploadTable]
    _ -> leafTablesFromToOnes toOneTables <> leafTablesFromToManys toManyTables

leafTablesFromToOnes :: [ToOne] -> [UploadTable]
leafTablesFromToOnes toOnes = do
  ToOne {toOneTable} <- toOnes
  leafTables toOneTable

leafTablesFromToManys :: [ToMany] -> [UploadTable]
leafTablesFromToManys toManys = do
  ToMany {toManyTable, records} <- toManys
  ToManyRecord {mappingItems, staticValues, toOneTables} <- records
  case toOneTables of
    [] -> [ UploadTable
            { tableName = toManyTable
            , idColumn = undefined
            , idMapping = Nothing
            , strategy = AlwaysCreate
            , mappingItems = mappingItems
            , staticValues = staticValues
            , toOneTables = []
            , toManyTables = []
            }
          ]
    _ -> leafTablesFromToOnes toOneTables


matchLeafRecords :: MonadSQL m => UploadPlan -> m ()
matchLeafRecords up@(UploadPlan {uploadTable, workbenchId}) = do
  let groups = uploadGroups uploadTable
  forM_ groups $ \group -> do
    forM_ group $ \table -> do
      findExistingRecords workbenchId table
      useFirst table
      flagNewRecords workbenchId table

uploadLeafRecords :: MonadSQL m => UploadPlan -> m ()
uploadLeafRecords up@(UploadPlan {uploadTable, workbenchId}) = do
  execute $ startTransaction
  let groups = uploadGroups uploadTable
  forM_ groups $ \group -> do
    let UploadTable {tableName} = head group
    let columns = columnsForTableGroup group
    let queries = (\ut -> valuesFromWB workbenchId ut (columnDescriptorsForTable columns ut)) <$> group
    execute $ createTempTable ("newvalues_" <> tableName) $ foldl1 union queries
    execute $ insertNewRecords tableName columns
    -- lastInsert <- doQuery (query [select $ rawExpr "last_insert_id()"])
    -- let lastInsertId = case lastInsert of
    --       [Only id] -> id :: Int
    -- _a
  execute $ rollback

insertNewRecords :: Text -> [Text] -> Statement
insertNewRecords tableName columns = insertFrom tableName (columns <> extraColumns) $
      query ((select . project <$> columns) <> (select <$> extraValues))
      `from` [table $ "newvalues_" <> tableName]
      `orderBy` (asc . project <$> columns)
  where
    extraColumns = ["version", "timestampcreated", "guid"]
    extraValues = rawExpr <$> ["0", "now()", "uuid()"]


valuesFromWB :: WorkbenchId -> UploadTable -> [ColumnDescriptor] -> QueryExpr
valuesFromWB (WorkbenchId wbId) (UploadTable {idMapping}) descriptors =
  newValuesFromWB (intLit wbId) (maybe null intLit idMapping) descriptors

newValuesFromWB :: Expr -> Expr -> [ColumnDescriptor] -> QueryExpr
newValuesFromWB wbId idMappingId descriptors =
  queryDistinct ((selectAs "idForUpload" null) : (selectWBVal <$> descriptors))
  `from`
  [ L.foldl joinWBCell (table "workbenchrow" `as` r) wbCols
    `join` (table "workbenchdataitem" `as` idCol)
    `on` ((idCol @@ "workbenchtemplatemappingitemid" `equal` idMappingId)
          `and` (idCol @@ "workbenchrowid" `equal` (r @@ "workbenchrowid"))
          `and` (idCol @@ "celldata" `equal` (stringLit "new"))
         )
  ]
  `suchThat`
  (((r @@ "workbenchid") `equal` wbId) `and` (r @@ "uploadstatus" `equal` intLit 0))
  `having`
  (not $ (row $ project . fst3 <$> wbCols) <=> (row $ null <$ wbCols))
  where
    idCol = alias "idcol"
    r = alias "r"
    c i = alias $ "c" <> ( Common.show i)

    wbCols = do
      ColumnDescriptor {colName, position=i, colSource} <- descriptors
      case colSource of
        WBValue (mappingId, _) -> [(colName, i, mappingId)]
        _ -> []


    selectWBVal (ColumnDescriptor {colName, colSource, position=i}) =
      case colSource of
        WBValue (_, colType) -> selectAs colName $ parseValue colType ((c i) @@ "celldata")
        StaticValue expr -> selectAs colName expr

    joinWBCell :: TableRef -> (Text, Int, Int) -> TableRef
    joinWBCell left (_, i, mappingId) =
      left `leftJoin` (table "workbenchdataitem" `as` c i)
      `on`
      ( ((c i @@ "workbenchrowid") `equal` (r @@ "workbenchrowid"))
        `and` (((c i) @@ "workbenchtemplatemappingitemid") `equal` (intLit mappingId))
      )


-- findNewRecords :: Expr -> UploadTable -> [Statement]
-- findNewRecords wbTemplateMappingItemId ut@(UploadTable {idColumn, mappingItems}) =
--   [ setUserVar "new_id" $ rawExpr "last_insert_id()"
--   , setUserVar "row_number" $ intLit 0
--   , UpdateStatement $ update
--     [ (table "workbenchdataitem")
--       `join`
--       (subqueryAs wbRow $ rowsFromWB (userVar "workbenchid") mappingItems' excludeRows) `using` ["workbenchrowid"]
--       `join`
--       (subqueryAs valuesWithId $
--        query
--        [ selectAs idColumn $ (userVar "row_number") `plus` (userVar "new_id")
--        , select $ "row_number" @= ((userVar "row_number") `plus` (intLit 1))
--        , starFrom newValues
--        ] `from`
--        [ subqueryAs newValues $ valuesFromWB (userVar "workbenchid") mappingItems' excludeRows ]
--       ) `on` ( newVals <=> wbVals )
--     ]
--     [("celldata", valuesWithId @@ idColumn)]
--   ]
--   where
--     t = alias "t"
--     wbRow = alias "wbrow"
--     newValues = alias "newvalues"
--     valuesWithId = alias "valueswithid"
--     mappingItems' = fmap (parseMappingItem t) mappingItems <> toOneMappingItems ut t <> toManyMappingItems ut
--     newVals = row $ fmap (\(MappingItem {selectFromWBas}) -> valuesWithId @@ selectFromWBas) mappingItems'
--     wbVals = row $ fmap (\(MappingItem {selectFromWBas}) -> wbRow @@ selectFromWBas) mappingItems'
