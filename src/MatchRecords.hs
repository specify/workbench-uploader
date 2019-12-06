module MatchRecords where

import Data.Text (Text)
import Prelude (fst, (.), const, Int, head, foldl1, (<$>), Maybe(..), pure, ($), (==), undefined, (<>), fmap)
import Control.Monad (forM_)
import Data.Maybe (isJust, catMaybes, fromMaybe)
import Data.List.Index (ifoldl, imap)
import qualified Data.List as L
import qualified Data.List.Extra as L
import Data.Tuple.Extra (fst3)
import UploadPlan (NamedValue(..), ColumnType, WorkbenchId(..), UploadPlan(..), MappingItem(..), UploadStrategy(..), ToManyRecord(..), ToMany(..), ToOne(..), UploadTable(..))
import SQL (ColumnName(..), Alias, SelectTerm, TableRef, Expr, QueryExpr)
import MonadSQL (MonadSQL(..))
import SQLSmart
 (rollback, union, createTempTable, startTransaction,  (<=>)
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
import Common (mappingId, parseValue, selectFromWBas, parseMappingItem)
import qualified Common


data ColumnDescriptor = ColumnDescriptor
  { colName :: Text
  , colSource :: ColumnSource
  , position :: Int
  }

data ColumnSource = WBValue (Int, ColumnType) | StaticValue Expr


columnDescriptorsForTable :: [ColumnName] -> UploadTable -> [ColumnDescriptor]
columnDescriptorsForTable columns (UploadTable {mappingItems, staticValues}) = do
  (i, ColumnName c) <- L.zip [0 ..] columns
  case L.find ((==) c . columnName) mappingItems of
    Just (MappingItem {id, columnType}) ->
      pure $ ColumnDescriptor {colName = c, colSource = WBValue (id, columnType), position = i}
    Nothing ->
      case L.find ((==) c . column) staticValues of
        Just (NamedValue {value}) ->
          pure $ ColumnDescriptor {colName = c, colSource = StaticValue $ rawExpr value, position = i}
        Nothing ->
          pure $ ColumnDescriptor {colName = c, colSource = StaticValue null, position = i}

columnsForTableGroup :: [UploadTable] -> [ColumnName]
columnsForTableGroup uploadTables = ColumnName <$> (L.nub $ L.sort $ (staticColumns <> mappingColumns))
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
    let columns = columnsForTableGroup group
    let queries = (\ut -> valuesFromWB workbenchId ut (columnDescriptorsForTable columns ut)) <$> group
    execute $ createTempTable ("newvalues" <> tableName (head group)) $ foldl1 union queries
  execute $ rollback

valuesFromWB :: WorkbenchId -> UploadTable -> [ColumnDescriptor] -> QueryExpr
valuesFromWB (WorkbenchId wbId) (UploadTable {idMapping}) descriptors =
  newValuesFromWB (intLit wbId) (nullable intLit idMapping) descriptors

newValuesFromWB :: Expr -> Expr -> [ColumnDescriptor] -> QueryExpr
newValuesFromWB wbId idMappingId descriptors =
  queryDistinct (selectWBVal <$> descriptors)
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
  (not $ (row $ (project . fst3) <$> wbCols) <=> (row $ (const null) <$> wbCols))
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

nullable :: (a -> Expr) -> Maybe a -> Expr
nullable toExpr v = fromMaybe null $ toExpr <$> v

