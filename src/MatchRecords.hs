module MatchRecords where

import Data.Text (Text)
import Prelude (fst, (.), const, Int, head, foldl1, (<$>), Maybe(..), pure, ($), (==), undefined, (<>), fmap)
import Control.Monad (forM_)
import Data.Maybe (isJust, catMaybes, fromMaybe)
import Data.List.Index (ifoldl, imap)
import qualified Data.List as L
import qualified Data.List.Extra as L
import UploadPlan (ColumnType, WorkbenchId(..), UploadPlan(..), MappingItem(..), UploadStrategy(..), ToManyRecord(..), ToMany(..), ToOne(..), UploadTable(..))
import SQL (Alias, SelectTerm, TableRef, Expr, QueryExpr)
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
  , mappingIdAndType :: Maybe (Int, ColumnType)
  , position :: Int
  }

reconcileMappingItems :: [UploadTable] -> [(UploadTable, [ColumnDescriptor])]
reconcileMappingItems uploadTables = do
  ut@(UploadTable {mappingItems}) <- uploadTables
  let reconciled = do
        (i, column) <- L.zip [0 ..] columns
        let mappingIdAndType = (\(MappingItem {id, columnType}) -> (id, columnType)) <$> L.find ((==) column . columnName) mappingItems
        pure $ ColumnDescriptor {colName = column, mappingIdAndType = mappingIdAndType, position = i}
  pure (ut, reconciled)
  where
    columns = L.nub $ L.sort $ do
      UploadTable {mappingItems} <- uploadTables
      (MappingItem {columnName}) <- mappingItems
      pure $ columnName


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
    let queries = (\(uploadTable, mappingItems) -> valuesFromWB workbenchId uploadTable mappingItems) <$> (reconcileMappingItems group)
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
  (not $ (row $ (project . colName) <$> wbCols) <=> (row $ (const null) <$> wbCols))
  where
    idCol = alias "idcol"
    r = alias "r"
    c i = alias $ "c" <> ( Common.show i)
    wbCols = L.filter (isJust . mappingIdAndType) descriptors
    selectWBVal (ColumnDescriptor {colName, mappingIdAndType=mit, position=i}) =
      selectAs colName $ nullable (\(_, colType) -> parseValue colType ((c i) @@ "celldata")) mit
    joinWBCell :: TableRef -> ColumnDescriptor -> TableRef
    joinWBCell left (ColumnDescriptor {position=i, mappingIdAndType}) =
      left `leftJoin` (table "workbenchdataitem" `as` c i)
      `on`
      ( ((c i @@ "workbenchrowid") `equal` (r @@ "workbenchrowid"))
        `and` (((c i) @@ "workbenchtemplatemappingitemid") `equal` (nullable intLit $ fst <$> mappingIdAndType))
      )

nullable :: (a -> Expr) -> Maybe a -> Expr
nullable toExpr v = fromMaybe null $ toExpr <$> v

