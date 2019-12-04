module MatchRecords where

import Prelude (head, foldl1, (<$>), Maybe(..), pure, ($), (==), undefined, (<>), fmap)
import Control.Monad (forM_)
import Data.Maybe (fromMaybe)
import qualified Data.List as L
import qualified Data.List.Extra as L
import UploadPlan (WorkbenchId(..), UploadPlan(..), MappingItem(..), UploadStrategy(..), ToManyRecord(..), ToMany(..), ToOne(..), UploadTable(..))
import SQL (QueryExpr)
import MonadSQL (MonadSQL(..))
import SQLSmart (startTransaction, createTempTable, union, intLit, null, alias, rollback)
import MatchExistingRecords (flagNewRecords, useFirst, findExistingRecords)
import Common (parseMappingItem, newValuesFromWB)

reconcileMappingItems :: [UploadTable] -> [(UploadTable, [Maybe MappingItem])]
reconcileMappingItems uploadTables = do
  ut@(UploadTable {mappingItems}) <- uploadTables
  let reconciled = do
        column <- columns
        pure $ L.find (\(MappingItem {columnName}) -> columnName == column) mappingItems
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

valuesFromWB :: WorkbenchId -> UploadTable -> [Maybe MappingItem] -> QueryExpr
valuesFromWB (WorkbenchId wbId) (UploadTable {idMapping}) mappingItems =
  newValuesFromWB (intLit wbId) (fromMaybe null $ intLit <$> idMapping) (convert <$> mappingItems)
  where
     convert = fmap $ parseMappingItem (alias "unused")
