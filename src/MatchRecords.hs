module MatchRecords where

import Prelude (Maybe(..), pure, ($), (==), undefined, (<>))
import qualified Data.List as L
import qualified Data.List.Extra as L
import UploadPlan (MappingItem(..), UploadStrategy(..), ToManyRecord(..), ToMany(..), ToOne(..), UploadTable(..))


reconcileMappingItems :: [[MappingItem]] -> [[Maybe MappingItem]]
reconcileMappingItems mappingItemsSets = do
  mappingItems <- mappingItemsSets
  pure $ do
    column <- columns
    pure $ L.find (\(MappingItem {columnName}) -> columnName == column) mappingItems
  where
    columns = L.nub $ L.sort $ do
      mappingItems <- mappingItemsSets
      (MappingItem {columnName}) <- mappingItems
      pure $ columnName


uploadGroups :: UploadTable -> [[UploadTable]]
uploadGroups ut =
  L.groupSortOn (\UploadTable {tableName, mappingItems} -> (tableName, L.length mappingItems))
  $ leafTables ut

leafTables :: UploadTable -> [UploadTable]
leafTables uploadTable@(UploadTable {toOneTables, toManyTables}) =
  case (toOneTables, toManyTables) of
    ([], []) -> [uploadTable]
    _ -> leafTablesFromToOnes toOneTables <> leafTablesFromToManys toManyTables

leafTablesFromToOnes :: [ToOne] -> [UploadTable]
leafTablesFromToOnes toOnes = do
  (ToOne {toOneTable}) <- toOnes
  leafTables toOneTable

leafTablesFromToManys :: [ToMany] -> [UploadTable]
leafTablesFromToManys toManys = do
  (ToMany {toManyTable, records}) <- toManys
  (ToManyRecord {mappingItems, staticValues, toOneTables}) <- records
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


