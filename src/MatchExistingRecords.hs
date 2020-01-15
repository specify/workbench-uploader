module MatchExistingRecords (clean, flagNewRecords, useFirst, findExistingRecords) where

import Prelude (foldl, (<$>), fmap, (<>), ($))

import Data.Maybe (fromMaybe)
import Control.Newtype.Generics (unpack)

import MonadSQL (MonadSQL, execute)
import SQLSmart
import UploadPlan (WorkbenchId(..), UploadPlan(..), UploadStrategy(..), ToOne(..), UploadTable(..), ToMany(..), ToManyRecord, NamedValue(..), ToManyRecord(..))
import Common (rowsWithValuesFor, rowsFromWB, joinToManys, toOneMappingItems, toManyMappingItems, strategyToWhereClause, parseMappingItem, MappingItem(..))


clean :: MonadSQL m => UploadPlan -> m ()
clean (UploadPlan {workbenchId, templateId}) = do
  execute $
    update [table "workbenchrow"] [(project "uploadstatus", intLit 0)]
    `when` (project "workbenchid" `equal` (intLit $ unpack workbenchId))

  execute $
    delete "workbenchdataitem"
    `when`
    ( project "workbenchtemplatemappingitemid" `inSubQuery`
      ( query [ select $ project "workbenchtemplatemappingitemid" ]
        `from` [table "workbenchtemplatemappingitem"]
        `when` (
          (project "metadata" `equal` stringLit "generated")
          `and` (project "workbenchtemplateid" `equal` (intLit $ unpack templateId))
          )
      )
    )


useFirst :: MonadSQL m => UploadTable -> m ()
useFirst (UploadTable {idMapping}) = execute $
  update
  [(table "workbenchdataitem" `as` i)
  `join` (table "workbenchtemplatemappingitem" `as` mi)
  `using` ["workbenchtemplatemappingitemid"]
  ]
  [(project "celldata", rawExpr "left(celldata, locate(',', celldata) - 1)")]
  `when`
  ((locate (stringLit ",") $ project "celldata") `and` (project "workbenchtemplatemappingitemid" `equal` wbtmiId))
  where
    i = alias "i"
    mi = alias "mi"
    wbtmiId = fromMaybe null $ intLit <$> idMapping

flagNewRecords :: MonadSQL m => WorkbenchId -> UploadTable -> m ()
flagNewRecords (WorkbenchId wbId) ut@(UploadTable {mappingItems, idMapping}) =
  execute $ insertFrom "workbenchdataitem" ["workbenchrowid", "celldata", "rownumber", "workbenchtemplatemappingitemid"] $
    query
    [ select $ wbRow @@ "workbenchrowid"
    , select $ stringLit "new"
    , select $ wbRow @@ "rownumber"
    , select $ fromMaybe null $ intLit <$> idMapping
    ] `from`
    [ subqueryAs wbRow $ rowsFromWB (intLit wbId) mappingItems' excludeRows ]
  where
    t = alias "t"
    wbRow = alias "wbrow"
    mappingItems' = fmap (parseMappingItem t) mappingItems <> toOneMappingItems ut t <> toManyMappingItems ut
    excludeRows = rowsWithValuesFor $ fromMaybe null $ intLit <$> idMapping


findExistingRecords :: MonadSQL m => WorkbenchId -> UploadTable -> m ()
findExistingRecords (WorkbenchId wbId) ut@(UploadTable {tableName, idColumn, strategy, mappingItems, idMapping}) =
  execute $ insertFrom "workbenchdataitem" ["workbenchrowid", "celldata", "rownumber", "workbenchtemplatemappingitemid"] $
  query
  [ selectAs "rowid" $ wb @@ "workbenchrowid"
  , selectAs "ids" $ rawExpr $ "group_concat(t." <> idColumn <> ")"--t @@ idColumn
  , selectAs "rownumber" $ wb @@ "rownumber"
  , selectAs "wbtmid" $ fromMaybe null $ intLit <$> idMapping
  ] `from`
  [ ( joinToManys t ut (table tableName `as` t) ) `join` wbSubQuery `on` (valuesFromWB <=> valuesFromTable) ]
  `st` strategyToWhereClause strategy t
  `groupBy` [project "rowid", project "rownumber", project "wbtmid"]
  where
    t = alias "t"
    wb = alias "wb"
    st = foldl when
    mappingItems' = fmap (parseMappingItem t) mappingItems <> toOneMappingItems ut t <> toManyMappingItems ut
    valuesFromWB = row $ fmap (\(MappingItem {selectFromWBas}) -> wb @@ selectFromWBas) mappingItems'
    valuesFromTable = row $ fmap (\(MappingItem {tableAlias, tableColumn}) -> tableAlias @@ tableColumn) mappingItems'
    wbSubQuery = subqueryAs wb $ rowsFromWB (intLit wbId) mappingItems' excludeRows
    excludeRows = rowsWithValuesFor $ fromMaybe null $ intLit <$> idMapping

