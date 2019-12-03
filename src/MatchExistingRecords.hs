module MatchExistingRecords (matchExistingRecords, clean, findExistingRecords, useFirst, flagNewRecords) where

import Prelude ((<$>), fmap, (<>), ($))

import Data.Maybe (fromMaybe)
import Control.Monad (forM_)
import Control.Newtype.Generics (unpack)

import SQL (MonadSQL, Statement(..), execute)
import SQLSmart (in_, using, locate, update, scalarSubQuery, startTransaction, rollback, insertValues, (@=), project, asc, orderBy, queryDistinct, stringLit, strToDate, nullIf, floatLit, leftJoin, inSubQuery, notInSubQuery, suchThat, row, (<=>), subqueryAs, starFrom, plus, selectAs, insertFrom, setUserVar, rawExpr, alias, and, as, equal, (@@), on, table, join, from, select, query, intLit, having, not, null, groupBy, max, delete)
import UploadPlan (WorkbenchId(..), UploadPlan(..), columnName, UploadStrategy(..), ToOne(..), UploadTable(..), ToMany(..), ToManyRecord, NamedValue(..), ToManyRecord(..), ColumnType(..))
import Common (maybeApply, rowsWithValuesFor, rowsFromWB, joinToManys, toOneMappingItems, toManyMappingItems, strategyToWhereClause, parseMappingItem, MappingItem(..))


clean :: MonadSQL m => UploadPlan -> m ()
clean (UploadPlan {workbenchId, templateId}) = do
  execute $ UpdateStatement $
    update [table "workbenchrow"] [("uploadstatus", intLit 0)]
    `suchThat` (project "workbenchid" `equal` (intLit $ unpack workbenchId))

  execute $ DeleteStatement $
    delete "workbenchdataitem"
    `suchThat`
    ( project "workbenchtemplatemappingitemid" `inSubQuery`
      ( query [ select $ project "workbenchtemplatemappingitemid" ]
        `from` [table "workbenchtemplatemappingitem"]
        `suchThat` (
          (project "metadata" `equal` stringLit "generated")
          `and` (project "workbenchtemplateid" `equal` (intLit $ unpack templateId))
          )
      )
    )

matchExistingRecords :: MonadSQL m => UploadPlan -> m ()
matchExistingRecords (UploadPlan {uploadTable, workbenchId}) = matchRecords workbenchId uploadTable

skipDegenerateRecords :: MonadSQL m => WorkbenchId -> m ()
skipDegenerateRecords (WorkbenchId workbenchId) = execute $ UpdateStatement $
  update
  [(table "workbenchrow" `as` r)
   `join` (table "workbenchdataitem" `as` i)
   `on` ( ((r @@ "workbenchrowid") `equal` (i @@ "workbenchrowid"))
          `and` (r @@ "workbenchid" `equal` (intLit workbenchId))
          `and` (locate (stringLit ",") $ i @@ "celldata" )
        )
   `join` (table "workbenchtemplatemappingitem" `as` mi)
   `on` ( (i @@ "workbenchtemplatemappingitemid" `equal` (mi @@ "workbenchtemplatemappingitemid"))
          `and` (mi @@ "metadata" `equal` (stringLit "generated"))
        )
  ]
  [("uploadstatus", intLit 1)]
  where
    r = alias "r"
    i = alias "i"
    mi = alias "mi"

useFirst :: MonadSQL m => UploadTable -> m ()
useFirst (UploadTable {idMapping}) = execute $ UpdateStatement $
  update
  [(table "workbenchdataitem" `as` i)
  `join` (table "workbenchtemplatemappingitem" `as` mi)
  `using` ["workbenchtemplatemappingitemid"]
  ]
  [("celldata", rawExpr "left(celldata, locate(',', celldata) - 1)")]
  `suchThat`
  ((locate (stringLit ",") $ project "celldata") `and` (project "workbenchtemplatemappingitemid" `equal` wbtmiId))
  where
    i = alias "i"
    mi = alias "mi"
    wbtmiId = fromMaybe null $ intLit <$> idMapping

matchRecords :: MonadSQL m => WorkbenchId -> UploadTable -> m ()
matchRecords wbId uploadTable = do
  matchToOnes wbId uploadTable
  matchToManys wbId uploadTable

  findExistingRecords wbId uploadTable
  useFirst uploadTable
  flagNewRecords wbId uploadTable

matchToManys :: MonadSQL m => WorkbenchId -> UploadTable -> m ()
matchToManys wbId ut = do
  forM_ (toManyTables ut) $ \(ToMany {records}) ->
    forM_ records $ \(ToManyRecord {toOneTables}) ->
      forM_ toOneTables $ \toOne -> matchToManyToOne wbId toOne

matchToManyToOne :: MonadSQL m => WorkbenchId -> ToOne -> m ()
matchToManyToOne wbId (ToOne {toOneTable}) = do
  matchToOnes wbId toOneTable
  matchToManys wbId toOneTable

  findExistingRecords wbId toOneTable
  useFirst toOneTable
  flagNewRecords wbId toOneTable

matchToOnes :: MonadSQL m => WorkbenchId -> UploadTable -> m ()
matchToOnes wbId (UploadTable {toOneTables}) =
  forM_ toOneTables $ \(ToOne {toOneTable}) -> do
    matchToOnes wbId toOneTable
    matchToManys wbId toOneTable

    findExistingRecords wbId toOneTable
    useFirst toOneTable
    flagNewRecords wbId toOneTable

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
    st = maybeApply suchThat
    mappingItems' = fmap (parseMappingItem t) mappingItems <> toOneMappingItems ut t <> toManyMappingItems ut
    valuesFromWB = row $ fmap (\(MappingItem {selectFromWBas}) -> wb @@ selectFromWBas) mappingItems'
    valuesFromTable = row $ fmap (\(MappingItem {tableAlias, tableColumn}) -> tableAlias @@ tableColumn) mappingItems'
    wbSubQuery = subqueryAs wb $ rowsFromWB (intLit wbId) mappingItems' excludeRows
    excludeRows = rowsWithValuesFor $ fromMaybe null $ intLit <$> idMapping

