module MatchExistingRecords (matchExistingRecords) where

import Prelude (fmap, Int, (<>), zip, Maybe(..), ($))

import Data.Text (Text)
import Control.Monad.Writer (execWriter, tell, Writer)
import Control.Monad (forM_)

import SQL (Statement(..), Expr, QueryExpr)
import SQLSmart (using, locate, update, scalarSubQuery, startTransaction, rollback, insertValues, (@=), project, asc, orderBy, queryDistinct, stringLit, strToDate, nullIf, floatLit, leftJoin, inSubQuery, notInSubQuery, suchThat, row, (<=>), userVar, subqueryAs, starFrom, plus, selectAs, insertFrom, setUserVar, rawExpr, alias, and, as, equal, (@@), on, table, join, from, select, query, intLit, having, not, null, groupBy, max, when)
import UploadPlan (UploadPlan(..), columnName, UploadStrategy(..), ToOne(..), UploadTable(..), ToMany(..), ToManyRecord, NamedValue(..), ToManyRecord(..), ColumnType(..))
import Common (remark, rowsFromWB, joinToManys, toOneIdColumnVar, toManyIdColumnVar, toOneMappingItems, toManyMappingItems, strategyToWhereClause, parseMappingItem, MappingItem(..), show)

matchExistingRecords :: UploadTable -> [Statement]
matchExistingRecords uploadTable = execWriter $ do
  insertIdFields uploadTable
  matchRecords uploadTable

insertIdFields :: UploadTable -> Writer [Statement] ()
insertIdFields ut@(UploadTable {tableName, idColumn}) = do
  tell [remark $ "Inserting an id field for the base table."]

  tell [
    insertFrom "workbenchtemplatemappingitem"
      ["timestampcreated", "workbenchtemplateid", "fieldname", "tablename", "vieworder", "caption", "metadata"] $
      query [ select $ rawExpr "now()"
            , select $ userVar "templateid"
            , select $ stringLit idColumn
            , select $ stringLit tableName
            , select $ (max $ project "vieworder") `plus` intLit 1
            , select $ stringLit $ idColumn
            , select $ stringLit $ "generated"
            ] `from` [table "workbenchtemplatemappingitem"]
    ]
  tell [setUserVar idColumn $ rawExpr "last_insert_id()"]

  insertIdFieldsFromToOnes ut
  insertIdFieldsFromToManys ut

insertIdFieldsFromToOnes :: UploadTable -> Writer [Statement] ()
insertIdFieldsFromToOnes (UploadTable {tableName, toOneTables}) = do
  forM_ toOneTables $ \(ToOne {toOneFK, toOneTable}) -> do
    let (UploadTable {tableName=toOneTableName}) = toOneTable
    tell [remark $ "Inserting an id field for the " <> tableName <> " to " <> toOneTableName <> " foreign key."]

    tell [
      insertFrom "workbenchtemplatemappingitem"
        ["timestampcreated", "workbenchtemplateid", "fieldname", "tablename", "vieworder", "caption", "metadata"] $
        query [ select $ rawExpr "now()"
              , select $ userVar "templateid"
              , select $ stringLit toOneFK
              , select $ stringLit tableName
              , select $ (max $ project "vieworder") `plus` intLit 1
              , select $ stringLit $ toOneIdColumnVar tableName toOneFK
              , select $ stringLit $ "generated"
              ] `from` [table "workbenchtemplatemappingitem"]
      ]
    tell [setUserVar (toOneIdColumnVar tableName toOneFK) $ rawExpr "last_insert_id()"]

    insertIdFieldsFromToOnes toOneTable
    insertIdFieldsFromToManys toOneTable

insertIdFieldsFromToManys :: UploadTable -> Writer [Statement] ()
insertIdFieldsFromToManys (UploadTable {toManyTables}) = do
  forM_ toManyTables $ \(ToMany {toManyTable, records}) ->
    forM_ (zip [0 ..] records) $ \(index, ToManyRecord {toOneTables}) ->
      forM_ toOneTables $ \(ToOne {toOneFK, toOneTable}) -> do
        let (UploadTable {tableName=toOneTableName}) = toOneTable
        tell [ remark $ "Inserting an id field for the " <> toManyTable <> show index <> " to " <> toOneTableName <> " foreign key."]

        tell [
          insertFrom "workbenchtemplatemappingitem"
            ["timestampcreated", "workbenchtemplateid", "fieldname", "tablename", "vieworder", "caption", "metadata"] $
            query [ select $ rawExpr "now()"
                  , select $ userVar "templateid"
                  , select $ stringLit toOneFK
                  , select $ stringLit toManyTable
                  , select $ (max $ project "vieworder") `plus` intLit 1
                  , select $ stringLit $ toManyIdColumnVar toManyTable index toOneFK
                  , select $ stringLit $ "generated"
                  ] `from` [table "workbenchtemplatemappingitem"]
          ]
        tell [ setUserVar (toManyIdColumnVar toManyTable index toOneFK) $ rawExpr "last_insert_id()"]

        insertIdFieldsFromToOnes toOneTable
        insertIdFieldsFromToManys toOneTable

matchRecords :: UploadTable -> Writer [Statement] ()
matchRecords uploadTable@(UploadTable {tableName}) = do
  matchToOnes uploadTable
  matchToManys uploadTable

  let wbTemplateMappingItemId = userVar $ idColumn uploadTable

  tell [remark $ "Finding existing " <> tableName <> " records."]
  tell [findExistingRecords wbTemplateMappingItemId uploadTable]

  tell [remark $ "Flagging new records."]
  tell $ flagNewRecords wbTemplateMappingItemId uploadTable


matchToManys :: UploadTable -> Writer [Statement] ()
matchToManys ut = do
  forM_ (toManyTables ut) $ \(ToMany {toManyTable, records}) ->
    forM_ (zip [0 .. ] records) $ \(index, (ToManyRecord {toOneTables})) ->
      forM_ toOneTables $ \toOne -> matchToManyToOne toManyTable index toOne

matchToManyToOne :: Text -> Int -> ToOne -> Writer [Statement] ()
matchToManyToOne toManyTable index (ToOne {toOneFK, toOneTable}) = do
  matchToOnes toOneTable
  matchToManys toOneTable

  let wbTemplateMappingItemId = userVar $ toManyIdColumnVar toManyTable index toOneFK
  let (UploadTable {tableName}) = toOneTable

  tell [remark $ "Finding existing " <> tableName <> " records for " <> toManyTable <> " " <> (show index) <> "."]
  tell [findExistingRecords wbTemplateMappingItemId toOneTable]

  tell [remark $ "Flagging new records."]
  tell $ flagNewRecords wbTemplateMappingItemId toOneTable

matchToOnes :: UploadTable -> Writer [Statement] ()
matchToOnes (UploadTable {tableName, toOneTables}) =
  forM_ toOneTables $ \(ToOne {toOneFK, toOneTable}) -> do
    matchToOnes toOneTable
    matchToManys toOneTable

    let wbTemplateMappingItemId = userVar $ toOneIdColumnVar tableName toOneFK
    let (UploadTable {tableName=toOneTableName}) = toOneTable

    tell [remark $ "Finding existing " <> toOneTableName <> " records."]
    tell [findExistingRecords wbTemplateMappingItemId toOneTable]

    tell [remark $ "Flagging new records."]
    tell $ flagNewRecords wbTemplateMappingItemId toOneTable

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

flagNewRecords :: Expr -> UploadTable -> [Statement]
flagNewRecords wbTemplateMappingItemId ut@(UploadTable {mappingItems}) =
  [ insertFrom "workbenchdataitem" ["workbenchrowid", "celldata", "rownumber", "workbenchtemplatemappingitemid"] $
    query
    [ select $ wbRow @@ "workbenchrowid"
    , select $ stringLit "new"
    , select $ wbRow @@ "rownumber"
    , select $ wbTemplateMappingItemId
    ] `from`
    [ subqueryAs wbRow $ rowsFromWB (userVar "workbenchid") mappingItems' excludeRows ]
  ]
  where
    t = alias "t"
    wbRow = alias "wbrow"
    mappingItems' = fmap (parseMappingItem t) mappingItems <> toOneMappingItems ut t <> toManyMappingItems ut
    excludeRows = rowsWithValuesFor wbTemplateMappingItemId


findExistingRecords :: Expr -> UploadTable -> Statement
findExistingRecords wbTemplateMappingItemId ut@(UploadTable {tableName, idColumn, strategy, mappingItems}) =
  insertFrom "workbenchdataitem" ["workbenchrowid", "celldata", "rownumber", "workbenchtemplatemappingitemid"] $
  query
  [ selectAs "rowid" $ wb @@ "workbenchrowid"
  , selectAs "ids" $ rawExpr $ "group_concat(t." <> idColumn <> ")"--t @@ idColumn
  , selectAs "rownumber" $ wb @@ "rownumber"
  , selectAs "wbtmid" $ wbTemplateMappingItemId
  ] `from`
  [ ( joinToManys t ut (table tableName `as` t) ) `join` wbSubQuery `on` (valuesFromWB <=> valuesFromTable)]
  `st` strategyToWhereClause strategy t
  `groupBy` [project "rowid", project "rownumber", project "wbtmid"]
  where
    t = alias "t"
    wb = alias "wb"
    st = maybeApply suchThat
    mappingItems' = fmap (parseMappingItem t) mappingItems <> toOneMappingItems ut t <> toManyMappingItems ut
    valuesFromWB = row $ fmap (\(MappingItem {selectFromWBas}) -> wb @@ selectFromWBas) mappingItems'
    valuesFromTable = row $ fmap (\(MappingItem {tableAlias, tableColumn}) -> tableAlias @@ tableColumn) mappingItems'
    wbSubQuery = subqueryAs wb $ rowsFromWB (userVar "workbenchid") mappingItems' excludeRows
    excludeRows = rowsWithValuesFor wbTemplateMappingItemId

