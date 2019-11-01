module Upload where

import Prelude (fmap, Int, (<>), zip, ($))

import Data.Text (Text)
import Control.Monad.Writer (execWriter, tell, Writer)
import Control.Monad (forM_)
import Control.Newtype.Generics (unpack)

import SQL (Statement(..), Expr, Script(..))
import SQLSmart (using, locate, update, scalarSubQuery, startTransaction, rollback, insertValues, (@=), project, asc, orderBy, queryDistinct, stringLit, strToDate, nullIf, floatLit, leftJoin, inSubQuery, notInSubQuery, suchThat, row, (<=>), userVar, subqueryAs, starFrom, plus, selectAs, insertFrom, setUserVar, rawExpr, alias, and, as, equal, (@@), on, table, join, from, select, query, intLit, having, not, null, groupBy, max, when)
import UploadPlan (UploadPlan(..), columnName, UploadStrategy(..), ToOne(..), UploadTable(..), ToMany(..), ToManyRecord, NamedValue(..), ToManyRecord(..), ColumnType(..))
import Common (toManyMappingItems, toOneMappingItems, valuesFromWB, rowsFromWB, showWB, rowsWithValuesFor, parseMappingItem, MappingItem(..))
import MatchExistingRecords (matchExistingRecords)

upload :: UploadPlan -> Script
upload (UploadPlan {templateId, workbenchId, uploadTable}) = Script $ execWriter $ do
  tell [rollback]
  tell [startTransaction]
  tell [setUserVar "templateid" $ intLit $ unpack templateId]
  tell [setUserVar "workbenchid" $ intLit $ unpack workbenchId]
  tell [clearUploadStatus]
  tell $ matchExistingRecords uploadTable
  -- tell [ QueryStatement $ showWB (userVar "workbenchid") ]

clearUploadStatus :: Statement
clearUploadStatus = UpdateStatement $
  update [table "workbenchrow"] [("uploadstatus", intLit 0)]
  `when` (project "workbenchid" `equal` userVar "workbenchid")

handleUpload :: UploadTable -> Writer [Statement] ()
handleUpload uploadTable@(UploadTable {tableName}) = do
  handleToOnes uploadTable
  handleToManys uploadTable

  -- let wbTemplateMappingItemId = userVar $ idColumn uploadTable


  -- tell [remark $ "Skipping degenerate records."]
  -- tell [skipDegenerateRecords wbTemplateMappingItemId]

  -- tell [remark $ "Inserting new " <> tableName <> " records."]
  -- tell [insertNewRecords wbTemplateMappingItemId uploadTable]

  -- tell [remark $ "Matching up newly created " <> tableName <> " records."]
  -- tell $ findNewRecords wbTemplateMappingItemId uploadTable


handleToManys :: UploadTable -> Writer [Statement] ()
handleToManys ut = do
  forM_ (toManyTables ut) $ \(ToMany {toManyTable, records}) ->
    forM_ (zip [0 .. ] records) $ \(index, (ToManyRecord {toOneTables})) ->
      forM_ toOneTables $ \toOne -> handleToManyToOne toManyTable index toOne

handleToManyToOne :: Text -> Int -> ToOne -> Writer [Statement] ()
handleToManyToOne toManyTable index (ToOne {toOneFK, toOneTable}) = do
  handleToOnes toOneTable
  handleToManys toOneTable

  -- let wbTemplateMappingItemId = userVar $ toManyIdColumnVar toManyTable index toOneFK
  -- let (UploadTable {tableName}) = toOneTable

  -- tell [remark $ "Skipping degenerate records."]
  -- tell [skipDegenerateRecords wbTemplateMappingItemId]

  -- tell [remark $ "Inserting new " <> tableName <> " records for " <> toManyTable <> " " <> (show index) <> "."]
  -- tell [insertNewRecords wbTemplateMappingItemId toOneTable]

  -- tell [remark $ "Matching up newly created " <> tableName <> " records for " <> toManyTable <> " " <> (show index) <> "."]
  -- tell $ findNewRecords wbTemplateMappingItemId toOneTable


handleToOnes :: UploadTable -> Writer [Statement] ()
handleToOnes (UploadTable {tableName, toOneTables}) =
  forM_ toOneTables $ \(ToOne {toOneFK, toOneTable}) -> do
    handleToOnes toOneTable
    handleToManys toOneTable

    -- let wbTemplateMappingItemId = userVar $ toOneIdColumnVar tableName toOneFK
    -- let (UploadTable {tableName=toOneTableName}) = toOneTable

    -- tell [remark $ "Finding existing " <> toOneTableName <> " records."]
    -- tell [findExistingRecords wbTemplateMappingItemId toOneTable]

    -- tell [remark $ "Skipping degenerate records."]
    -- tell [skipDegenerateRecords wbTemplateMappingItemId]

    -- tell [remark $ "Flagging new records."]
    -- tell $ flagNewRecords wbTemplateMappingItemId toOneTable
    -- tell [remark $ "Inserting new " <> toOneTableName <> " records."]
    -- tell [insertNewRecords wbTemplateMappingItemId toOneTable]

    -- tell [remark $ "Matching up newly created " <> toOneTableName <> " records."]
    -- tell $ findNewRecords wbTemplateMappingItemId toOneTable



insertNewRecords :: Expr -> UploadTable -> Statement
insertNewRecords wbTemplateMappingItemId ut@(UploadTable {tableName, staticValues, mappingItems}) =
  insertFrom tableName columns $
  query (newVals <> constantVals)
  `from` [subqueryAs newValues $ valuesFromWB (userVar "workbenchid") (mappingItems' <> toManyMappingItems ut) excludeRows]
  where
    newValues = alias "newvalues"
    t = alias "t" --unused
    mappingItems' = fmap (parseMappingItem t) mappingItems <> toOneMappingItems ut t
    newVals = (\(MappingItem {tableColumn}) -> select $ newValues @@ tableColumn) `fmap` mappingItems'
    constantVals = (\(NamedValue {column, value}) -> selectAs column $ rawExpr value) `fmap` staticValues
    columns = fmap tableColumn mappingItems' <> fmap column staticValues
    excludeRows = rowsWithValuesFor wbTemplateMappingItemId

findNewRecords :: Expr -> UploadTable -> [Statement]
findNewRecords wbTemplateMappingItemId table@(UploadTable {tableName, idColumn, strategy, mappingItems}) =
  [ setUserVar "new_id" $ rawExpr "last_insert_id()"
  , setUserVar "row_number" $ intLit 0
  , insertFrom "workbenchdataitem" ["workbenchrowid", "celldata", "rownumber", "workbenchtemplatemappingitemid"] $
    query
    [ select $ wbRow @@ "workbenchrowid"
    , select $ valuesWithId @@ idColumn
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




skipDegenerateRecords :: Expr -> Statement
skipDegenerateRecords wbTemplateMappingItemId = UpdateStatement $
  update
  [(table "workbenchrow" `as` r) `join` (table "workbenchdataitem" `as` i)
   `on` ( ((r @@ "workbenchrowid") `equal` (i @@ "workbenchrowid"))
        `and` (i @@ "workbenchtemplatemappingitemid" `equal` wbTemplateMappingItemId)
        `and` (locate (stringLit ",") $ i @@ "celldata" )
        )
  ]
  [("uploadstatus", intLit 1)]
  where
    r = alias "r"
    i = alias "i"

