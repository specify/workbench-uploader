module Upload (upload) where

import Prelude (undefined, fmap, Int, (<>), zip, ($))

import Data.Text (Text)
import Control.Monad.Writer (execWriter, tell, Writer)
import Control.Monad (forM_)
import Control.Newtype.Generics (unpack)

import SQL (Statement(..), Expr, Script(..))
import SQLSmart (using, locate, update, scalarSubQuery, startTransaction, rollback, insertValues, (@=), project, asc, orderBy, queryDistinct, stringLit, strToDate, nullIf, floatLit, leftJoin, inSubQuery, notInSubQuery, suchThat, row, (<=>), userVar, subqueryAs, starFrom, plus, selectAs, insertFrom, setUserVar, rawExpr, alias, and, as, equal, (@@), on, table, join, from, select, query, intLit, having, not, null, groupBy, max, when)
import UploadPlan (UploadPlan(..), columnName, UploadStrategy(..), ToOne(..), UploadTable(..), ToMany(..), ToManyRecord, NamedValue(..), ToManyRecord(..), ColumnType(..))
import Common (remark, toManyMappingItems, toOneMappingItems, valuesFromWB, rowsFromWB, showWB, rowsWithValuesFor, parseMappingItem, MappingItem(..), show)
import MatchExistingRecords (matchExistingRecords)

skipDegenerateRecords = undefined
toManyIdColumnVar = undefined
toOneIdColumnVar = undefined

upload :: UploadPlan -> Script
upload up@(UploadPlan {templateId, workbenchId, uploadTable}) = Script $ execWriter $ do
  tell [rollback]
  tell [startTransaction]
  tell [setUserVar "templateid" $ intLit $ unpack templateId]
  tell [setUserVar "workbenchid" $ intLit $ unpack workbenchId]
  tell [clearUploadStatus]
  tell $ matchExistingRecords up
  tell [remark $ "Skipping degenerate records."]
  tell [skipDegenerateRecords uploadTable]
  handleUpload uploadTable
  tell [ QueryStatement $ showWB (userVar "workbenchid") ]

clearUploadStatus :: Statement
clearUploadStatus = UpdateStatement $
  update [table "workbenchrow"] [("uploadstatus", intLit 0)]
  `when` (project "workbenchid" `equal` userVar "workbenchid")

handleUpload :: UploadTable -> Writer [Statement] ()
handleUpload uploadTable@(UploadTable {tableName}) = do
  handleToOnes uploadTable
  handleToManys uploadTable

  let wbTemplateMappingItemId = userVar $ idColumn uploadTable

  tell [remark $ "Inserting new " <> tableName <> " records."]
  tell [insertNewRecords wbTemplateMappingItemId uploadTable]

  tell [remark $ "Matching up newly created " <> tableName <> " records."]
  tell $ findNewRecords wbTemplateMappingItemId uploadTable


handleToManys :: UploadTable -> Writer [Statement] ()
handleToManys ut = do
  forM_ (toManyTables ut) $ \(ToMany {toManyTable, records}) ->
    forM_ (zip [0 .. ] records) $ \(index, (ToManyRecord {toOneTables})) ->
      forM_ toOneTables $ \toOne -> handleToManyToOne toManyTable index toOne

handleToManyToOne :: Text -> Int -> ToOne -> Writer [Statement] ()
handleToManyToOne toManyTable index (ToOne {toOneFK, toOneTable}) = do
  handleToOnes toOneTable
  handleToManys toOneTable

  let wbTemplateMappingItemId = userVar $ toManyIdColumnVar toManyTable index toOneFK
  let (UploadTable {tableName}) = toOneTable

  tell [remark $ "Inserting new " <> tableName <> " records for " <> toManyTable <> " " <> (show index) <> "."]
  tell [insertNewRecords wbTemplateMappingItemId toOneTable]

  tell [remark $ "Matching up newly created " <> tableName <> " records for " <> toManyTable <> " " <> (show index) <> "."]
  tell $ findNewRecords wbTemplateMappingItemId toOneTable


handleToOnes :: UploadTable -> Writer [Statement] ()
handleToOnes (UploadTable {tableName, toOneTables}) =
  forM_ toOneTables $ \(ToOne {toOneFK, toOneTable}) -> do
    handleToOnes toOneTable
    handleToManys toOneTable

    let wbTemplateMappingItemId = userVar $ toOneIdColumnVar tableName toOneFK
    let (UploadTable {tableName=toOneTableName}) = toOneTable

    tell [remark $ "Inserting new " <> toOneTableName <> " records."]
    tell [insertNewRecords wbTemplateMappingItemId toOneTable]

    tell [remark $ "Matching up newly created " <> toOneTableName <> " records."]
    tell $ findNewRecords wbTemplateMappingItemId toOneTable



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
findNewRecords wbTemplateMappingItemId ut@(UploadTable {idColumn, mappingItems}) =
  [ setUserVar "new_id" $ rawExpr "last_insert_id()"
  , setUserVar "row_number" $ intLit 0
  , UpdateStatement $ update
    [ (table "workbenchdataitem")
      `join`
      (subqueryAs wbRow $ rowsFromWB (userVar "workbenchid") mappingItems' excludeRows) `using` ["workbenchrowid"]
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
    [("celldata", valuesWithId @@ idColumn)]
  ]
  where
    t = alias "t"
    wbRow = alias "wbrow"
    newValues = alias "newvalues"
    valuesWithId = alias "valueswithid"
    mappingItems' = fmap (parseMappingItem t) mappingItems <> toOneMappingItems ut t <> toManyMappingItems ut
    newVals = row $ fmap (\(MappingItem {selectFromWBas}) -> valuesWithId @@ selectFromWBas) mappingItems'
    wbVals = row $ fmap (\(MappingItem {selectFromWBas}) -> wbRow @@ selectFromWBas) mappingItems'
    excludeRows = rowsWithValuesFor wbTemplateMappingItemId

