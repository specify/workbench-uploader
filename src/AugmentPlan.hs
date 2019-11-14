module AugmentPlan where

import Prelude (IO, (<>), zip, ($))

import qualified Data.Text as T
import Data.String (fromString)
import Data.Text.IO (putStrLn)
import Control.Monad (forM_, forM, return)
import Control.Newtype.Generics (unpack)
import Database.MySQL.Simple.QueryResults (QueryResults)
import Database.MySQL.Simple.Types (Only(..))
import Database.MySQL.Simple (Connection)
import qualified Database.MySQL.Simple as MySQL

import SQL (QueryExpr, Statement(..), Expr)
import SQLRender (renderQuery, renderStatement, renderSQL)
import SQLSmart (max, query, rawExpr, select, table, from, stringLit, intLit, plus, project, insertFrom, userVar)
import UploadPlan (TemplateId(..), UploadPlan(..), columnName, id, ToOne(..), UploadTable(..), ToMany(..), ToManyRecord, ToManyRecord(..), ColumnType(..), MappingItem(..))
import Common (show, toManyIdColumnVar, toOneIdColumnVar)


execute :: Connection -> [Statement] -> IO ()
execute conn statements =
  forM_ statements $ \s -> MySQL.execute_ conn $ fromString $ T.unpack $ renderSQL $ renderStatement s


runQuery :: (QueryResults r) => Connection -> QueryExpr -> IO [r]
runQuery conn q = MySQL.query_ conn $ fromString $ T.unpack $ renderSQL $ renderQuery q

augmentPlan :: Connection -> UploadPlan -> IO UploadPlan
augmentPlan conn up@(UploadPlan {uploadTable, templateId}) = do
  ut <- insertIdFields conn templateId uploadTable
  return up {uploadTable = ut}


idFields :: UploadTable -> [Expr]
idFields ut@(UploadTable {idColumn}) =
  [userVar idColumn] <> toOneIdFields ut <> toManyIdFields ut
  where
    toOneIdFields (UploadTable {tableName, toOneTables}) = do
      (ToOne {toOneFK, toOneTable}) <- toOneTables
      [userVar $ toOneIdColumnVar tableName toOneFK] <> toOneIdFields toOneTable <> toManyIdFields toOneTable

    toManyIdFields (UploadTable {toManyTables}) = do
      (ToMany {toManyTable, records}) <- toManyTables
      (index, ToManyRecord {toOneTables}) <- zip [0 ..] records
      (ToOne {toOneFK, toOneTable}) <- toOneTables
      [userVar $ toManyIdColumnVar toManyTable index toOneFK] <> toOneIdFields toOneTable <> toManyIdFields toOneTable

insertIdFields :: Connection -> TemplateId -> UploadTable -> IO UploadTable
insertIdFields conn templateId ut@(UploadTable {tableName, idColumn, mappingItems}) = do
  putStrLn  "Inserting an id field for the base table."

  execute conn [
    insertFrom "workbenchtemplatemappingitem"
      ["timestampcreated", "workbenchtemplateid", "fieldname", "tablename", "vieworder", "caption", "metadata"] $
      query [ select $ rawExpr "now()"
            , select $ intLit $ unpack templateId
            , select $ stringLit idColumn
            , select $ stringLit tableName
            , select $ (max $ project "vieworder") `plus` intLit 1
            , select $ stringLit $ idColumn
            , select $ stringLit $ "generated"
            ] `from` [table "workbenchtemplatemappingitem"]
    ]

  [Only id] <- runQuery conn (query [select $ rawExpr "last_insert_id()"])
  let ut' = ut {mappingItems = MappingItem {columnName = idColumn, id = id, columnType = IdType} : mappingItems } :: UploadTable

  ut'' <- insertIdFieldsFromToOnes conn templateId ut'
  ut''' <- insertIdFieldsFromToManys conn templateId ut''
  return ut'''

insertIdFieldsFromToOnes :: Connection -> TemplateId -> UploadTable -> IO UploadTable
insertIdFieldsFromToOnes conn templateId ut@(UploadTable {tableName, toOneTables}) = do
  toOneTables' <- forM toOneTables $ \toOne@(ToOne {toOneFK, toOneTable}) -> do
    let (UploadTable {tableName=toOneTableName, mappingItems}) = toOneTable
    putStrLn $ "Inserting an id field for the " <> tableName <> " to " <> toOneTableName <> " foreign key."

    execute conn [
      insertFrom "workbenchtemplatemappingitem"
        ["timestampcreated", "workbenchtemplateid", "fieldname", "tablename", "vieworder", "caption", "metadata"] $
        query [ select $ rawExpr "now()"
              , select $ intLit $ unpack templateId
              , select $ stringLit toOneFK
              , select $ stringLit tableName
              , select $ (max $ project "vieworder") `plus` intLit 1
              , select $ stringLit $ toOneIdColumnVar tableName toOneFK
              , select $ stringLit $ "generated"
              ] `from` [table "workbenchtemplatemappingitem"]
      ]

    [Only id] <- runQuery conn (query [select $ rawExpr "last_insert_id()"])

    let toOneTable' = toOneTable
          { mappingItems = MappingItem {columnName = toOneFK, id = id, columnType = IdType} : mappingItems
          } :: UploadTable

    toOneTable'' <- insertIdFieldsFromToOnes conn templateId toOneTable'
    toOneTable''' <- insertIdFieldsFromToManys conn templateId toOneTable''
    return $ toOne {toOneTable = toOneTable'''}
  return (ut {toOneTables = toOneTables'} :: UploadTable)

insertIdFieldsFromToManys :: Connection -> TemplateId -> UploadTable -> IO UploadTable
insertIdFieldsFromToManys conn templateId ut@(UploadTable {toManyTables}) = do
  toManyTables' <- forM toManyTables $ \toMany@(ToMany {toManyTable, records}) -> do
    records' <- forM (zip [0 ..] records) $ \(index, tmr@(ToManyRecord {toOneTables})) -> do
      toOneTables' <- forM toOneTables $ \toOne@(ToOne {toOneFK, toOneTable}) -> do
        let (UploadTable {tableName=toOneTableName, mappingItems}) = toOneTable
        putStrLn $ "Inserting an id field for the " <> toManyTable <> show index <> " to " <> toOneTableName <> " foreign key."

        execute conn [
          insertFrom "workbenchtemplatemappingitem"
            ["timestampcreated", "workbenchtemplateid", "fieldname", "tablename", "vieworder", "caption", "metadata"] $
            query [ select $ rawExpr "now()"
                  , select $ intLit $ unpack templateId
                  , select $ stringLit toOneFK
                  , select $ stringLit toManyTable
                  , select $ (max $ project "vieworder") `plus` intLit 1
                  , select $ stringLit $ toManyIdColumnVar toManyTable index toOneFK
                  , select $ stringLit $ "generated"
                  ] `from` [table "workbenchtemplatemappingitem"]
          ]

        [Only id] <- runQuery conn (query [select $ rawExpr "last_insert_id()"])

        let toOneTable' = toOneTable {
              mappingItems = MappingItem {columnName = toOneFK, id = id, columnType = IdType} :
                mappingItems
              } :: UploadTable

        toOneTable'' <- insertIdFieldsFromToOnes conn templateId toOneTable'
        toOneTable''' <- insertIdFieldsFromToManys conn templateId toOneTable''
        return $ toOne {toOneTable = toOneTable'''}
      return (tmr {toOneTables = toOneTables'} :: ToManyRecord)
    return $ toMany {records = records'}
  return $ ut {toManyTables = toManyTables'}
