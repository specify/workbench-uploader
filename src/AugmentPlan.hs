module AugmentPlan where

import Prelude (Maybe(..), IO, (<>), zip, ($))

import Data.Text.IO (putStrLn)
import Control.Monad (forM, return, (=<<))
import Control.Newtype.Generics (unpack)
import Database.MySQL.Simple.Types (Only(..))
import Database.MySQL.Simple (Connection)

import SQL (Expr)
import SQLSmart (max, query, rawExpr, select, table, from, stringLit, intLit, plus, project, insertFrom, userVar)
import UploadPlan (TemplateId(..), UploadPlan(..), columnName, id, ToOne(..), UploadTable(..), ToMany(..), ToManyRecord, ToManyRecord(..), ColumnType(..), MappingItem(..))
import Common (show, execute, runQuery)


augmentPlan :: Connection -> UploadPlan -> IO UploadPlan
augmentPlan conn up@(UploadPlan {uploadTable, templateId}) = do
  ut <- insertIdFields conn templateId uploadTable
  return up {uploadTable = ut}


insertIdFields :: Connection -> TemplateId -> UploadTable -> IO UploadTable
insertIdFields conn templateId ut@(UploadTable {tableName, idColumn}) = do
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

  insertIdFieldsFromToManys conn templateId =<< (insertIdFieldsFromToOnes conn templateId $ ut {idMapping = Just id})


insertIdFieldsFromToOnes :: Connection -> TemplateId -> UploadTable -> IO UploadTable
insertIdFieldsFromToOnes conn templateId ut@(UploadTable {tableName, toOneTables}) = do
  toOneTables' <- forM toOneTables $ \toOne@(ToOne {toOneFK, toOneTable}) -> do
    let (UploadTable {tableName=toOneTableName}) = toOneTable
    putStrLn $ "Inserting an id field for the " <> tableName <> " to " <> toOneTableName <> " foreign key."

    execute conn [
      insertFrom "workbenchtemplatemappingitem"
        ["timestampcreated", "workbenchtemplateid", "fieldname", "tablename", "vieworder", "caption", "metadata"] $
        query [ select $ rawExpr "now()"
              , select $ intLit $ unpack templateId
              , select $ stringLit toOneFK
              , select $ stringLit tableName
              , select $ (max $ project "vieworder") `plus` intLit 1
              , select $ stringLit $ tableName <> toOneFK
              , select $ stringLit $ "generated"
              ] `from` [table "workbenchtemplatemappingitem"]
      ]

    [Only id] <- runQuery conn (query [select $ rawExpr "last_insert_id()"])

    toOneTable' <- insertIdFieldsFromToManys conn templateId
      =<< (insertIdFieldsFromToOnes conn templateId $ toOneTable {idMapping = Just id})

    return $ toOne {toOneTable = toOneTable'}
  return (ut {toOneTables = toOneTables'} :: UploadTable)

insertIdFieldsFromToManys :: Connection -> TemplateId -> UploadTable -> IO UploadTable
insertIdFieldsFromToManys conn templateId ut@(UploadTable {toManyTables}) = do
  toManyTables' <- forM toManyTables $ \toMany@(ToMany {toManyTable, records}) -> do
    records' <- forM (zip [0 ..] records) $ \(index, tmr@(ToManyRecord {toOneTables})) -> do
      toOneTables' <- forM toOneTables $ \toOne@(ToOne {toOneFK, toOneTable}) -> do
        let (UploadTable {tableName=toOneTableName}) = toOneTable
        putStrLn $ "Inserting an id field for the " <> toManyTable <> show index <> " to " <> toOneTableName <> " foreign key."

        execute conn [
          insertFrom "workbenchtemplatemappingitem"
            ["timestampcreated", "workbenchtemplateid", "fieldname", "tablename", "vieworder", "caption", "metadata"] $
            query [ select $ rawExpr "now()"
                  , select $ intLit $ unpack templateId
                  , select $ stringLit toOneFK
                  , select $ stringLit toManyTable
                  , select $ (max $ project "vieworder") `plus` intLit 1
                  , select $ stringLit $ toManyTable <> (show index) <> toOneFK
                  , select $ stringLit $ "generated"
                  ] `from` [table "workbenchtemplatemappingitem"]
          ]

        [Only id] <- runQuery conn (query [select $ rawExpr "last_insert_id()"])

        toOneTable' <- insertIdFieldsFromToManys conn templateId
          =<< (insertIdFieldsFromToOnes conn templateId $ toOneTable {idMapping = Just id})

        return $ toOne {toOneTable = toOneTable'}
      return (tmr {toOneTables = toOneTables'} :: ToManyRecord)
    return $ toMany {records = records'}
  return $ ut {toManyTables = toManyTables'}
