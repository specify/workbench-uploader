{-# OPTIONS_GHC -fno-warn-type-defaults #-}
module AugmentPlan where

import Prelude (Maybe(..), (<>), zip, ($))

import Control.Monad (forM, return, (=<<))
import Control.Newtype.Generics (unpack)
import Database.MySQL.Simple.Types (Only(..))

import MonadSQL (MonadSQL(..))
import SQLSmart (max, query, rawExpr, select, table, from, stringLit, intLit, plus, project, insertFrom)
import UploadPlan (TemplateId(..), UploadPlan(..), columnName, id, ToOne(..), UploadTable(..), ToMany(..), ToManyRecord, ToManyRecord(..), ColumnType(..), MappingItem(..))
import Common (show)


augmentPlan :: MonadSQL m => UploadPlan -> m UploadPlan
augmentPlan up@(UploadPlan {uploadTable, templateId}) = do
  ut <- insertIdFields templateId uploadTable
  return up {uploadTable = ut}


insertIdFields :: MonadSQL m => TemplateId -> UploadTable -> m UploadTable
insertIdFields templateId ut@(UploadTable {tableName, idColumn}) = do
  log  "Inserting an id field for the base table."

  execute $
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


  result <- doQuery (query [select $ rawExpr "last_insert_id()"])
  case result of
    [Only id] ->
      insertIdFieldsFromToManys templateId =<< (insertIdFieldsFromToOnes templateId $ ut {idMapping = Just id})


insertIdFieldsFromToOnes :: MonadSQL m => TemplateId -> UploadTable -> m UploadTable
insertIdFieldsFromToOnes templateId ut@(UploadTable {tableName, toOneTables}) = do
  toOneTables' <- forM toOneTables $ \toOne@(ToOne {toOneFK, toOneTable}) -> do
    let (UploadTable {tableName=toOneTableName}) = toOneTable
    log $ "Inserting an id field for the " <> tableName <> " to " <> toOneTableName <> " foreign key."

    execute $
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


    result <- doQuery (query [select $ rawExpr "last_insert_id()"])
    toOneTable' <- case result of
      [Only id] -> insertIdFieldsFromToManys  templateId
                   =<< (insertIdFieldsFromToOnes templateId $ toOneTable {idMapping = Just id})

    return $ toOne {toOneTable = toOneTable'}
  return (ut {toOneTables = toOneTables'} :: UploadTable)

insertIdFieldsFromToManys :: MonadSQL m => TemplateId -> UploadTable -> m UploadTable
insertIdFieldsFromToManys templateId ut@(UploadTable {toManyTables}) = do
  toManyTables' <- forM toManyTables $ \toMany@(ToMany {toManyTable, records}) -> do
    records' <- forM (zip [0 ..] records) $ \(index, tmr@(ToManyRecord {toOneTables})) -> do
      toOneTables' <- forM toOneTables $ \toOne@(ToOne {toOneFK, toOneTable}) -> do
        let (UploadTable {tableName=toOneTableName}) = toOneTable
        log $ "Inserting an id field for the " <> toManyTable <> show index <> " to " <> toOneTableName <> " foreign key."

        execute $
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

        result <- doQuery (query [select $ rawExpr "last_insert_id()"])
        toOneTable' <- case result of
          [Only id] -> insertIdFieldsFromToManys templateId
                       =<< (insertIdFieldsFromToOnes templateId $ toOneTable {idMapping = Just id})

        return $ toOne {toOneTable = toOneTable'}
      return (tmr {toOneTables = toOneTables'} :: ToManyRecord)
    return $ toMany {records = records'}
  return $ ut {toManyTables = toManyTables'}
