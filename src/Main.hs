module Main where

import Prelude (fmap, ($), IO)

import Data.Text.IO (putStrLn)
import SQLSmart (intLit, alias)
import SQLRender (renderQuery, renderSQL)
import Upload (parseMappingItem, rowsFromWB, rowsWithValuesFor)
import ExamplePlan (uploadPlan)
import UploadPlan (UploadTable(..), mappingItems, uploadTable)

main :: IO ()
main = do
  let t = alias "t"
  let excludedRows = rowsWithValuesFor $ intLit 0
  let (UploadTable {mappingItems}) = uploadTable uploadPlan
  let q = rowsFromWB (intLit 0) (fmap (parseMappingItem t) mappingItems) excludedRows
  let sql = renderSQL (renderQuery q)
  putStrLn sql
