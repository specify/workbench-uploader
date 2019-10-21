module Main where

import Prelude (fmap, ($), IO)

import Data.Text.IO (putStrLn)
import SQLSmart (intLit, alias)
import SQLRender (renderScript, renderQuery, renderSQL)
import Upload (findNewRecords, valuesFromWB, parseMappingItem, rowsFromWB, rowsWithValuesFor)
import ExamplePlan (uploadPlan)
import UploadPlan (UploadTable(..), mappingItems, uploadTable)

main :: IO ()
main = do
  let t = alias "t"
  let excludedRows = rowsWithValuesFor $ intLit 0
  let (UploadTable {mappingItems}) = uploadTable uploadPlan

  let rfwb = rowsFromWB (intLit 0) (fmap (parseMappingItem t) mappingItems) excludedRows
  putStrLn $ renderSQL (renderQuery rfwb)

  let vfwb = valuesFromWB (intLit 0) (fmap (parseMappingItem t) mappingItems) excludedRows
  putStrLn $ renderSQL (renderQuery vfwb)

  let fnr = findNewRecords (intLit 0) (uploadTable uploadPlan)
  putStrLn $ renderSQL (renderScript fnr)
  
