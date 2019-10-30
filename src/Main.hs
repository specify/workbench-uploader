module Main where

import Prelude (fmap, ($), IO)

import Data.Text.IO (putStrLn)
import SQLSmart (intLit, alias)
import SQLRender (renderScript, renderQuery, renderSQL)
import Upload (upload, findNewRecords, valuesFromWB, parseMappingItem, rowsFromWB, rowsWithValuesFor)
import ExamplePlan (uploadPlan)
import UploadPlan (UploadTable(..), mappingItems, uploadTable)
import MatchRecords ()

main :: IO ()
main = do
  let script = upload uploadPlan
  putStrLn $ renderSQL (renderScript script)

