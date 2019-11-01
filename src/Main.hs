module Main where

import Prelude (($), IO)

import Data.Text.IO (putStrLn)
import SQLRender (renderScript, renderSQL)
import Upload (upload)
import ExamplePlan (uploadPlan)
import MatchRecords ()

main :: IO ()
main = do
  let script = upload uploadPlan
  putStrLn $ renderSQL (renderScript script)

