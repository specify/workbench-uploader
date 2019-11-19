module Main where

import Prelude ((<>), Either(..), (<$>), String, undefined, Integer, (.), ($), IO, show, fail)

import System.Environment (getArgs)
import Control.Monad (forM_)
import Data.Text (Text, unpack, pack)
import Data.Text.IO (putStrLn)
import Data.Text.Encoding (decodeUtf8)
import Data.Aeson (eitherDecode)
import Data.Aeson.Encode.Pretty (encodePretty)
import Data.ByteString.Lazy (toStrict, readFile, writeFile)
import Database.MySQL.Simple (defaultConnectInfo, Connection, connect, ConnectInfo(..))

import SQLRender (renderScript, renderSQL)
import SQLSmart (intLit)
import Upload (upload)
import ExamplePlan (uploadPlan)
import AugmentPlan (augmentPlan)
import UploadPlan (WorkbenchId(..), UploadPlan(..))
import Common (runQuery, showWB)

main :: IO ()
main = do
  args <- getArgs
  case args of
    [] -> fail "no command given"
    ("print" : args') -> doPrint args'
    ("example" : _) -> printPlan uploadPlan
    ("augment" : args') -> doAugment args'
    _ -> fail "bad command"

doPrint :: [String] -> IO ()
doPrint (planFile : _) = do
  conn <- connectTo "bishop"
  decoded <- eitherDecode <$> readFile planFile
  case decoded of
    Right plan -> printWB conn plan
    Left err -> fail $ "couldn't parse json:" <> err
doPrint _ = fail "missing plan file"

doAugment :: [String] -> IO ()
doAugment (inFile : outFile : _) = do
  conn <- connectTo "bishop"
  decoded <- eitherDecode <$> readFile inFile
  augmented <- case decoded of
    Right plan -> augmentPlan conn plan
    Left err -> fail $ "couldn't parse json:" <> err
  writeFile outFile $ encodePretty augmented
doAugment _ = fail "expected inFile and outFile"

printPlan :: UploadPlan -> IO ()
printPlan = putStrLn . decodeUtf8 . toStrict . encodePretty

connectTo :: String -> IO Connection
connectTo dbName = connect defaultConnectInfo {connectDatabase=dbName, connectUser="Master", connectPassword="Master"}

printWB :: Connection -> UploadPlan -> IO ()
printWB conn (UploadPlan {workbenchId=WorkbenchId wbid}) = do
  rows  :: [(Integer, Text)] <- runQuery conn $ showWB (intLit wbid)
  forM_ rows $ \(_, cols) -> putStrLn cols
