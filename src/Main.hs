module Main where

import Prelude (pure, (<>), Either(..), (<$>), String, Integer, (.), ($), IO, fail)

import System.Environment (getArgs)
import Control.Monad (forM_)
import Control.Monad.Reader (runReaderT, lift, ask, ReaderT)
import Data.String (fromString)
import Data.Text (Text, unpack)
import Data.Text.IO (putStrLn)
import Data.Text.Encoding (decodeUtf8)
import Data.Aeson (eitherDecode)
import Data.Aeson.Encode.Pretty (encodePretty)
import Data.ByteString.Lazy (toStrict, readFile, writeFile)
import Database.MySQL.Simple (defaultConnectInfo, Connection, connect, ConnectInfo(..))
import qualified Database.MySQL.Simple as MySQL


import MonadSQL (MonadSQL(..))
import SQLRender (renderQuery, renderStatement, renderSQL)
import SQLSmart (intLit, rollback, startTransaction)
import ExamplePlan (uploadPlan)
import AugmentPlan (augmentPlan)
import UploadPlan (WorkbenchId(..), UploadPlan(..))
import Common (showWB)
import MatchExistingRecords (clean)
import MatchRecords (uploadLeafRecords, matchLeafRecords)

data Env = Env {conn :: Connection}

type UploadMonad = ReaderT Env IO

instance MonadSQL UploadMonad where
  execute stmt = do
    let sql = renderStatement stmt
    log sql
    Env conn <- ask
    _ <- lift $ MySQL.execute_ conn $ fromString $ unpack $ sql
    pure ()

  doQuery q = do
    let sql = renderSQL $ renderQuery q
    log sql
    Env conn <- ask
    lift $ MySQL.query_ conn $ fromString $ unpack $ sql

  log msg = do
    lift $ putStrLn msg
    lift $ putStrLn ""

main :: IO ()
main = do
  args <- getArgs
  case args of
    [] -> fail "no command given"
    ("print" : args') -> doPrint args'
    ("example" : _) -> printPlan uploadPlan
    ("augment" : args') -> doAugment args'
    ("match" : args') -> doMatch args'
    ("upload" : args') -> doUpload args'
    ("clean" : args') -> doClean args'
    _ -> fail "bad command"

runDB :: String -> UploadMonad a -> IO a
runDB dbName sql = do
  conn <- connectTo dbName
  runReaderT sql (Env conn)

doPrint :: [String] -> IO ()
doPrint (planFile : _) = do
  decoded <- eitherDecode <$> readFile planFile
  case decoded of
    Right plan -> runDB "bishop" $ printWB plan
    Left err -> fail $ "couldn't parse json:" <> err
doPrint _ = fail "missing plan file"

doAugment :: [String] -> IO ()
doAugment (inFile : outFile : _) = do
  decoded <- eitherDecode <$> readFile inFile
  augmented <- case decoded of
    Right plan -> runDB "bishop" $ augmentPlan plan
    Left err -> fail $ "couldn't parse json:" <> err
  writeFile outFile $ encodePretty augmented
doAugment _ = fail "expected inFile and outFile"

doMatch :: [String] -> IO ()
doMatch (inFile : _) = do
  decoded <- eitherDecode <$> readFile inFile
  case decoded of
    Right plan -> runDB "bishop" $ matchLeafRecords plan
    Left err -> fail $ "couldn't parse json:" <> err
doMatch _ = fail "expected inFile"

doUpload :: [String] -> IO ()
doUpload (inFile : _) = do
  decoded <- eitherDecode <$> readFile inFile
  case decoded of
    Right plan -> runDB "bishop" $ do
      execute $ startTransaction
      uploadLeafRecords plan
      printWB plan
      execute $ rollback
    Left err -> fail $ "couldn't parse json:" <> err
doUpload _ = fail "expected inFile"

doClean :: [String] -> IO ()
doClean (inFile : _) = do
  decoded <- eitherDecode <$> readFile inFile
  case decoded of
    Right plan -> do
      runDB "bishop" $ clean plan
    Left err -> fail $ "couldn't parse json:" <> err
doClean _ = fail "expected inFile"

printPlan :: UploadPlan -> IO ()
printPlan = putStrLn . decodeUtf8 . toStrict . encodePretty

connectTo :: String -> IO Connection
connectTo dbName = connect defaultConnectInfo {connectDatabase=dbName, connectUser="Master", connectPassword="Master"}

printWB :: MonadSQL m => UploadPlan -> m ()
printWB (UploadPlan {workbenchId=WorkbenchId wbid}) = do
  rows  :: [(Integer, Text)] <- doQuery $ showWB (intLit wbid)
  forM_ rows $ \(_, cols) -> log cols
