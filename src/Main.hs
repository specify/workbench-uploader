module Main where

import Prelude (Integer, (.), ($), IO, show)

import Control.Monad (forM_)
import Data.Text (Text, unpack, pack)
import Data.Text.IO (putStrLn)
import Data.Text.Encoding (decodeUtf8)
import Data.Aeson.Encode.Pretty (encodePretty)
import Data.ByteString.Lazy (toStrict)
import Database.MySQL.Simple (defaultConnectInfo, Connection, connect, ConnectInfo(..))

import SQLRender (renderScript, renderSQL)
import SQLSmart (intLit)
import Upload (upload)
import ExamplePlan (uploadPlan)
import AugmentPlan (runQuery, augmentPlan)
import UploadPlan (WorkbenchId(..), UploadPlan(..))
import Common (showWB)

main :: IO ()
main = do
  let script = upload uploadPlan
  putStrLn $ renderSQL (renderScript script)


printPlan :: UploadPlan -> IO ()
printPlan = putStrLn . decodeUtf8 . toStrict . encodePretty

connectTo :: Text -> IO Connection
connectTo dbName = connect defaultConnectInfo {connectDatabase = unpack dbName, connectUser="Master", connectPassword="Master"}

printWB :: Connection -> UploadPlan -> IO ()
printWB conn (UploadPlan {workbenchId=WorkbenchId wbid}) = do
  rows  :: [(Integer, Text)] <- runQuery conn $ showWB (intLit wbid)
  forM_ rows $ \(_, cols) -> putStrLn cols
