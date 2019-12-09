module MonadSQL where

import Prelude (($), Monad)
import Data.Text
import Database.MySQL.Simple.QueryResults (QueryResults)
import SQL (QueryExpr)
import SQLRender (renderStatement, renderQuery, renderSQL, Executable(..))

class Monad m => MonadSQL m where
  execute :: Executable s => s -> m ()
  doQuery :: (QueryResults r) => QueryExpr -> m [r]
  log :: Text -> m ()

logQuery :: MonadSQL m => QueryExpr -> m ()
logQuery q = log $ renderSQL $ renderQuery q

logStatement :: (MonadSQL m, Executable s) => s -> m ()
logStatement s = log $ renderStatement s
