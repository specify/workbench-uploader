module MonadSQL where

import Prelude (($), Monad)
import Data.Text
import Database.MySQL.Simple.QueryResults (QueryResults)
import SQL (QueryExpr, Statement)
import SQLRender (renderStatement, renderQuery, renderSQL)

class Monad m => MonadSQL m where
  execute :: Statement -> m ()
  doQuery :: (QueryResults r) => QueryExpr -> m [r]
  log :: Text -> m ()

logQuery :: MonadSQL m => QueryExpr -> m ()
logQuery q = log $ renderSQL $ renderQuery q

logStatement :: MonadSQL m => Statement -> m ()
logStatement s = log $ renderSQL $ renderStatement s
