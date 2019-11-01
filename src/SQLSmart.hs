module SQLSmart where

import Data.Text (Text)
import SQL hiding (as, having)
import qualified SQL

insertValues :: Text -> [Text] -> [[Expr]] -> Statement
insertValues tableName cols values =
  InsertValues { tableName = TableName tableName, columns = map ColumnName cols, values = values }

insertFrom :: Text -> [Text] -> QueryExpr -> Statement
insertFrom tableName cols queryExpr =
  InsertFrom { tableName = TableName tableName, columns = map ColumnName cols, queryExpr = queryExpr}

update :: [TableRef] -> [(Text, Expr)] -> UpdateStatement
update tables set =
  Update { tables = tables, where_ = Nothing, set = fmap (\(col, val) -> (ColumnName col, val)) set}

when :: UpdateStatement -> Expr -> UpdateStatement
when update when = update { where_ = Just when }

distinct :: QueryExpr -> QueryExpr
distinct q = q { selectType = SelectDistinct }

query :: [SelectTerm] -> QueryExpr
query terms =
  QueryExpr { selectType = SelectAll
            , selectTerms = terms
            , fromExpr = [TableFactor Dual]
            , where_ = Nothing
            , SQL.having = Nothing
            , grouping = []
            , ordering = []
            , limit = Nothing
            }

queryDistinct :: [SelectTerm] -> QueryExpr
queryDistinct terms = distinct $ query terms

from :: QueryExpr -> [TableRef] -> QueryExpr
from q fs = q { fromExpr = from' }
  where
    from' = case fs of
      [] -> [TableFactor Dual]
      _ -> fs

join :: TableRef -> TableRef -> TableRef
join t f = JoinedTable $ InnerJoin t f Nothing

leftJoin :: TableRef -> TableRef -> TableRef
leftJoin t u = JoinedTable $ LeftJoin t u (JoinOn $ intLit 0)

on :: TableRef -> Expr -> TableRef
on (JoinedTable jt) expr = JoinedTable $ case jt of
  InnerJoin l r _ -> InnerJoin l r $ Just $ JoinOn expr
  LeftJoin l r _ ->  LeftJoin l r $ JoinOn expr
  RightJoin l r _ ->  RightJoin l r $ JoinOn expr
  NaturalLeftJoin l r -> NaturalLeftJoin l r
  NaturalRightJoin l r ->  NaturalRightJoin l r

on tf _ = tf

using :: TableRef -> [Text] -> TableRef
using (JoinedTable jt) cols = JoinedTable $ case jt of
  InnerJoin l r _ -> InnerJoin l r $ Just $ JoinUsing $ fmap ColumnName cols
  LeftJoin l r _ ->  LeftJoin l r $ JoinUsing $ fmap ColumnName cols
  RightJoin l r _ ->  RightJoin l r $ JoinUsing $ fmap ColumnName cols
  NaturalLeftJoin l r -> NaturalLeftJoin l r
  NaturalRightJoin l r ->  NaturalRightJoin l r

table :: Text -> TableRef
table name = TableFactor $ Table {name = TableName name, SQL.as = Nothing}

as :: TableRef -> Alias -> TableRef
as t a = case t of
  TableFactor tf -> TableFactor $ tfAs tf a
  JoinedTable jt -> JoinedTable $ jtAs jt a
  where
    tfAs :: TableFactor -> Alias -> TableFactor
    tfAs tf a' = case tf of
      t'@(Table {}) -> t' { SQL.as = Just a' }
      Query sq@(SubQuery {}) -> Query $ sq { SQL.alias = a'}
      Tables ts -> Tables ts
      Dual -> Dual
    jtAs :: JoinedTable -> Alias -> JoinedTable
    jtAs jt a' = case jt of
      InnerJoin l r o -> InnerJoin l (r `as` a') o
      LeftJoin l r o -> LeftJoin l (r `as` a') o
      RightJoin l r o -> RightJoin l (r `as` a') o
      NaturalLeftJoin l r -> NaturalLeftJoin l (r `as` a')
      NaturalRightJoin l r -> NaturalRightJoin l (r `as` a')


suchThat :: QueryExpr -> Expr -> QueryExpr
suchThat q expr = q { where_ = Just expr }

having :: QueryExpr -> Expr -> QueryExpr
having q expr = q { SQL.having = Just expr }

groupBy :: QueryExpr -> [Expr] -> QueryExpr
groupBy q exprs = q { grouping = exprs }

orderBy :: QueryExpr -> [OrderTerm] -> QueryExpr
orderBy q terms = q { ordering = terms }

asc :: Expr -> OrderTerm
asc = Ascending

desc :: Expr -> OrderTerm
desc = Descending

floatLit :: Float -> Expr
floatLit n = Literal $ FloatLit n

intLit :: Int -> Expr
intLit i = Literal $ IntLit i

stringLit :: Text -> Expr
stringLit s = Literal $ TextLit s

null :: Expr
null = Literal $ NullLit

select :: Expr -> SelectTerm
select = SelectTerm

selectAs :: Text -> Expr -> SelectTerm
selectAs = SelectAs

(@@) :: Alias -> Text -> Expr
(@@) alias name = IdentExpr $ Qualified alias name

project :: Text -> Expr
project name = IdentExpr $ Identifier name

userVar :: Text -> Expr
userVar = VarExpr

not :: Expr -> Expr
not = NotExpr

and :: Expr -> Expr -> Expr
and = AndExpr

equal :: Expr -> Expr -> Expr
equal x y = CompOp E_Op x y

nullIf :: Expr -> Expr -> Expr
nullIf x y = FCallExpr $ FCall {fname = "nullif", args = [x, y]}

strToDate :: Expr -> Expr -> Expr
strToDate format value = FCallExpr $ FCall {fname = "strtodate", args = [format, value]}

max :: Expr -> Expr
max x = FCallExpr $ FCall {fname = "max", args = [x]}

locate :: Expr -> Expr -> Expr
locate sub str = FCallExpr $ FCall {fname = "locate", args = [sub, str]}

plus :: Expr -> Expr -> Expr
plus x y = BinOp PlusOp x y

inSubQuery :: Expr -> QueryExpr -> Expr
inSubQuery = InPred

notInSubQuery :: Expr -> QueryExpr -> Expr
notInSubQuery = NotInPred

in_ :: Expr -> [Expr] -> Expr
in_ = InExpr

notIn_ :: Expr -> [Expr] -> Expr
notIn_ = NotInExpr

regexp :: Expr -> Expr -> Expr
regexp = Regexp

subqueryAs :: Alias -> QueryExpr -> TableRef
subqueryAs as query = TableFactor $ Query $ SubQuery {subquery =  query, alias = as, exposedColumns = []}

scalarSubQuery :: QueryExpr -> Expr
scalarSubQuery = SubQueryExpr

(<=>) :: Expr -> Expr -> Expr
(<=>) = IsNotDistinctFrom

row :: [Expr] -> Expr
row = RowExpr

rawExpr :: Text -> Expr
rawExpr = RawExpr

setUserVar :: Text -> Expr -> Statement
setUserVar = SetUserVar

(@=) :: Text -> Expr -> Expr
(@=) = Assignment


star :: SelectTerm
star = Star

starFrom :: Alias -> SelectTerm
starFrom = StarFrom

alias :: Text -> Alias
alias = Alias

startTransaction :: Statement
startTransaction = StartTransaction

commit :: Statement
commit = Commit

rollback :: Statement
rollback = RollBack
