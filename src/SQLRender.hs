module SQLRender where

import qualified Prelude as P
import Prelude (Maybe(..), id, (.), ($), pure, fmap, mempty, sequence_, (<>), (>>=))
import Data.Text (pack, Text)
import Data.List (intercalate)
import Control.Monad (forM_)
import Control.Monad.Writer (execWriter, Writer, tell)

import SQL (OrderTerm(..), JoinSpec(..), ColumnName(..), TableName(..), JoinedTable(..), TableFactor(..), TableRef(..), SelectTerm(..), SelectType(..), QueryExpr(..), SubQuery(..), FCall(..), Literal(..), BinOp(..), Expr(..), CompOp(..), Alias(..), Identifier(..), Statement(..), Script(..))

type SQL = Writer Text ()

show :: forall a. P.Show a => a -> Text
show = pack . P.show

notImplemented :: forall a. a -> SQL
notImplemented _ = do
  tell "<not implemented>"

renderSQL :: SQL -> Text
renderSQL s = execWriter s

separatedBy :: forall a. SQL -> (a -> SQL) -> [a] -> SQL
separatedBy separator render xs =
  sequence_ $ intercalate [separator] $ fmap (pure . render) xs

commaSeparated :: forall a. (a -> SQL) -> [a] -> SQL
commaSeparated = separatedBy cs

nl :: SQL
nl = tell "\n"

sp :: SQL
sp = tell " "

cs :: SQL
cs = tell ", "

cnl :: SQL
cnl = tell ",\n"

endStatement :: SQL
endStatement = tell ";\n"

inParens :: SQL -> SQL
inParens s = do
  tell "("
  s
  tell ")"

renderScript :: Script -> SQL
renderScript (Script statements) = forM_ statements renderStatement

renderStatement :: Statement -> SQL
renderStatement (QueryStatement q) = do
  renderQuery q
  endStatement

renderStatement (InsertValues {tableName, columns, values}) = do
  tell "insert into"
  sp
  renderTableName tableName
  sp
  inParens $ commaSeparated renderColumnName columns
  nl
  tell "values"
  nl
  separatedBy cnl (inParens . commaSeparated renderExpr) values
  endStatement

renderStatement (InsertFrom {tableName, columns, queryExpr}) = do
  tell "insert into"
  sp
  renderTableName tableName
  sp
  inParens $ commaSeparated renderColumnName columns
  nl
  renderQuery queryExpr
  endStatement

renderStatement (SetUserVar n v) = do
  tell "set "
  tell "@"
  tell $ escapeIdentifier n
  tell " = "
  renderExpr v
  endStatement

renderAlias :: Alias -> SQL
renderAlias (Alias a) = tell $ escapeIdentifier a

renderQuery :: QueryExpr -> SQL
renderQuery (QueryExpr {selectType, selectTerms, fromExpr, where_, ordering}) = case selectTerms of
  [] -> tell mempty
  _ -> do
    renderSelectType selectType
    sp
    commaSeparated renderSelectTerm selectTerms
    renderFrom fromExpr
    renderWhere where_
    renderOrderBy ordering
  -- renderLimit q.limit

renderWhere :: Maybe Expr -> SQL
renderWhere Nothing = tell mempty
renderWhere (Just x) = do
  nl
  tell "where "
  renderExpr x

renderOrderBy :: [OrderTerm] -> SQL
renderOrderBy [] = tell mempty
renderOrderBy terms = do
  nl
  tell "order by"
  sp
  commaSeparated renderOrderTerm terms
  where
    renderOrderTerm (Ascending x) = renderExpr x >>= \_ -> tell " asc"
    renderOrderTerm (Descending x) = renderExpr x >>= \_ -> tell " desc"

renderSelectType :: SelectType -> SQL
renderSelectType SelectAll = tell "select"
renderSelectType SelectDistinct = tell "select distinct"
renderSelectType SelectDistinctRow = tell "select distinctrow"

renderSelectTerm :: SelectTerm -> SQL
renderSelectTerm Star = tell "*"
renderSelectTerm (StarFrom (Alias c)) = tell $ c <> ".*"
renderSelectTerm (SelectTerm expr) = renderExpr expr
renderSelectTerm (SelectAs as expr) = do
  renderExpr expr
  sp
  tell "as"
  sp
  tell as

renderFrom :: [TableRef] -> SQL
renderFrom trs = case trs of
  [] -> tell mempty
  _ -> do
    nl
    tell "from "
    commaSeparated renderTableRef trs

renderTableRef :: TableRef -> SQL
renderTableRef (TableFactor tf) = renderTableFactor tf
renderTableRef (JoinedTable jt) = renderJoinedTable jt

renderJoinedTable :: JoinedTable -> SQL
renderJoinedTable (InnerJoin l r o) = do
  renderTableRef l
  tell " join "
  renderTableRef r
  case o of
    Nothing -> tell mempty
    Just on -> renderJoinSpec on

renderJoinedTable (LeftJoin l r o) = do
  renderTableRef l
  tell " left join "
  renderTableRef r
  renderJoinSpec o

renderJoinedTable p@(RightJoin _ _ _) = notImplemented p
renderJoinedTable p@(NaturalLeftJoin _ _) = notImplemented p
renderJoinedTable p@(NaturalRightJoin _ _) = notImplemented p


renderJoinSpec :: JoinSpec -> SQL
renderJoinSpec (JoinOn expr) = do
  tell " on "
  renderExpr expr

renderJoinSpec (JoinUsing cols) = case cols of
  [] -> tell mempty
  _  -> do
    tell " using "
    inParens $ commaSeparated renderColumnName cols

renderColumnName :: ColumnName -> SQL
renderColumnName (ColumnName n) = tell $ escapeIdentifier n

renderTableFactor :: TableFactor -> SQL
renderTableFactor p@(Tables _) = notImplemented p
renderTableFactor Dual = tell "dual"
renderTableFactor (Table {name, as}) = do
  renderTableName name
  case as of
    Just (Alias a) -> do
      tell " as "
      tell a
    Nothing ->
      tell mempty

renderTableFactor (Query (SubQuery {subquery, alias})) = do
  inParens $ do
    nl
    renderQuery subquery
    nl
  tell " as "
  renderAlias alias

renderTableName :: TableName -> SQL
renderTableName (TableName n) = tell $ escapeIdentifier n

renderExpr :: Expr -> SQL
renderExpr (OrExpr x y) = inParens $ do
  renderExpr x
  sp
  tell "or"
  sp
  renderExpr y

renderExpr (XorExpr x y) = inParens $ do
  renderExpr x
  sp
  tell "xor"
  sp
  renderExpr y

renderExpr (AndExpr x y) = inParens $ do
  renderExpr x
  sp
  tell "and"
  sp
  renderExpr y

renderExpr (NotExpr x) = inParens $ do
  tell "not"
  sp
  renderExpr x

renderExpr (IsTrueExpr p) = inParens $ do
  renderExpr p
  sp
  tell "is true"

renderExpr (IsFalseExpr p) = inParens $ do
  renderExpr p
  sp
  tell "is false"

renderExpr (IsUnknownExpr p) = inParens $ do
  renderExpr p
  sp
  tell "is unknown"

renderExpr (IsNull p) = inParens $ do
  renderExpr p
  sp
  tell "is null"

renderExpr (IsNotNull p) = inParens $ do
  renderExpr p
  sp
  tell "is not null"

renderExpr (IsNotDistinctFrom x y) = inParens $ do
  renderExpr x
  sp
  tell "<=>"
  sp
  renderExpr y

renderExpr (CompOp op x y) = inParens $ do
  renderExpr x
  sp
  renderCompOp op
  sp
  renderExpr y

renderExpr p@(CompAll _ _ _) = notImplemented p
renderExpr p@(CompAny _ _ _) = notImplemented p

renderExpr (InPred x sq) = inParens $ do
  renderExpr x
  sp
  tell "in"
  sp
  inParens $ do
    nl
    renderQuery sq
    nl

renderExpr (NotInPred x sq) = inParens $ do
  renderExpr x
  sp
  tell "not in"
  sp
  inParens $ do
    nl
    renderQuery sq
    nl


renderExpr p@(Between _ _ _) = notImplemented p
renderExpr p@(NotBetween _ _ _) = notImplemented p
renderExpr p@(SoundsLike _ _) = notImplemented p
renderExpr p@(Like _ _) = notImplemented p
renderExpr p@(NotLike _ _) = notImplemented p
renderExpr p@(Regexp _ _) = notImplemented p
renderExpr p@(NotRegexp _ _) = notImplemented p


renderExpr (BinOp op x y) = inParens $ do
  renderExpr x
  sp
  renderBinOp op
  sp
  renderExpr y


renderExpr (Literal l) = renderLiteral l
renderExpr (IdentExpr i) = renderIdentifier i

renderExpr (FCallExpr (FCall {fname, args})) = do
  tell fname
  inParens $ commaSeparated renderExpr args

renderExpr (VarExpr s) = tell $ ("@" <> escapeIdentifier s)

renderExpr p@(UnaryOp _ _) = notImplemented p

renderExpr (RowExpr values) = inParens $ commaSeparated renderExpr values

renderExpr (SubQueryExpr (SubQuery {subquery, alias})) = do
  inParens $ do
    nl
    renderQuery subquery
    nl
  tell " as "
  renderAlias alias


renderExpr p@(Exists _) = notImplemented p
renderExpr (RawExpr s) = inParens $ tell s
renderExpr (Assignment s x) = inParens $ do
  tell s
  tell " := "
  renderExpr x

renderBinOp :: BinOp -> SQL
renderBinOp AndOp = tell "&"
renderBinOp OrOp = tell "|"
renderBinOp PlusOp = tell "+"
renderBinOp MinusOp = tell "-"

renderCompOp :: CompOp -> SQL
renderCompOp E_Op = tell "="
renderCompOp GE_Op = tell ">="
renderCompOp G_Op = tell ">"
renderCompOp LE_Op = tell "<="
renderCompOp L_Op = tell "<"
renderCompOp NE_Op = tell "<>"

renderLiteral :: Literal -> SQL
renderLiteral (IntLit i) = tell $ show i
renderLiteral (FloatLit n) = tell $ show n
renderLiteral (TextLit s) = do
  tell "'"
  tell $ escapeText s
  tell "'"

renderIdentifier :: Identifier -> SQL
renderIdentifier (Identifier s) = tell $ escapeIdentifier s
renderIdentifier (Qualified (Alias a) s) = do
  tell $ escapeIdentifier a
  tell "."
  tell $ escapeIdentifier s


escapeText :: Text -> Text
escapeText = id -- todo

escapeIdentifier :: Text -> Text
escapeIdentifier = id -- todo
