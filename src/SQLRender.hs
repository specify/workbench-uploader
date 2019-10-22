module SQLRender where

import Prelude (Maybe(..), id, (.), ($), fmap, mempty, (<>))
import Data.Text (Text)
import Data.Text.Prettyprint.Doc (nest, indent, group, line, sep, (<+>), vsep, comma, hsep, punctuate, parens, squotes, pretty, defaultLayoutOptions, layoutPretty, Doc)
import Data.Text.Prettyprint.Doc.Render.Text (renderStrict)

import SQL (OrderTerm(..), JoinSpec(..), ColumnName(..), TableName(..), JoinedTable(..), TableFactor(..), TableRef(..), SelectTerm(..), SelectType(..), QueryExpr(..), SubQuery(..), FCall(..), Literal(..), BinOp(..), Expr(..), CompOp(..), Alias(..), Identifier(..), Statement(..), Script(..))


notImplemented :: forall a b. a -> Doc b
notImplemented _ =  "<not implemented>"

renderSQL :: Doc a -> Text
renderSQL = renderStrict . layoutPretty defaultLayoutOptions

renderScript :: Script -> Doc a
renderScript (Script statements) = vsep $ fmap ((<>) line . renderStatement) statements

renderStatement :: Statement -> Doc a
renderStatement Commit = "commit;"
renderStatement RollBack = "rollback;"
renderStatement StartTransaction = "start transaction;"
renderStatement (QueryStatement q) = renderQuery q <> ";"
renderStatement (SetUserVar n v) = "set" <+> "@" <> (pretty $ escapeIdentifier n) <+> "=" <+> renderExpr v <> ";"
renderStatement (InsertValues {tableName, columns, values}) =
  group $ vsep
  [ "insert into"
  , (group $ renderTableName tableName <> line <> (parens $ sep $ punctuate comma $ fmap renderColumnName columns))
  , "values"
  , (vsep $ punctuate comma $ fmap (parens . hsep . punctuate comma . fmap renderExpr) values)
  ] <> ";"

renderStatement (InsertFrom {tableName, columns, queryExpr}) =
  group $ vsep
  [ "insert into"
  , (group $ renderTableName tableName <> line <> (parens $ sep $ punctuate comma $ fmap renderColumnName columns))
  , renderQuery queryExpr
  ] <> ";"


renderAlias :: Alias -> Doc a
renderAlias (Alias a) = pretty $ escapeIdentifier a

renderQuery :: QueryExpr -> Doc a
renderQuery (QueryExpr {selectType, selectTerms, fromExpr, where_, having, ordering}) = case selectTerms of
  [] -> mempty
  _ -> group (
    group $ renderSelectType selectType
      <> (nest 4 $ line <> (sep $ punctuate comma $ fmap renderSelectTerm selectTerms))
    )
    <> renderFrom fromExpr
    <> renderWhere where_
    <> renderHaving having
    <> renderOrderBy ordering
  -- renderLimit q.limit

renderWhere :: Maybe Expr -> Doc a
renderWhere Nothing = mempty
renderWhere (Just x) = line <> "where" <+> renderExpr x

renderOrderBy :: [OrderTerm] -> Doc a
renderOrderBy [] = mempty
renderOrderBy terms = line <> "order by" <+> (hsep $ punctuate comma $ fmap renderOrderTerm terms)
  where
    renderOrderTerm (Ascending x) = renderExpr x <+> "asc"
    renderOrderTerm (Descending x) = renderExpr x <+> "desc"

renderHaving :: Maybe Expr -> Doc a
renderHaving Nothing = mempty
renderHaving (Just x) = line <> "having" <+> renderExpr x

renderSelectType :: SelectType -> Doc a
renderSelectType SelectAll = "select"
renderSelectType SelectDistinct = "select distinct"
renderSelectType SelectDistinctRow = "select distinctrow"

renderSelectTerm :: SelectTerm -> Doc a
renderSelectTerm Star = "*"
renderSelectTerm (StarFrom a) = renderAlias a <> ".*"
renderSelectTerm (SelectTerm expr) = renderExpr expr
renderSelectTerm (SelectAs as expr) = renderExpr expr <+> "as" <+> (pretty $ escapeIdentifier as)


renderFrom :: [TableRef] -> Doc a
renderFrom trs = case trs of
  [] -> mempty
  _ -> line <> "from" <+> (sep $ punctuate comma $ fmap renderTableRef trs)

renderTableRef :: TableRef -> Doc a
renderTableRef (TableFactor tf) = renderTableFactor tf
renderTableRef (JoinedTable jt) = renderJoinedTable jt

renderJoinedTable :: JoinedTable -> Doc a
renderJoinedTable (InnerJoin l r o) = group $ renderTableRef l <> line <> "join" <+> renderTableRef r <+>
  case o of
    Nothing -> mempty
    Just on -> renderJoinSpec on

renderJoinedTable (LeftJoin l r o) = group $ renderTableRef l <> line <> "left join" <+> renderTableRef r <+> renderJoinSpec o
renderJoinedTable p@(RightJoin _ _ _) = notImplemented p
renderJoinedTable p@(NaturalLeftJoin _ _) = notImplemented p
renderJoinedTable p@(NaturalRightJoin _ _) = notImplemented p


renderJoinSpec :: JoinSpec -> Doc a
renderJoinSpec (JoinOn expr) = line <> (indent 2 $ "on" <+> renderExpr expr)

renderJoinSpec (JoinUsing cols) = case cols of
  [] -> mempty
  _  -> "using" <+> (parens $ hsep $ punctuate comma $ fmap renderColumnName cols)

renderColumnName :: ColumnName -> Doc a
renderColumnName (ColumnName n) = pretty $ escapeIdentifier n

renderTableFactor :: TableFactor -> Doc a
renderTableFactor p@(Tables _) = notImplemented p
renderTableFactor Dual = "dual"
renderTableFactor (Table {name, as}) = renderTableName name <>
  case as of
    Just a -> " as" <+> renderAlias a
    Nothing -> mempty

renderTableFactor (Query (SubQuery {subquery, alias})) = group $ renderSubQuery subquery <+> "as" <+> renderAlias alias


renderSubQuery :: QueryExpr -> Doc a
renderSubQuery sq = parens $ nest 4 $ line <> renderQuery sq <> line


renderTableName :: TableName -> Doc a
renderTableName (TableName n) = pretty $ escapeIdentifier n

renderExpr :: Expr -> Doc a
renderExpr (OrExpr x y) = parens $ renderExpr x <+> "or" <+> renderExpr y
renderExpr (XorExpr x y) = parens $ renderExpr x <+> "xor" <+> renderExpr y
renderExpr (AndExpr x y) = parens $ renderExpr x <+> "and" <+> renderExpr y
renderExpr (NotExpr x) = parens $ "not" <+> renderExpr x
renderExpr (IsTrueExpr p) = parens $ renderExpr p <+> "is true"
renderExpr (IsFalseExpr p) = parens $ renderExpr p <+> "is false"
renderExpr (IsUnknownExpr p) = parens $ renderExpr p <+> "is unknown"
renderExpr (IsNull p) = parens $ renderExpr p <+> "is null"
renderExpr (IsNotNull p) = parens $ renderExpr p <+> "is not null"
renderExpr (IsNotDistinctFrom x y) = parens $ renderExpr x <+> "<=>" <+> renderExpr y
renderExpr (CompOp op x y) = parens $ renderExpr x <+> renderCompOp op <+> renderExpr y
renderExpr p@(CompAll _ _ _) = notImplemented p
renderExpr p@(CompAny _ _ _) = notImplemented p
renderExpr (InPred x sq) = parens $ renderExpr x <+> "in" <+> renderSubQuery sq
renderExpr (NotInPred x sq) = parens $ renderExpr x <+> "not in" <+> renderSubQuery sq
renderExpr p@(Between _ _ _) = notImplemented p
renderExpr p@(NotBetween _ _ _) = notImplemented p
renderExpr p@(SoundsLike _ _) = notImplemented p
renderExpr p@(Like _ _) = notImplemented p
renderExpr p@(NotLike _ _) = notImplemented p
renderExpr p@(Regexp _ _) = notImplemented p
renderExpr p@(NotRegexp _ _) = notImplemented p
renderExpr (BinOp op x y) = parens $ renderExpr x <+> renderBinOp op <+> renderExpr y
renderExpr (Literal l) = renderLiteral l
renderExpr (IdentExpr i) = renderIdentifier i
renderExpr (FCallExpr (FCall {fname, args})) = pretty fname <> (parens $ hsep $ punctuate comma $ fmap renderExpr args)
renderExpr (VarExpr s) = pretty $ ("@" <> escapeIdentifier s)
renderExpr p@(UnaryOp _ _) = notImplemented p
renderExpr (RowExpr values) = parens $ hsep $ punctuate comma $ fmap renderExpr values
renderExpr (SubQueryExpr (SubQuery {subquery, alias})) = renderSubQuery subquery <+> "as" <+> renderAlias alias
renderExpr p@(Exists _) = notImplemented p
renderExpr (RawExpr s) = parens $ pretty s
renderExpr (Assignment s x) = parens $ "@" <> (pretty $ escapeIdentifier s) <+> ":=" <+> renderExpr x

renderBinOp :: BinOp -> Doc a
renderBinOp AndOp =  "&"
renderBinOp OrOp =  "|"
renderBinOp PlusOp =  "+"
renderBinOp MinusOp =  "-"

renderCompOp :: CompOp -> Doc a
renderCompOp E_Op =  "="
renderCompOp GE_Op =  ">="
renderCompOp G_Op =  ">"
renderCompOp LE_Op =  "<="
renderCompOp L_Op =  "<"
renderCompOp NE_Op =  "<>"

renderLiteral :: Literal -> Doc a
renderLiteral NullLit = "null"
renderLiteral (IntLit i) = pretty i
renderLiteral (FloatLit n) = pretty n
renderLiteral (TextLit s) = squotes $ pretty $ escapeText s

renderIdentifier :: Identifier -> Doc a
renderIdentifier (Identifier s) = pretty $ escapeIdentifier s
renderIdentifier (Qualified (Alias a) s) = (pretty $ escapeIdentifier a) <> "." <> (pretty $ escapeIdentifier s)

escapeText :: Text -> Text
escapeText = id -- todo

escapeIdentifier :: Text -> Text
escapeIdentifier = id -- todo
