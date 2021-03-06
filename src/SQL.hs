module SQL where

import Data.Text


data SimpleStatement
  = StartTransaction
  | Commit
  | RollBack


data UpdateStatement = UpdateStatement
  { tables :: [TableRef]
  , where_ :: Maybe Expr
  , set :: [(Expr, Expr)]
  , ordering :: [OrderTerm]
  }

data DeleteStatement = DeleteStatement
  { tableName :: TableName
  , where_ :: Maybe Expr
  }

data InsertValuesStatement = InsertValuesStatement
  { tableName :: TableName
  , columns :: [ColumnName]
  , values :: [[Expr]]
  }

data InsertFromStatement = InsertFromStatement
  { tableName :: TableName
  , columns :: [ColumnName]
  , queryExpr :: QueryExpr
  }

data CreateTempTableStatement = CreateTempTableStatement
  { tableName :: TableName
  , queryExpr :: QueryExpr
  }

data SetUserVarStatement = SetUserVarStatement Text Expr


data QueryExpr = Select SelectExpr | Union SelectExpr QueryExpr

data SelectExpr = SelectExpr
  { selectType :: SelectType
  , selectTerms :: [SelectTerm]
  , fromExpr :: [TableRef]
  , where_ :: Maybe Expr
  , having :: Maybe Expr
  , grouping :: [Expr]
  , ordering :: [OrderTerm]
  , limit :: Maybe LimitExpr
  }

data SelectType
  = SelectAll
  | SelectDistinct
  | SelectDistinctRow

data SelectTerm
  = SelectTerm Expr
  | SelectAs Text Expr
  | Star
  | StarFrom Alias

data OrderTerm = Ascending Expr | Descending Expr

data LimitExpr = LimitExpr {}

data TableRef = TableFactor TableFactor | JoinedTable JoinedTable

data TableFactor
  = Table {name :: TableName, as :: Maybe Alias}
  | Query SubQuery
  | Tables [TableRef]
  | Dual


data SubQuery = SubQuery {subquery :: QueryExpr, alias :: Alias, exposedColumns :: [ColumnName]}

newtype TableName = TableName Text
newtype Alias = Alias Text
newtype ColumnName = ColumnName Text

data JoinedTable
  = InnerJoin TableRef TableRef (Maybe JoinSpec)
  | LeftJoin TableRef TableRef JoinSpec
  | RightJoin TableRef TableRef JoinSpec
  | NaturalLeftJoin TableRef TableRef
  | NaturalRightJoin TableRef TableRef


data JoinSpec = JoinOn Expr | JoinUsing [ColumnName]

data BinOp = AndOp | OrOp | PlusOp | MinusOp

data UnaryOp = NotOp | IsNullOp

data CompOp = E_Op | GE_Op | G_Op | LE_Op | L_Op | NE_Op


data Expr
  = OrExpr Expr Expr
  | XorExpr Expr Expr
  | AndExpr Expr Expr
  | NotExpr Expr
  | IsTrueExpr Expr
  | IsFalseExpr Expr
  | IsUnknownExpr Expr
  | IsNull Expr
  | IsNotNull Expr
  | IsNotDistinctFrom Expr Expr
  | CompOp CompOp Expr Expr
  | CompAll CompOp Expr SubQuery
  | CompAny CompOp Expr SubQuery
  | InPred Expr QueryExpr
  | NotInPred Expr QueryExpr
  | InExpr Expr [Expr]
  | NotInExpr Expr [Expr]
  | Between Expr Expr Expr
  | NotBetween Expr Expr Expr
  | SoundsLike Expr Expr
  | Like Expr Expr
  | NotLike Expr Expr
  | Regexp Expr Expr
  | NotRegexp Expr Expr
  | BinOp BinOp Expr Expr
  -- | PlusInterval Expr IntervalExpr
  -- | MinusInterval Expr IntervalExpr
  | Literal Literal
  | IdentExpr Identifier
  | FCallExpr FCall
  | VarExpr Text
  | UnaryOp UnaryOp Expr
  | RowExpr ([Expr])
  | SubQueryExpr QueryExpr
  | Exists SubQuery
  -- | IntervalExpr IntervalExpr
  | RawExpr Text
  | Assignment Text Expr

data Literal
  = TextLit Text
  | IntLit Int
  | FloatLit Float
  | NullLit

data Identifier
  = Identifier Text
  | Qualified Alias Text


data FCall = FCall {fname :: Text, args :: [Expr]}
