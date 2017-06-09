package models

import (
	"bytes"
	"database/sql"
	sqlDriver "database/sql/driver"
	"fmt"
	"github.com/pkg/errors"
	"reflect"
	"strings"

	"github.com/degupta/godao/datastore"
)

/******* Column ******/

type Column struct {
	name       string
	fieldName  string
	fieldType  string
	partOfPKey bool
	auto       bool
	modelInfo  *ModelInfo
}

func (column *Column) GetFieldName() string {
	return column.fieldName
}

func (column *Column) GetFielType() string {
	return column.fieldType
}

func (column *Column) nameForView() string {
	return fmt.Sprintf("%s_in_%s", column.name, column.modelInfo.TableName)
}
func (column *Column) nameWithTable() string {
	return fmt.Sprintf("%s.%s", column.modelInfo.TableName, column.name)
}

type Columns []*Column

func (columns Columns) toCsvForView() string {
	columnNames := make([]string, len(columns))
	for i, column := range columns {
		columnNames[i] = fmt.Sprintf("%s AS %s", column.nameWithTable(), column.nameForView())
	}
	return strings.Join(columnNames, ", ")
}

func (columns Columns) ToAliasedCsv(tableAlias string) string {
	columnNames := make([]string, 0)
	for _, column := range columns {
		if tableAlias == "" {
			columnNames = append(columnNames, column.nameWithTable())
		} else {
			columnNames = append(columnNames, tableAlias+"."+column.name)
		}
	}
	return strings.Join(columnNames, ", ")
}

func (columns Columns) toCsv(tableName bool) string {
	columnNames := make([]string, 0)
	for _, column := range columns {
		if tableName {
			columnNames = append(columnNames, column.nameWithTable())
		} else {
			columnNames = append(columnNames, column.name)
		}
	}
	return strings.Join(columnNames, ", ")
}

func (columns Columns) toAutoCsv(tableName, auto bool) (string, int) {
	columnNames := make([]string, 0)
	for _, column := range columns {
		if auto != (column.partOfPKey || column.auto) {
			continue
		}
		if tableName {
			columnNames = append(columnNames, column.modelInfo.TableName+"."+column.name)
		} else {
			columnNames = append(columnNames, column.name)
		}
	}
	return strings.Join(columnNames, ", "), len(columnNames)
}

/******* Statement ******/
type Statements struct {
	CreateStmt           *sql.Stmt
	UpdateStmt           *sql.Stmt
	DeleteStmt           *sql.Stmt
	SelectStmt           *sql.Stmt
	SelectLast           *sql.Stmt
	InsertIfNotExistStmt *sql.Stmt
}

/******* ModelInfo **********/
// Is also used for a View
type ModelInfo struct {
	reflectedType reflect.Type
	// Table Info
	TableName string
	Columns   Columns // All columns
	createdAt *Column
	pKey      Columns // Columns that belong to the primary key
	notPKey   Columns // Columns that don't belong to the primary key

	// Statements
	statements Statements
}

func (modelInfo *ModelInfo) Name() string {
	return modelInfo.reflectedType.Name()
}

func (modelInfo *ModelInfo) String() string {
	return fmt.Sprintf("Model(%s; table=%s)", modelInfo.Name(), modelInfo.TableName)
}

func (modelInfo *ModelInfo) mergeColumns(other *ModelInfo) {
	modelInfo.Columns = append(modelInfo.Columns, other.Columns...)
	if modelInfo.createdAt == nil {
		modelInfo.createdAt = other.createdAt
	}
	modelInfo.pKey = append(modelInfo.pKey, other.pKey...)
	modelInfo.notPKey = append(modelInfo.notPKey, other.notPKey...)
}

func (modelInfo *ModelInfo) setColumns(generateStatements bool) error {
	n := modelInfo.reflectedType.NumField()

	modelInfo.Columns = make(Columns, 0)
	modelInfo.pKey = make(Columns, 0)
	modelInfo.notPKey = make(Columns, 0)

	for i := 0; i < n; i++ {
		field := modelInfo.reflectedType.Field(i)

		if field.Tag.Get("embedded") == "true" {
			// TODO : Check for non pointer case
			embeddedModelInfo, err := GetModelInfo(reflect.New(field.Type.Elem()).Elem().Interface(), modelInfo.TableName, generateStatements)
			if err != nil {
				return err
			}
			modelInfo.mergeColumns(embeddedModelInfo)
		}

		name := field.Tag.Get("col")
		if name == "" {
			continue
		}
		partOfPKey := field.Tag.Get("id") == "true"
		auto := field.Tag.Get("auto") == "true"
		fieldName := field.Name
		fieldType := field.Type.Name()
		column := &Column{name: name, fieldName: fieldName, fieldType: fieldType, modelInfo: modelInfo, partOfPKey: partOfPKey, auto: auto}

		if partOfPKey {
			modelInfo.pKey = append(modelInfo.pKey, column)
		} else {
			modelInfo.notPKey = append(modelInfo.notPKey, column)
		}
		modelInfo.Columns = append(modelInfo.Columns, column)

		if name == "created_at" {
			modelInfo.createdAt = column
		}
	}

	if len(modelInfo.pKey) == 0 {
		return errors.New("No primary keys specified")
	} else {
		return nil
	}
}

// valueOffset should typically be 0 unless there are clauses that already used some $%d variables
func (modelInfo *ModelInfo) pKeyWhereClause(valueOffset int) string {
	pKeyWhereConditions := []string{}
	for i, pCol := range modelInfo.pKey {
		pKeyWhereConditions = append(pKeyWhereConditions, fmt.Sprintf("%s = $%d", pCol.name, i+valueOffset+1))
	}
	return strings.Join(pKeyWhereConditions, " AND ")
}

func (modelInfo *ModelInfo) CreateInsertIfNotExistStatementWithPKey(returningClause string) error {
	// Ah need to do this ugly hack to simulate Insert if does not exist, but If exists return the row
	id := modelInfo.pKey[0].name
	conflictClause := fmt.Sprintf("(%s) DO UPDATE set %s = EXCLUDED.%s", id, id, id)
	if returningClause == "" {
		returningClause = modelInfo.Columns.toCsv(false)
	}
	if stmt, err := modelInfo.prepareInsertStatementFor(true, returningClause, conflictClause); err != nil {
		return err
	} else {
		modelInfo.statements.InsertIfNotExistStmt = stmt
		return nil
	}
}

// HELPER FUNCTIONS
func writeToBuffer(buf *bytes.Buffer, strings ...string) {
	for _, str := range strings {
		buf.WriteString(str)
	}
}

func valuesCsv(start, end int) string {
	numbers := make([]string, 0)
	for i := start; i <= end; i++ {
		numbers = append(numbers, fmt.Sprintf("$%d", i))
	}
	return strings.Join(numbers, ", ")
}

func PrepareExistsStatementFor(modelInfo *ModelInfo, whereClause string) (*sql.Stmt, error) {
	return datastore.Prepare("SELECT COUNT(*) > 0 FROM (SELECT * FROM " + modelInfo.TableName + " WHERE " + whereClause + " LIMIT 1) as temp")
}

func PrepareCountStatementFor(modelInfo *ModelInfo, whereClause string) (*sql.Stmt, error) {
	return datastore.Prepare("SELECT COUNT(*) FROM " + modelInfo.TableName + " WHERE " + whereClause)
}

func SelectSentence(modelInfo *ModelInfo, distinctCols []string, whereClause, orderAndLimit string) string {
	return SelectSentenceWithAlias(modelInfo, distinctCols, whereClause, orderAndLimit, "")
}

func SelectSentenceWithAlias(modelInfo *ModelInfo, distinctCols []string, whereClause, orderAndLimit, alias string) string {
	return SelectSentenceWithAliasAndJoins(modelInfo, distinctCols, whereClause, orderAndLimit, alias, nil)
}

// SelectJoinOpts is information related to constructing joined statement
type SelectJoinOpts struct {
	ModelInfo     *ModelInfo
	Alias         string
	Condition     string // ON statement
	SelectColumns bool   // select all columns from this table
	JoinSubquery  string // replace join table with join subquery statement
	JoinType      string // type of join, but default INNER JOIN
}

func SelectSentenceWithAliasAndJoins(modelInfo *ModelInfo, distinctCols []string, whereClause, orderAndLimit, alias string, joinOpts []*SelectJoinOpts) string {
	phrases := []string{"SELECT"}
	if len(distinctCols) > 0 {
		phrases = append(phrases, fmt.Sprintf("DISTINCT ON (%s)", strings.Join(distinctCols, ",")))
	}
	var cols string
	if len(joinOpts) > 0 && alias != "" {
		cols = modelInfo.Columns.ToAliasedCsv(alias)
	} else {
		cols = modelInfo.Columns.toCsv(false)
	}
	colIndex := len(phrases)
	phrases = append(phrases, cols, "FROM", modelInfo.TableName)
	if alias != "" {
		phrases = append(phrases, alias)
	}
	for _, opts := range joinOpts {
		joinType := "INNER JOIN"
		if opts.JoinType != "" {
			joinType = opts.JoinType
		}
		phrases = append(phrases, joinType)
		if opts.SelectColumns {
			phrases[colIndex] += "," + opts.ModelInfo.Columns.ToAliasedCsv(opts.Alias)
		}
		var join string
		if opts.JoinSubquery != "" {
			join = "( " + opts.JoinSubquery + " )"
		} else {
			join = opts.ModelInfo.TableName
		}
		phrases = append(phrases, join, opts.Alias, "ON", opts.Condition)
	}
	if whereClause != "" {
		phrases = append(phrases, "WHERE", whereClause)
	}
	if orderAndLimit != "" {
		phrases = append(phrases, "ORDER BY", orderAndLimit)
	}
	return strings.Join(phrases, " ")
}

func TableFuncSelectSentence(modelInfo *ModelInfo, tableFunc, whereClause, orderAndLimit string) string {
	phrases := []string{"SELECT"}
	phrases = append(phrases, modelInfo.Columns.toCsv(false), "FROM", tableFunc)
	if whereClause != "" {
		phrases = append(phrases, "WHERE", whereClause)
	}
	if orderAndLimit != "" {
		phrases = append(phrases, "ORDER BY", orderAndLimit)
	}
	return strings.Join(phrases, " ")
}

func PrepareSelectDistinctStatementFor(modelInfo *ModelInfo, distinctCols []string, whereClause, orderByClause string) (*sql.Stmt, error) {
	return datastore.Prepare(SelectSentence(modelInfo, distinctCols, whereClause, orderByClause))
}

func PrepareSelectStatementFor(modelInfo *ModelInfo, whereClause, orderByClause string) (*sql.Stmt, error) {
	return datastore.Prepare(SelectSentence(modelInfo, nil, whereClause, orderByClause))
}

func PrepareTableFuncSelectStatementFor(modelInfo *ModelInfo, funcName, whereClause, orderByClause string) (*sql.Stmt, error) {
	return datastore.Prepare(TableFuncSelectSentence(modelInfo, funcName, whereClause, orderByClause))
}

func (modelInfo *ModelInfo) prepareInsertStatementFor(includePKeys bool, returningClause, onConflictClause string) (*sql.Stmt, error) {

	columns := make(Columns, 0)

	if includePKeys {
		columns = modelInfo.Columns
	} else {
		columns = modelInfo.notPKey
	}

	phrases := []string{
		"INSERT INTO", modelInfo.TableName,
		"(", columns.toCsv(false), ")",
		"VALUES (", valuesCsv(1, len(columns)), ")"}

	if onConflictClause != "" {
		phrases = append(phrases, "ON CONFLICT", onConflictClause)
	}

	if returningClause == "" {
		phrases = append(phrases, "RETURNING", modelInfo.pKey.toCsv(false))
	} else {
		phrases = append(phrases, "RETURNING", returningClause)
	}

	return datastore.Prepare(strings.Join(phrases, " "))
}

func (modelInfo *ModelInfo) PrepareInsertStatementFor(returningClause string) (*sql.Stmt, error) {
	return modelInfo.prepareInsertStatementFor(false, returningClause, "")
}

func (modelInfo *ModelInfo) PrepareInsertStatementWithPKeyFor(returningClause string) (*sql.Stmt, error) {
	return modelInfo.prepareInsertStatementFor(true, returningClause, "")
}

func PrepareInsertAutoStatementFor(modelInfo *ModelInfo, returningClause string) (*sql.Stmt, error) {
	csv, count := modelInfo.Columns.toAutoCsv(false, false)
	phrases := []string{
		"INSERT INTO", modelInfo.TableName,
		"(", csv, ")",
		"VALUES (", valuesCsv(1, count), ")",
		"RETURNING", returningClause}
	if returningClause == "" {
		phrases[len(phrases)-1] = modelInfo.pKey.toCsv(false)
	}
	return datastore.Prepare(strings.Join(phrases, " "))
}

func PrepareDeleteStatementFor(modelInfo *ModelInfo, whereClause string) (*sql.Stmt, error) {
	phrases := []string{
		"DELETE FROM", modelInfo.TableName,
		"WHERE", whereClause,
		"RETURNING", modelInfo.pKey.toCsv(false)}
	return datastore.Prepare(strings.Join(phrases, " "))
}

func PrepareUpdateStatementFor(modelInfo *ModelInfo, returningClause string) (*sql.Stmt, error) {
	start := len(modelInfo.pKey) + 1
	phrases := []string{
		"UPDATE", modelInfo.TableName,
		"SET", "(", modelInfo.notPKey.toCsv(false), ")", "=", "(", valuesCsv(start, len(modelInfo.notPKey)+start-1), ")",
		"WHERE", modelInfo.pKeyWhereClause(0),
		"RETURNING", modelInfo.pKey.toCsv(false)}
	if returningClause != "" {
		phrases[len(phrases)-1] = returningClause
	}
	return datastore.Prepare(strings.Join(phrases, " "))
}

// Pass argument columns in order first, followed by columns that make up primary key in order
func PrepareUpdateFor(modelInfo *ModelInfo, columns []string) (*sql.Stmt, error) {
	values := make([]string, len(columns))
	for i, _ := range columns {
		values = append(values, fmt.Sprintf("$%d", i+1))
	}
	columnsCsv := strings.Join(columns, ", ")
	valuesCsv := strings.Join(values, ", ")

	phrases := []string{
		"UPDATE", modelInfo.TableName,
		"SET (", columnsCsv, ") = (", valuesCsv, ")",
		"WHERE", modelInfo.pKeyWhereClause(len(columns)),
		"RETURNING", modelInfo.pKey.toCsv(false)}
	return datastore.Prepare(strings.Join(phrases, " "))
}

func (modelInfo *ModelInfo) PrepareGetLast() (*sql.Stmt, error) {
	phrases := []string{
		"SELECT", modelInfo.Columns.toCsv(false),
		"FROM", modelInfo.TableName,
		"ORDER BY", modelInfo.createdAt.name, "DESC LIMIT 1"}
	return datastore.Prepare(strings.Join(phrases, " "))
}

// FIXME: use new api
func PrepareBelongsToStatementsFor(modelInfo *ModelInfo, foreignKey string) (*Statements, error) {
	stmts := &Statements{}
	stmt, err := PrepareSelectStatementFor(modelInfo, fmt.Sprintf("%s = $1", foreignKey), modelInfo.createdAt.name)
	if err != nil {
		return nil, err
	} else {
		stmts.SelectStmt = stmt
	}

	stmt, err = PrepareDeleteStatementFor(modelInfo, fmt.Sprintf("%s = $1", foreignKey))
	if err != nil {
		return nil, err
	} else {
		stmts.DeleteStmt = stmt
	}

	return stmts, nil
}

func createStatements(modelInfo *ModelInfo) error {
	stmt, err := PrepareSelectStatementFor(modelInfo, modelInfo.pKeyWhereClause(0), "")
	if err != nil {
		return err
	} else {
		modelInfo.statements.SelectStmt = stmt
	}

	stmt, err = PrepareUpdateStatementFor(modelInfo, "")
	if err != nil {
		return err
	} else {
		modelInfo.statements.UpdateStmt = stmt
	}

	stmt, err = PrepareDeleteStatementFor(modelInfo, modelInfo.pKeyWhereClause(0))
	if err != nil {
		return err
	} else {
		modelInfo.statements.DeleteStmt = stmt
	}

	stmt, err = modelInfo.PrepareInsertStatementFor("")
	if err != nil {
		return err
	} else {
		modelInfo.statements.CreateStmt = stmt
	}

	stmt, err = modelInfo.PrepareGetLast()
	if err != nil {
		return err
	} else {
		modelInfo.statements.SelectLast = stmt
	}

	return nil
}

// PUBLIC FUNCTIONS

func GetModelInfo(m interface{}, tableName string, generateStatements bool) (*ModelInfo, error) {

	modelInfo := &ModelInfo{TableName: tableName, reflectedType: reflect.TypeOf(m)}

	err := modelInfo.setColumns(generateStatements)
	if err != nil {
		return nil, err
	}

	if generateStatements {
		err = createStatements(modelInfo)
		if err != nil {
			return nil, err
		}
	}

	return modelInfo, nil
}

// This is fairly fragile since it depends heavily on view/column naming conventions
// see the plpgsql function update_view_job_users_and_jobs_and_users_and_job_roles()
// in migrations/20161026113210_job_users_and_roles_view.up.sql to update the view
// when updating its member tables
func CreateJoinViewModelInfo(modelInfos []*ModelInfo) *ModelInfo {
	view := &ModelInfo{}

	tableNames := make([]string, len(modelInfos))
	for i, modelInfo := range modelInfos {
		tableNames[i] = modelInfo.TableName
		for _, column := range modelInfo.Columns {
			viewColumn := &Column{name: column.nameForView(), modelInfo: view}
			view.Columns = append(view.Columns, viewColumn)
		}
	}

	view.TableName = "view_" + strings.Join(tableNames, "_and_")
	view.notPKey = view.Columns
	return view
}

func Create(modelInfo *ModelInfo, autoFields []interface{}, args ...interface{}) error {
	return modelInfo.statements.CreateStmt.QueryRow(args...).Scan(autoFields...)
}

func CreateTx(tx datastore.TxWrapper, modelInfo *ModelInfo, autoFields []interface{}, args ...interface{}) error {
	return tx.Stmt(modelInfo.statements.CreateStmt).QueryRow(args...).Scan(autoFields...)
}

func insertIfNotExist(modelInfo *ModelInfo, allFields []interface{}) error {
	return modelInfo.statements.InsertIfNotExistStmt.QueryRow(allFields...).Scan(allFields...)
}

func InsertIfNotExistTx(tx datastore.TxWrapper, modelInfo *ModelInfo, autoFields []interface{}, args ...interface{}) error {

	if modelInfo.statements.InsertIfNotExistStmt == nil {
		return errors.New("UPSERT not available for " + modelInfo.TableName)
	}

	allFields := append(autoFields, args...)

	if tx != nil {
		return tx.Stmt(modelInfo.statements.InsertIfNotExistStmt).QueryRow(allFields...).Scan(allFields...)
	} else {
		return insertIfNotExist(modelInfo, allFields)
	}
}

func Update(modelInfo *ModelInfo, args ...interface{}) error {
	id := args[0:len(modelInfo.pKey)]
	return modelInfo.statements.UpdateStmt.QueryRow(args...).Scan(id...)
}

func UpdateTx(tx datastore.TxWrapper, modelInfo *ModelInfo, args ...interface{}) error {
	id := args[0:len(modelInfo.pKey)]
	return tx.Stmt(modelInfo.statements.UpdateStmt).QueryRow(args...).Scan(id...)
}

func Delete(modelInfo *ModelInfo, pKey ...interface{}) error {
	return modelInfo.statements.DeleteStmt.QueryRow(pKey...).Scan(pKey...)
}

func DeleteTx(tx datastore.TxWrapper, modelInfo *ModelInfo, pKey ...interface{}) error {
	return tx.Stmt(modelInfo.statements.DeleteStmt).QueryRow(pKey...).Scan(pKey...)
}

type NoSuchObject struct {
	modelInfo *ModelInfo
	pKey      []interface{}
	fKey      []interface{}
	fKeyName  string
}

// formatArgs uses the database/sql default converted to convert args to
// to a printable string
func formatArgs(args []interface{}) string {
	rv := make([]string, len(args))
	for i, arg := range args {
		val, err := sqlDriver.DefaultParameterConverter.ConvertValue(arg)
		if err != nil {
			rv[i] = fmt.Sprintf("%v<!err:%s>", arg, err)
		} else {
			switch value := val.(type) {
			case nil:
				rv[i] = "NULL"
			default:
				rv[i] = fmt.Sprintf("%v", value)
			}
		}
	}
	return strings.Join(rv, ", ")
}

func (nso *NoSuchObject) Error() string {
	args := make([]interface{}, 3)
	args[0] = nso.modelInfo.Name()
	if nso.fKeyName != "" {
		args[1] = nso.fKeyName
		args[2] = formatArgs(nso.fKey)
	} else {
		args[1] = nso.modelInfo.pKey.toCsv(false)
		args[2] = formatArgs(nso.pKey)
	}
	return fmt.Sprintf("No such %s (%s = %s)", args...)
}

// GetById returns an object from the database given a type (ModelInfo
// and primary key).  All columns in the ModelInfo are selected into
// the fields passed via result.  Returns NoSuchObject if an object
// with the ID was not found.
func GetById(modelInfo *ModelInfo, pKey []interface{}, result ...interface{}) error {
	err := modelInfo.statements.SelectStmt.QueryRow(pKey...).Scan(result...)
	switch err {
	case sql.ErrNoRows:
		return &NoSuchObject{modelInfo: modelInfo, pKey: pKey}
	default:
		return errors.Wrapf(err, "Error fetching from %s by id (%s = %s)",
			modelInfo, modelInfo.pKey.toCsv(false), formatArgs(pKey))
	}
}

// GetByIdTx operates just by GetById but uses the passed database
// transaction to do the select.
func GetByIdTx(tx datastore.TxWrapper, modelInfo *ModelInfo, pKey []interface{}, result ...interface{}) error {
	err := tx.Stmt(modelInfo.statements.SelectStmt).QueryRow(pKey...).Scan(result...)
	switch err {
	case sql.ErrNoRows:
		return &NoSuchObject{modelInfo: modelInfo, pKey: pKey}
	default:
		return errors.Wrapf(err, "Error fetching from %s by id (%s = %s)",
			modelInfo, modelInfo.pKey.toCsv(false), formatArgs(pKey))
	}
}

func PrepareJoinStatement(distinctClause string, whereClause string, keys [][2]string, modelInfos ...*ModelInfo) (*sql.Stmt, error) {
	return PrepareJoinStatementWithOrder(distinctClause, whereClause, "", keys, modelInfos...)
}

func PrepareJoinStatementWithOrder(distinctClause, whereClause, orderClause string, keys [][2]string, modelInfos ...*ModelInfo) (*sql.Stmt, error) {
	var selectBuffer bytes.Buffer
	writeToBuffer(&selectBuffer, "SELECT ")
	if distinctClause != "" {
		writeToBuffer(&selectBuffer, " DISTINCT ON ( ", distinctClause+" ) ")
	}
	for _, modelInfo := range modelInfos {
		writeToBuffer(&selectBuffer, modelInfo.Columns.toCsv(true), ", ")
	}
	selectBuffer.Truncate(selectBuffer.Len() - 2) // Delete the last ', '
	writeToBuffer(&selectBuffer, " FROM ", modelInfos[0].TableName)

	for i, modelInfo := range modelInfos[1:] {
		writeToBuffer(&selectBuffer, " INNER JOIN ", modelInfo.TableName, " ON ", modelInfos[i].TableName, ".", keys[i][0], " = ", modelInfo.TableName, ".", keys[i][1])
	}

	if whereClause != "" {
		writeToBuffer(&selectBuffer, " WHERE ", whereClause)
	}
	if orderClause != "" {
		writeToBuffer(&selectBuffer, " ORDER BY ", orderClause)
	}
	return datastore.Prepare(selectBuffer.String())
}

func (primary *ModelInfo) PrepareDistinctJoinStatement(secondary *ModelInfo, keys [2]string, aliases [2]string, whereClause string) (*sql.Stmt, error) {
	phrases := []string{
		"SELECT DISTINCT ON (", primary.Columns.ToAliasedCsv(aliases[0]), ")", primary.Columns.ToAliasedCsv(aliases[0]), "FROM",
		primary.TableName, aliases[0], "INNER JOIN",
		secondary.TableName, aliases[1], "ON",
		aliases[0] + "." + keys[0], "=", aliases[1] + "." + keys[1],
		"WHERE", whereClause}
	return datastore.Prepare(strings.Join(phrases, " "))
}
