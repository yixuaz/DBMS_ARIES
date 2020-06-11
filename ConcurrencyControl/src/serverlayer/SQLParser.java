package serverlayer;

import dbengine.storage.ITuple;
import dbms.SystemCatalog;
import dbms.TableSystemCatalog;
import dbengine.storage.Expression;
import dbengine.storage.tables.ITable;
import dbengine.transaction.LockMode;
import serverlayer.model.InvalidSqlException;
import serverlayer.model.LogicalPlan;
import serverlayer.model.Predicate;
import serverlayer.model.UpdatedFunction;

import java.util.ArrayList;
import java.util.List;

// this is toy code which have a lot of assumption in input
public class SQLParser {
    static final String TABLE_TAG = "from ";
    static final String WHERE_TAG = "where ";
    static final String SET_TAG = "set ";
    static final String VALUES_TAG = "values(";
    static final String SHARE_MODE_TAG = "lock in share mode";
    static final String EXLCUSIVE_MODE_TAG = "for update";
    static final String SELECT = "select ";
    static final String UPDATE = "update ";
    static final String INSERT = "insert ";

    public LogicalPlan parse(String sql, int txnId) throws InvalidSqlException {
        sql = sql.trim().replaceAll(" +", " ");
        LogicalPlan rawPlan = new LogicalPlan();
        if (sql.startsWith(SELECT)) {
            LockMode mode = null;
            if (sql.endsWith(SHARE_MODE_TAG)) {
                mode = LockMode.SHARE;
                sql = sql.substring(0, sql.length() - SHARE_MODE_TAG.length()).trim();
            } else if (sql.endsWith(EXLCUSIVE_MODE_TAG)) {
                mode = LockMode.EXCLUSIVE;
                sql = sql.substring(0, sql.length() - EXLCUSIVE_MODE_TAG.length()).trim();
            }
            rawPlan.setLockMode(mode);
            rawPlan.setTable(getTable(sql, TABLE_TAG));
            rawPlan.setSelectedColumns(getSelections(sql, rawPlan.getTable()));
            rawPlan.setPredicates(getPredicate(rawPlan.getTable(), sql));
        } else if (sql.startsWith(UPDATE)) {
            rawPlan.setLockMode(LockMode.EXCLUSIVE);
            rawPlan.setTable(getTable(sql, UPDATE));
            rawPlan.setSelectedColumns(selectAll(rawPlan.getTable()));
            rawPlan.setPredicates(getPredicate(rawPlan.getTable(), sql));
            rawPlan.setUpdatedValues(getUpdateFunction(rawPlan.getTable(), sql));
        } else if (sql.startsWith(INSERT)) {
            rawPlan.setLockMode(LockMode.INSERT_INTENTION);
            rawPlan.setTable(getTable(sql, INSERT));
            rawPlan.setInsertTuple(getInsertTuple(sql, rawPlan.getTable(), txnId));
        }

        return rawPlan;
    }

    private List<Integer> getSelections(String sql, ITable table) throws InvalidSqlException {
        int ed = sql.indexOf(TABLE_TAG);
        if (ed == -1) {
            throw new InvalidSqlException();
        }
        String rawSelectionsStr = sql.substring(SELECT.length(), ed).trim();
        if ("*".equals(rawSelectionsStr)) {
            return selectAll(table);
        }
        String[] selections = rawSelectionsStr.split(",");
        TableSystemCatalog tableConfig = SystemCatalog.getTableConfig(table);
        List<Integer> res = new ArrayList<>();
        for (String columnName : selections) {
            res.add(tableConfig.getOffset(columnName));
        }
        return res;
    }

    private ITuple getInsertTuple(String sql, ITable table, int txnId) throws InvalidSqlException {
        int idx = sql.indexOf(VALUES_TAG);
        if (idx == -1) {
            throw new InvalidSqlException("invalid insert sql syntax");
        }
        idx += VALUES_TAG.length();
        int end = sql.indexOf(")", idx);
        String[] offsetValues = sql.substring(idx, end).split(",");
        String rawPid = offsetValues[0].trim();
        int pid = rawPid.equals("null") ? SystemCatalog.NULL_PRIMARY_ID : Integer.parseInt(rawPid);
        TableSystemCatalog tableConfig = SystemCatalog.getTableConfig(table);
        List<Comparable> otherFields = new ArrayList<>();
        for (int i = 1; i < tableConfig.columns(); i++) {
            otherFields.add(tableConfig.toRealType(i, removeQuote(offsetValues[i].trim())));
        }
        ITuple ret = table.getClusterIndex().buildInsertTuple(pid, txnId, otherFields.toArray(new Comparable[0]));
        return ret;
    }

    private String removeQuote(String trim) {
        if (trim.length() >= 2 && (trim.charAt(0) == '\'' || trim.charAt(0) == '\"'))
            return trim.substring(1, trim.length() - 1);
        return trim;
    }

    private List<UpdatedFunction> getUpdateFunction(ITable table, String sql) throws InvalidSqlException {
        List<UpdatedFunction> res = new ArrayList<>();
        int idx = sql.indexOf(SET_TAG);
        if (idx == -1) {
            throw new InvalidSqlException();
        }
        int ed = sql.indexOf(WHERE_TAG);
        if (ed < 0) {
            ed = sql.length();
        }
        String[] updates = sql.substring(idx + SET_TAG.length(), ed).split(",");
        for (String update : updates) {
            update = update.trim();
            String[] columnAndVal = update.split("=");
            TableSystemCatalog tableConfig = SystemCatalog.getTableConfig(table);
            int offset = tableConfig.getOffset(columnAndVal[0].trim());
            Comparable val = tableConfig.toRealType(offset, columnAndVal[1].trim());
            res.add(new UpdatedFunction(offset, val));
        }
        return res;
    }

    private List<Predicate> getPredicate(ITable table, String sql) {
        List<Predicate> ret = new ArrayList<>();
        int idx = sql.indexOf(WHERE_TAG);
        if (idx == -1) {
            return ret;
        }
        idx += WHERE_TAG.length();
        int end = sql.length();
        String whereCondition = sql.substring(idx, end).trim();
        String[] conditions = whereCondition.split("and");
        for (String condition : conditions) {
            ret.add(Expression.generatedPredicate(condition, table));
        }
        return ret;
    }

    private ITable getTable(String sql, String table_tag) throws InvalidSqlException {
        int idx = sql.indexOf(table_tag);
        if (idx == -1) {
            throw new InvalidSqlException();
        }
        idx += table_tag.length();
        int end = sql.indexOf(" ", idx);
        if (end < 0) {
            end = sql.length();
        }
        String tablename = sql.substring(idx, end);
        return SystemCatalog.getTable(tablename);
    }

    private List<Integer> selectAll(ITable table) {
        List<Integer> selectAll = new ArrayList<>();
        TableSystemCatalog tableConfig = SystemCatalog.getTableConfig(table);
        for (int i = 0; i < tableConfig.columns(); i++) {
            selectAll.add(i);
        }
        return selectAll;
    }
}
