package serverlayer;

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
    static final String SHARE_MODE_TAG = "in share mode";
    static final String EXLCUSIVE_MODE_TAG = "for update";
    public LogicalPlan parse(String sql) throws InvalidSqlException {
        sql = sql.trim().replaceAll(" +"," ");
        LogicalPlan rawPlan = new LogicalPlan();
        if (sql.startsWith("select")) {
            // assumption we only support select *

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
            rawPlan.setSelectedColumns(selectAll(rawPlan.getTable()));
            rawPlan.setPredicates(getPredicate(rawPlan.getTable(), sql));
        } else if (sql.startsWith("update")) {
            rawPlan.setLockMode(LockMode.EXCLUSIVE);
            rawPlan.setTable(getTable(sql, "update "));
            rawPlan.setSelectedColumns(selectAll(rawPlan.getTable()));
            rawPlan.setPredicates(getPredicate(rawPlan.getTable(), sql));
            rawPlan.setUpdatedValues(getUpdateFunction(rawPlan.getTable(), sql));
        }
        return rawPlan;
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
        int all = table.columns();
        for (int i = 0; i < all; i++) selectAll.add(i);
        return selectAll;
    }
}
