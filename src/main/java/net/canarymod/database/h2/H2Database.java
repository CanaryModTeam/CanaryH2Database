package net.canarymod.database.h2;

import net.canarymod.config.Configuration;
import net.canarymod.database.Column;
import net.canarymod.database.DataAccess;
import net.canarymod.database.Database;
import net.canarymod.database.exceptions.DatabaseAccessException;
import net.canarymod.database.exceptions.DatabaseReadException;
import net.canarymod.database.exceptions.DatabaseTableInconsistencyException;
import net.canarymod.database.exceptions.DatabaseWriteException;
import org.apache.logging.log4j.LogManager;

import java.io.File;
import java.sql.*;
import java.util.*;

import static net.canarymod.Canary.log;

/**
 * Represents access to a H2 database
 *
 * @author Aaron Sommers (somners)
 * @author Chris Ksoll (damagefilter)
 * @author Jason Jones (darkdiplomat)
 */
public class H2Database extends Database {

    private final Connection conn;
    private final String LIST_REGEX = "\u00B6";
    private final String NULL_STRING = "NULL";

    public H2Database() {
        try {
            File dir = new File("db/h2/");
            dir.mkdirs();

            DriverManager.registerDriver(new org.h2.Driver());
            conn = DriverManager.getConnection("jdbc:h2:./db/h2/" + Configuration.getDbConfig().getDatabaseName());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void insert(DataAccess data) throws DatabaseWriteException {
        if (this.doesEntryExist(data)) {
            return;
        }
        PreparedStatement ps = null;

        try {
            StringBuilder fields = new StringBuilder();
            StringBuilder values = new StringBuilder();
            HashMap<Column, Object> columns = data.toDatabaseEntryList();
            Iterator<Column> it = columns.keySet().iterator();

            Column column;
            while (it.hasNext()) {
                column = it.next();
                if (!column.autoIncrement()) {
                    fields.append(cleanColumnName(column.columnName())).append(",");
                    values.append("?").append(",");
                }
            }
            if (fields.length() > 0) {
                fields.deleteCharAt(fields.length() - 1);
            }
            if (values.length() > 0) {
                values.deleteCharAt(values.length() - 1);
            }
            ps = conn.prepareStatement("INSERT INTO " + data.getName() + " (" + fields.toString() + ") VALUES(" + values.toString() + ")");

            int i = 1;
            for (Column c : columns.keySet()) {
                if (!c.autoIncrement()) {
                    setToStatement(i, columns.get(c), ps, c);
                    i++;
                }
            }

            if (ps.executeUpdate() == 0) {
                throw new DatabaseWriteException("Error inserting H2: no rows updated!");
            }
        } catch (SQLException ex) {
            log.error(ex.getMessage(), ex);
        } catch (DatabaseTableInconsistencyException dtie) {
            log.error(dtie.getMessage(), dtie);
        } finally {
            close(ps, null);
        }
    }

    @Override
    public void update(DataAccess data, Map<String, Object> filters) throws DatabaseWriteException {
        if (!this.doesEntryExist(data)) {
            return;
        }
        ResultSet rs = null;

        try {
            rs = this.getResultSet(conn, data, filters, true);
            if (rs != null) {
                if (rs.next()) {
                    HashMap<Column, Object> columns = data.toDatabaseEntryList();
                    Iterator<Column> it = columns.keySet().iterator();
                    Column column;
                    while (it.hasNext()) {
                        column = it.next();

                        /* FIXME: Fix null ID update attempts */
                        if (column.columnName().equals("id")) {
                            continue;
                        }

                        if (column.isList()) {
                            rs.updateObject(cleanColumnName(column.columnName()), this.getString((List<?>) columns.get(column)));
                        }
                        else {
                            rs.updateObject(cleanColumnName(column.columnName()), columns.get(column));
                        }
                    }
                    rs.updateRow();
                }
                else {
                    throw new DatabaseWriteException("Error updating DataAccess to H2, no such entry: " + data.toString());
                }
            }
        } catch (SQLException ex) {
            log.error(ex.getMessage(), ex);
        } catch (DatabaseTableInconsistencyException dtie) {
            log.error(dtie.getMessage(), dtie);
        } catch (DatabaseReadException e) {
            log.error(e.getMessage(), e);
        } finally {
            PreparedStatement st = null;
            try {
                st = rs != null && rs.getStatement() instanceof PreparedStatement ? (PreparedStatement) rs.getStatement() : null;
            } catch (SQLException e) {
                log.error(e.getMessage(), e);
            }
            close(st, rs);
        }
    }

    @Override
    public void remove(DataAccess dataAccess, Map<String, Object> filters) throws DatabaseWriteException {
        PreparedStatement ps = null;

        try {
            if (filters.size() > 0) {
                StringBuilder sb = new StringBuilder();
                Object[] fieldNames = filters.keySet().toArray();
                for (int i = 0; i < fieldNames.length && i < fieldNames.length; i++) {
                    sb.append(fieldNames[i]);
                    if (i + 1 < fieldNames.length) {
                        sb.append("=? AND ");
                    }
                    else {
                        sb.append("=?");
                    }
                }

                ps = conn.prepareStatement("DELETE FROM " + dataAccess.getName() + " WHERE " + sb.toString() + "LIMIT 1");
                for (int i = 0; i < fieldNames.length && i < fieldNames.length; i++) {
                    String fieldName = String.valueOf(fieldNames[i]);
                    Column col = dataAccess.getColumnForName(fieldName);
                    if (col == null) {
                        throw new DatabaseReadException("Error deleting H2 row in " + dataAccess.getName() + ". Column " + fieldNames[i] + " does not exist!");
                    }
                    setToStatement(i + 1, filters.get(fieldName), ps, col);
                }

                if (ps.executeUpdate() == 0) {
                    throw new DatabaseWriteException("Error removing from H2: no rows updated!");
                }
            }

        } catch (DatabaseReadException dre) {
            log.error(dre.getMessage(), dre);
        } catch (SQLException ex) {
            log.error(ex.getMessage(), ex);
        } finally {
            close(ps, null);
        }
    }

    @Override
    public void removeAll(DataAccess dataAccess, Map<String, Object> filters) throws DatabaseWriteException {
        PreparedStatement ps = null;

        try {
            if (filters.size() > 0) {
                StringBuilder sb = new StringBuilder();
                Object[] fieldNames = filters.keySet().toArray();
                for (int i = 0; i < fieldNames.length && i < fieldNames.length; i++) {
                    sb.append(fieldNames[i]);
                    if (i + 1 < fieldNames.length) {
                        sb.append("=? AND ");
                    }
                    else {
                        sb.append("=?");
                    }
                }

                ps = conn.prepareStatement("DELETE FROM " + dataAccess.getName() + " WHERE " + sb.toString());
                for (int i = 0; i < fieldNames.length && i < fieldNames.length; i++) {
                    String fieldName = String.valueOf(fieldNames[i]);
                    Column col = dataAccess.getColumnForName(fieldName);
                    if (col == null) {
                        throw new DatabaseReadException("Error deleting H2 row in " + dataAccess.getName() + ". Column " + fieldNames[i] + " does not exist!");
                    }
                    setToStatement(i + 1, filters.get(fieldName), ps, col);
                }

                if (ps.executeUpdate() == 0) {
                    throw new DatabaseWriteException("Error removing from H2: no rows updated!");
                }
            }

        } catch (DatabaseReadException dre) {
            log.error(dre.getMessage(), dre);
        } catch (SQLException ex) {
            log.error(ex.getMessage(), ex);
        } finally {
            close(ps, null);
        }
    }

    @Override
    public void load(DataAccess da, Map<String, Object> filters) throws DatabaseReadException {
        ResultSet rs = null;
        HashMap<String, Object> dataSet = new HashMap<String, Object>();
        try {
            rs = this.getResultSet(conn, da, filters, true);
            if (rs != null) {
                if (rs.next()) {
                    for (Column column : da.getTableLayout()) {
                        if (column.isList()) {
                            dataSet.put(column.columnName(), this.getList(column.dataType(), rs.getString(cleanColumnName(column.columnName()))));
                        }
                        else if (rs.getObject(cleanColumnName(column.columnName())) instanceof Boolean) {
                            dataSet.put(column.columnName(), rs.getBoolean(cleanColumnName(column.columnName())));
                        }
                        else {
                            dataSet.put(column.columnName(), rs.getObject(cleanColumnName(column.columnName())));
                        }
                    }
                    da.load(dataSet);
                }
            }
        } catch (DatabaseReadException dre) {
            log.error(dre.getMessage(), dre);
        } catch (SQLException ex) {
            log.error(ex.getMessage(), ex);
        } catch (DatabaseTableInconsistencyException dtie) {
            log.error(dtie.getMessage(), dtie);
        } catch (DatabaseAccessException e) {
            log.error(e.getMessage(), e);
        } finally {
            try {
                PreparedStatement st = rs != null && rs.getStatement() instanceof PreparedStatement ? (PreparedStatement) rs.getStatement() : null;
                close(st, rs);
            } catch (SQLException ex) {
                log.error(ex.getMessage(), ex);
            }
        }
    }

    @Override
    public void loadAll(DataAccess typeTemplate, List<DataAccess> datasets, Map<String, Object> filters) throws DatabaseReadException {
        ResultSet rs = null;
        List<HashMap<String, Object>> stuff = new ArrayList<HashMap<String, Object>>();
        try {
            rs = this.getResultSet(conn, typeTemplate, filters, false);
            if (rs != null) {
                while (rs.next()) {
                    HashMap<String, Object> dataSet = new HashMap<String, Object>();
                    for (Column column : typeTemplate.getTableLayout()) {
                        if (column.isList()) {
                            dataSet.put(column.columnName(), this.getList(column.dataType(), rs.getString(cleanColumnName(column.columnName()))));
                        }
                        else if (rs.getObject(cleanColumnName(column.columnName())) instanceof Boolean) {
                            dataSet.put(column.columnName(), rs.getBoolean(cleanColumnName(column.columnName())));
                        }
                        else {
                            dataSet.put(column.columnName(), rs.getObject(cleanColumnName(column.columnName())));
                        }
                    }
                    stuff.add(dataSet);
                }
            }
        } catch (DatabaseReadException dre) {
            log.error(dre.getMessage(), dre);
        } catch (SQLException ex) {
            log.error(ex.getMessage(), ex);
        } catch (DatabaseTableInconsistencyException dtie) {
            log.error(dtie.getMessage(), dtie);
        } finally {
            try {
                PreparedStatement st = rs != null && rs.getStatement() instanceof PreparedStatement ? (PreparedStatement) rs.getStatement() : null;
                close(st, rs);
            } catch (SQLException ex) {
                log.error(ex.getMessage(), ex);
            }
        }
        try {
            for (HashMap<String, Object> temp : stuff) {
                DataAccess newData = typeTemplate.getInstance();
                newData.load(temp);
                datasets.add(newData);
            }
        } catch (DatabaseAccessException dae) {
            log.error(dae.getMessage(), dae);
        }
    }

    @Override
    public void updateSchema(DataAccess schemaTemplate) throws DatabaseWriteException {
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            // First check if the table exists, if it doesn't we'll skip the rest
            // of this method since we're creating it fresh.
            DatabaseMetaData metadata = conn.getMetaData();
            rs = metadata.getTables(null, null, schemaTemplate.getName(), null);
            if (!rs.first()) {
                this.createTable(schemaTemplate);
            }
            else {

                LinkedList<String> toRemove = new LinkedList<String>();
                HashMap<String, Column> toAdd = new HashMap<String, Column>();
                Iterator<Column> it = schemaTemplate.getTableLayout().iterator();

                // TODO: Should update primary keys ...
                Column column;
                while (it.hasNext()) {
                    column = it.next();
                    toAdd.put(cleanColumnName(column.columnName()), column);
                }

                for (String col : this.getColumnNames(schemaTemplate)) {
                    if (!toAdd.containsKey(col)) {
                        toRemove.add(col);
                    }
                    else {
                        toAdd.remove(col);
                    }
                }

                for (String name : toRemove) {
                    this.deleteColumn(schemaTemplate.getName(), name);
                }
                for (Map.Entry<String, Column> entry : toAdd.entrySet()) {
                    this.insertColumn(schemaTemplate.getName(), entry.getValue());
                }
            }
        } catch (SQLException sqle) {
            throw new DatabaseWriteException("Error updating H2 schema: " + sqle.getMessage());
        } catch (DatabaseTableInconsistencyException dtie) {
            log.error("Error updating H2 schema." + dtie.getMessage(), dtie);
        } finally {
            close(ps, rs);
        }
    }

    public void createTable(DataAccess data) throws DatabaseWriteException {
        PreparedStatement ps = null;

        try {
            StringBuilder fields = new StringBuilder();
            HashMap<Column, Object> columns = data.toDatabaseEntryList();
            Iterator<Column> it = columns.keySet().iterator();
            List<String> primary = new ArrayList<String>(2);

            Column column;
            while (it.hasNext()) {
                column = it.next();
                fields.append(cleanColumnName(column.columnName())).append(" ");
                fields.append(this.getDataTypeSyntax(column.dataType()));
                if (column.autoIncrement()) {
                    fields.append(" AUTO_INCREMENT");
                }
                if (column.columnType().equals(Column.ColumnType.PRIMARY)) {
                    primary.add(cleanColumnName(column.columnName()));
                }
                if (it.hasNext()) {
                    fields.append(", ");
                }
            }
            if (primary.size() > 0) {
                fields.append(", PRIMARY KEY(").append(
                        net.visualillusionsent.utils.StringUtils.joinString(primary.toArray(new String[primary.size()]), ",", 0)
                                                      )
                      .append(")");
            }
            ps = conn.prepareStatement("CREATE TABLE IF NOT EXISTS " + data.getName() + "(" + fields.toString() + ")");
            ps.execute();
        } catch (SQLException ex) {
            throw new DatabaseWriteException("Error creating H2 table '" + data.getName() + "'. " + ex.getMessage());
        } catch (DatabaseTableInconsistencyException ex) {
            log.error(ex.getMessage() + " Error creating H2 table '" + data.getName() + "'. ", ex);
        } finally {
            close(ps, null);
        }
    }

    public void insertColumn(String tableName, Column column) throws DatabaseWriteException {
        PreparedStatement ps = null;

        try {
            if (column != null && !column.columnName().trim().equals("")) {
                ps = conn.prepareStatement("ALTER TABLE " + tableName + " ADD " + cleanColumnName(column.columnName()) + " " + this.getDataTypeSyntax(column.dataType()));
                ps.execute();
            }
        } catch (SQLException ex) {
            throw new DatabaseWriteException("Error adding H2 column: " + column.columnName());
        } finally {
            close(ps, null);
        }
    }

    public void deleteColumn(String tableName, String columnName) throws DatabaseWriteException {
        PreparedStatement ps = null;

        try {
            if (columnName != null && !columnName.trim().equals("")) {
                ps = conn.prepareStatement("ALTER TABLE " + tableName + " DROP " + columnName);
                ps.execute();
            }
        } catch (SQLException ex) {
            throw new DatabaseWriteException("Error deleting H2 column: " + columnName);
        } finally {
            close(ps, null);
        }
    }

    public boolean doesEntryExist(DataAccess data) throws DatabaseWriteException {
        PreparedStatement ps = null;
        ResultSet rs = null;
        boolean toRet = false;

        try {
            StringBuilder sb = new StringBuilder();
            HashMap<Column, Object> columns = data.toDatabaseEntryList();
            Iterator<Column> it = columns.keySet().iterator();

            Column column;
            while (it.hasNext()) {
                column = it.next();
                if (!column.autoIncrement()) {
                    Object o = columns.get(column);
                    if (o == null) {
                        continue;
                    }
                    if (sb.length() > 0) {
                        sb.append(" AND ").append(cleanColumnName(column.columnName()));
                    }
                    else {
                        sb.append(cleanColumnName(column.columnName()));
                    }
                    sb.append(" = ?");
                    // if (it.hasNext()) {
                    // sb.append("' = ? AND ");
                    // } else {
                    // sb.append("' = ?");
                    // }
                }
            }
            ps = conn.prepareStatement("SELECT * FROM " + data.getName() + " WHERE " + sb.toString());
            it = columns.keySet().iterator();

            int index = 1;

            while (it.hasNext()) {
                column = it.next();
                if (!column.autoIncrement()) {
                    Object o = columns.get(column);
                    if (o == null) {
                        continue;
                    }
                    setToStatement(index, columns.get(column), ps, column);
//                    ps.setObject(index, this.convert(columns.get(column)));
                    index++;
                }
            }
            rs = ps.executeQuery();
            if (rs != null) {
                toRet = rs.next();
            }
        } catch (SQLException ex) {
            throw new DatabaseWriteException(ex.getMessage() + " Error checking H2 Entry Key in "
                                                     + data.toString()
            );
        } catch (DatabaseTableInconsistencyException ex) {
            LogManager.getLogger().error("", ex);
        } finally {
            close(ps, rs);
        }
        return toRet;
    }

    public ResultSet getResultSet(Connection conn, DataAccess data, Map<String, Object> filters, boolean limitOne) throws DatabaseReadException {
        PreparedStatement ps;
        ResultSet toRet;

        try {

            if (filters.size() > 0) {
                StringBuilder sb = new StringBuilder();
                Object[] fieldNames = filters.keySet().toArray();
                for (int i = 0; i < fieldNames.length && i < fieldNames.length; i++) {
                    sb.append(fieldNames[i]);
                    if (i + 1 < fieldNames.length) {
                        sb.append("=? AND ");
                    }
                    else {
                        sb.append("=?");
                    }
                }
                if (limitOne) {
                    sb.append(" LIMIT 1");
                }
                ps = conn.prepareStatement("SELECT * FROM " + data.getName() + " WHERE " + sb.toString(), ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
                for (int i = 0; i < fieldNames.length && i < fieldNames.length; i++) {
                    String fieldName = String.valueOf(fieldNames[i]);
                    Column col = data.getColumnForName(fieldName);
                    if (col == null) {
                        throw new DatabaseReadException("Error fetching H2 ResultSet in " + data.getName() + ". Column " + fieldNames[i] + " does not exist!");
                    }
                    setToStatement(i + 1, filters.get(fieldName), ps, col);
                }
            }
            else {
                if (limitOne) {
                    ps = conn.prepareStatement("SELECT * FROM " + data.getName() + " LIMIT 1", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
                }
                else {
                    ps = conn.prepareStatement("SELECT * FROM " + data.getName(), ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
                }
            }
            toRet = ps.executeQuery();
        } catch (SQLException ex) {
            throw new DatabaseReadException("Error fetching H2 ResultSet in " + data.getName(), ex);
        } catch (Exception ex) {
            throw new DatabaseReadException("Error fetching H2 ResultSet in " + data.getName(), ex);
        }

        return toRet;
    }

    public List<String> getColumnNames(DataAccess data) {
        Statement statement = null;
        ResultSet resultSet = null;

        ArrayList<String> columns = new ArrayList<String>();
        String columnName;

        try {
            statement = conn.createStatement();
            resultSet = statement.executeQuery("SHOW COLUMNS FROM " + data.getName());
            while (resultSet.next()) {
                columnName = resultSet.getString("field");
                columns.add(columnName);
            }
        } catch (SQLException ex) {
            log.error(ex.getMessage(), ex);
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    log.error(e.getMessage(), e);
                }
            }
            close(null, resultSet);
        }
        return columns;
    }

    private String getDataTypeSyntax(Column.DataType type) {
        switch (type) {
            case BYTE:
                return "INT";
            case INTEGER:
                return "INT";
            case FLOAT:
                return "DOUBLE";
            case DOUBLE:
                return "DOUBLE";
            case LONG:
                return "BIGINT";
            case SHORT:
                return "INT";
            case STRING:
                return "LONGVARCHAR";
            case BOOLEAN:
                return "BOOLEAN";
        }
        return "";
    }

    /**
     * Replaces '*' character with '\\*' if the Object is a String.
     *
     * @param o
     *
     * @return string representation of the given object
     */
    private String convert(Object o) {
        if (o instanceof String && ((String) o).contains("*")) {
            ((String) o).replace("*", "\\*");
        }
        return String.valueOf(o);
    }

    /**
     * Sets the given object as the given type to the given index
     * of the given PreparedStatement.
     *
     * @param index
     *         the index to set to
     * @param o
     *         the object to set
     * @param ps
     *         the prepared statement
     * @param t
     *         the DataType hint
     *
     * @throws DatabaseWriteException
     *         when an SQLException was raised or when the data type doesn't match the objects type
     */
    private void setToStatement(int index, Object o, PreparedStatement ps, Column t) throws DatabaseWriteException {
        try {
            if (t.isList()) {
                ps.setString(index, getString((List<?>) o));
            }
            else {
                switch (t.dataType()) {
                    case BYTE:
                        ps.setByte(index, (Byte) o);
                        break;
                    case INTEGER:
                        ps.setInt(index, (Integer) o);
                        break;
                    case FLOAT:
                        ps.setFloat(index, (Float) o);
                        break;
                    case DOUBLE:
                        ps.setDouble(index, (Double) o);
                        break;
                    case LONG:
                        ps.setLong(index, (Long) o);
                        break;
                    case SHORT:
                        ps.setShort(index, (Short) o);
                        break;
                    case STRING:
                        ps.setString(index, (String) o);
                        break;
                    case BOOLEAN:
                        ps.setBoolean(index, (Boolean) o);
                        break;
                }
            }
        } catch (SQLException e) {
            throw new DatabaseWriteException("Failed to set property to prepared statement!", e);
        } catch (ClassCastException e) {
            throw new DatabaseWriteException("Failed to set property to prepared statement!", e);
        }
    }

    /**
     * Gets a Java List representation from the H2 String.
     *
     * @param type
     * @param field
     *
     * @return
     */
    private List<Comparable<?>> getList(Column.DataType type, String field) {
        List<Comparable<?>> list = new ArrayList<Comparable<?>>();
        if (field == null) {
            return list;
        }
        switch (type) {
            case BYTE:
                for (String s : field.split(this.LIST_REGEX)) {
                    if (s.equals(NULL_STRING)) {
                        list.add(null);
                        continue;
                    }
                    list.add(Byte.valueOf(s));
                }
                break;
            case INTEGER:
                for (String s : field.split(this.LIST_REGEX)) {
                    if (s.equals(NULL_STRING)) {
                        list.add(null);
                        continue;
                    }
                    list.add(Integer.valueOf(s));
                }
                break;
            case FLOAT:
                for (String s : field.split(this.LIST_REGEX)) {
                    if (s.equals(NULL_STRING)) {
                        list.add(null);
                        continue;
                    }
                    list.add(Float.valueOf(s));
                }
                break;
            case DOUBLE:
                for (String s : field.split(this.LIST_REGEX)) {
                    if (s.equals(NULL_STRING)) {
                        list.add(null);
                        continue;
                    }
                    list.add(Double.valueOf(s));
                }
                break;
            case LONG:
                for (String s : field.split(this.LIST_REGEX)) {
                    if (s.equals(NULL_STRING)) {
                        list.add(null);
                        continue;
                    }
                    list.add(Long.valueOf(s));
                }
                break;
            case SHORT:
                for (String s : field.split(this.LIST_REGEX)) {
                    if (s.equals(NULL_STRING)) {
                        list.add(null);
                        continue;
                    }
                    list.add(Short.valueOf(s));
                }
                break;
            case STRING:
                for (String s : field.split(this.LIST_REGEX)) {
                    if (s.equals(NULL_STRING)) {
                        list.add(null);
                        continue;
                    }
                    list.add(s);
                }
                break;
            case BOOLEAN:
                for (String s : field.split(this.LIST_REGEX)) {
                    if (s.equals(NULL_STRING)) {
                        list.add(null);
                        continue;
                    }
                    list.add(Boolean.valueOf(s));
                }
                break;
        }
        return list;
    }

    /**
     * Get the database entry for a Java List.
     *
     * @param list
     *
     * @return a string representation of the passed list.
     */
    public String getString(List<?> list) {
        if (list == null) {
            return NULL_STRING;
        }
        StringBuilder sb = new StringBuilder();
        for (Object o : list) {
            if (o == null) {
                sb.append(NULL_STRING);
            }
            else {
                sb.append(String.valueOf(o));
            }
            sb.append(this.LIST_REGEX);
        }
        if (sb.length() > 0) {
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }

    /**
     * Close a set of working data.
     * This will return all the data to the connection pool.
     * You can pass null for objects that are not relevant in your given context
     *
     * @param ps
     *         the prepared statement
     * @param rs
     *         the result set
     */
    private void close(PreparedStatement ps, ResultSet rs) {
        try {
            if (ps != null) {
                ps.close();
            }
            if (rs != null) {
                rs.close();
            }
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }
    }

    /**
     * H2 doesn't like it when we used reserved words for columns and tables
     */
    private String cleanColumnName(String name) {
        if (name.equalsIgnoreCase("group")) {
            return "primaryGroup";
        }
        return name;
    }
}