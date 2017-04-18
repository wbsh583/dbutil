package com.ican.dbutil.db;

import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.util.Log;

import com.j256.ormlite.android.AndroidDatabaseConnection;
import com.j256.ormlite.android.apptools.OrmLiteSqliteOpenHelper;
import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.stmt.QueryBuilder;
import com.j256.ormlite.stmt.UpdateBuilder;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 数据库基础类
 * @author wbs
 * @time 17/2/20 15:32
 */
public abstract class BaseDb extends OrmLiteSqliteOpenHelper {
    private final String TAG = this.getClass().getSimpleName();
    private final Map<String, Dao> mDaoMap = new HashMap<String, Dao>();

    public BaseDb(Context context, String dbName, int dbVersion) {
        super(context, dbName, null, dbVersion);
    }


    public Dao getDao(Class _cla) throws SQLException {
        Dao dao = null;
        String className = _cla.getSimpleName();
        synchronized (mDaoMap) {
            if (mDaoMap.containsKey(className)) {
                dao = mDaoMap.get(className);
            }
            if (dao == null) {
                dao = super.getDao(_cla);
                mDaoMap.put(className, dao);
            }
        }
        return dao;
    }

    @Override
    public void close() {
        super.close();

        synchronized (mDaoMap) {
            for (String key : mDaoMap.keySet()) {
                Dao dao = mDaoMap.get(key);
                dao = null;//TODO:release
            }
        }
    }


    /**
     * 根据对象统计,返回行数
     *
     * @param c
     * @return
     */
    public <T> int count(Class<T> c) {
        try {
            Dao<T, Long> dao = this.getDao(c);
            return (int) dao.countOf();
        } catch (SQLException e) {
            Log.e(TAG, "count", e);
        }
        return 0;
    }

    /**
     * 批量插入数据
     * @param list
     * @param c
     */
    public <T> void createBatch(List list, T c) {
        AndroidDatabaseConnection connection = null;
        String pointName = c.toString();
        Savepoint savepoint = null;
        SQLiteDatabase dbc = null;
        try {
            Dao<T, Long> dao = this.getDao((Class<T>) c.getClass());
            dao.clearObjectCache();
            // beging transaction
            dbc = this.getWritableDatabase();
            connection = new AndroidDatabaseConnection(dbc, true);
            connection.setAutoCommit(false);
            dao.setAutoCommit(connection, false);
            savepoint = connection.setSavePoint(pointName);
            if (list != null && list.size() > 0) {
                for (int i = 0; i < list.size(); i++) {
                    c = (T) list.get(i);
                    dao.createOrUpdate(c);
                }
                dao.commit(connection);
                connection.commit(savepoint);
            }

        } catch (Exception e) {
            e.printStackTrace();
            try {
                if (connection != null)
                    connection.rollback(savepoint);
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        }

    }


//    public <T> void createBath2(List list, T c) {
//        try {
//            SQLiteDatabase sd = this.getWritableDatabase();
//            Dao<T, Long> dao = this.getDao((Class<T>) c.getClass());
//            sd.beginTransaction();
//            dao.setAutoCommit(false);
//            for (int i = 0; i < list.size(); i++) {
//                c = (T) list.get(i);
//                dao.createOrUpdate(c);
//            }
//            dao.commit(null);
//            sd.setTransactionSuccessful();
//            sd.endTransaction();
//        } catch (SQLException e) {
//            Log.e(TAG, "create", e);
//        }
//    }

    /**
     * 插入数据
     *
     * @param po
     * @return
     */
    public <T> int create(T po) {
        try {
            Dao<T, Long> dao = this.getDao((Class<T>) po.getClass());
            return dao.create(po);
        } catch (SQLException e) {
            Log.e(TAG, "create", e);
        }
        return -1;
    }

    /**
     * 插入数据，如果该数据已存在则更新数据
     *
     * @param po
     * @return
     */
    public <T> int createOrUpdate(T po) {
        try {
            Dao dao = this.getDao(po.getClass());
            return dao.createOrUpdate(po).getNumLinesChanged();
        } catch (SQLException e) {
            Log.e(TAG, "createOrUpdate", e);
        }
        return -1;
    }

    /**
     * 删除数据
     *
     * @param po
     * @return
     */
    public <T> int remove(T po) {
        try {
            Dao<T, Long> dao = this.getDao((Class<T>) po.getClass());
            return dao.delete(po);
        } catch (SQLException e) {
            Log.e(TAG, "remove", e);
        }
        return -1;
    }

    /**
     * 批量删除数据
     *
     * @param po
     * @return
     */
    public <T> int remove(Collection<T> po) {
        try {
            Dao dao = this.getDao(po.getClass());
            return dao.delete(po);
        } catch (SQLException e) {
            Log.e(TAG, "remove", e);
        }
        return -1;
    }

    /**
     * 根据特定条件更新特定字段
     *
     * @param c
     * @param values
     * @param columnName where字段
     * @param value      where�? *
     * @return
     */
    public <T> int update(Class<T> c, HashMap<String, Object> values,
                          String columnName, Object value) {
        try {
            Dao<T, Long> dao = this.getDao(c);
            UpdateBuilder<T, Long> updateBuilder = dao.updateBuilder();
            updateBuilder.where().eq(columnName, value);
            for (String key : values.keySet()) {
                updateBuilder.updateColumnValue(key, values.get(key));
            }
            return updateBuilder.update();
        } catch (SQLException e) {
            Log.e(TAG, "update", e);
        }
        return -1;
    }

    /**
     * 根据特定条件更新特定字段1
     *
     * @param c
     * @param values
     * @param columnName where字段
     * @param value      where�? *
     * @return
     */
    public <T> int update(Class<T> c, HashMap<String, Object> values,
                          String columnName, Object value,String columnName1,Object value1) {
        try {
            Dao<T, Long> dao = this.getDao(c);
            UpdateBuilder<T, Long> updateBuilder = dao.updateBuilder();
            updateBuilder.where().eq(columnName, value).and().eq(columnName1,value1);
            for (String key : values.keySet()) {
                updateBuilder.updateColumnValue(key, values.get(key));
            }
            return updateBuilder.update();
        } catch (SQLException e) {
            Log.e(TAG, "update", e);
        }
        return -1;
    }

    /**
     * 更新表数据
     *
     * @param po
     * @return
     */
    public <T> int update(T po) {
        try {
            Dao<T, Long> dao = this.getDao((Class<T>) po.getClass());
            return dao.update(po);
        } catch (SQLException e) {
            Log.e(TAG, "update", e);
        }
        return -1;
    }

    public <T> List<T> queryForAll(Class<T> c) {
        try {
            Dao<T, Long> dao = this.getDao(c);
            return dao.queryForAll();
        } catch (SQLException e) {
            Log.e(TAG, "queryForAll", e);
        }
        return new ArrayList<T>();
    }

    public <T> List<T> queryForAllOrderby(Class<T> c, String orderFieldName) {
        return queryForAllOrderby(c, orderFieldName, false);
    }

    public <T> List<T> queryForAllOrderby(Class<T> c, String orderFieldName,
                                          boolean asc) {
        try {
            Dao<T, Long> dao = this.getDao(c);
            QueryBuilder<T, Long> query = dao.queryBuilder();
            query.orderBy(orderFieldName, asc);
            return dao.query(query.prepare());
        } catch (SQLException e) {
            Log.e(TAG, "queryForAllOrderby", e);
        }
        return new ArrayList<T>();
    }

    public <T> List<T> queryForAllOrderby(Class<T> c, String fieldName,
                                          Object value, String orderFieldName, boolean asc) {
        try {
            Dao<T, Long> dao = this.getDao(c);
            QueryBuilder<T, Long> query = dao.queryBuilder();
            query.orderBy(orderFieldName, asc);
            query.where().eq(fieldName, value);
            return dao.query(query.prepare());
        } catch (SQLException e) {
            Log.e(TAG, "queryForAllOrderby", e);
        }
        return new ArrayList<T>();
    }

    public <T> List<T> queryForAll(Class<T> c, String[] fieldNames, Object[] values) {
        try {
            Dao<T, Long> dao = this.getDao(c);
            QueryBuilder<T, Long> query = dao.queryBuilder();
            for (int i = 0; i < values.length; i++) {
                Object val = values[i];
                String filedName = fieldNames[i];
                if (filedName != null && val != null) {
                    query.where().eq(filedName, val);
                }
            }
            return dao.query(query.prepare());
        } catch (SQLException e) {
            Log.e(TAG, "queryForAll", e);
        }
        return new ArrayList<T>();
    }

    public <T> List<T> queryForAll(Class<T> c, Map<String, Object> map) {
        try {
            Dao<T, Long> dao = this.getDao(c);
            return dao.queryForFieldValues(map);
//            QueryBuilder<T, Long> query = dao.queryBuilder();
//            for (Map.Entry<String, Object> entry : map.entrySet()) {
//                String filedName = entry.getKey();
//                Object val = entry.getValue();
//                if (filedName != null && val != null) {
//                    query.where().eq(filedName, val);
//                }
//            }
//            return dao.query(query.prepare());
        } catch (SQLException e) {
            Log.e(TAG, "queryForAll", e);
        }
        return new ArrayList<T>();
    }

    public <T> List<T> queryForAll(Class<T> c, String fieldName, Object value) {
        try {
            Dao<T, Long> dao = this.getDao(c);
            return dao.queryForEq(fieldName, value);
        } catch (SQLException e) {
            Log.e(TAG, "queryForAll", e);
        }
        return new ArrayList<T>();
    }

    /**
     * @param c
     * @param fieldName
     * @param value
     * @return
     */
    public <T> T query(Class<T> c, String fieldName, Object value) {
        try {
            Dao<T, Long> dao = this.getDao(c);
            List<T> result = dao.queryForEq(fieldName, value);
            if (result != null && result.size() > 0)
                return result.get(0);
        } catch (SQLException e) {
            Log.e(TAG, "query", e);
        }
        return null;
    }

    public <T> T query(Class<T> c, long id) {
        try {
            Dao<T, Long> dao = this.getDao(c);
            return dao.queryForId(id);
        } catch (SQLException e) {
            Log.e(TAG, "query", e);
        }
        return null;
    }

    public boolean isExsits(String tablename) {
        int count = 0;
        Cursor c = null;
        try {
            SQLiteDatabase sd = this.getWritableDatabase();
            String sql = "select count(*) as isExsit from sqlite_master where type=\"table\" and name= \""
                    + tablename + "\"";
            c = sd.rawQuery(sql, null);
            while (c.moveToNext()) {
                count = c.getInt(0);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (c != null) {
                c.close();
            }
        }

        if (count > 0) {
            return true;
        } else {
            return false;
        }
    }


//    public String getListstructure(String tablename) {
//        String str = "";
//        Cursor c = null;
//        try {
//            SQLiteDatabase sd = this.getWritableDatabase();
//            String sql = "select  sql as isExsit from sqlite_master where type=\"table\" and name= \""
//                    + tablename + "\"";
//            c = sd.rawQuery(sql, null);
//            while (c.moveToNext()) {
//                str = c.getString(0);
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            if (c != null) {
//                c.close();
//            }
//        }
//        return str;
//    }
//
//    public boolean haveLocalmodify() {
//        int count = 0;
//        boolean b = false;
//        Cursor c = null;
//        try {
//            SQLiteDatabase sd = this.getWritableDatabase();
//            String sql = "select  count(*) from sqlite_master where type=\"table\" and name in(SELECT TableName FROM SyncTable) AND sql not like '%localmodify%'";
//            c = sd.rawQuery(sql, null);
//            while (c.moveToNext()) {
//                count = c.getInt(0);
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            if (c != null) {
//                c.close();
//            }
//        }
//
//        if (count == 0) {
//            b = true;
//        }
//        return b;
//    }

    public int queryCount(String sql) {
        int count = 0;
        Cursor c = null;
        try {
            SQLiteDatabase sd = this.getReadableDatabase();
            c = sd.rawQuery(sql, null);
            c.moveToNext();
            count = c.getInt(0);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (c != null) {
                c.close();
            }
        }
        return count;
    }

    public List queryObjcet(Class cl, String sql) {
        List list = new ArrayList();
        Cursor c = null;
        try {
            SQLiteDatabase sd = this.getReadableDatabase();
            c = sd.rawQuery(sql, null);
            while (c.moveToNext()) {
                if (cl != null) {
                    Object vo = (Object) Class.forName(cl.getName())
                            .newInstance();
                    Field[] fields = cl.getDeclaredFields();
                    for (int j = 0; j < fields.length; j++) {
                        String filedname = fields[j].getName();
                        Field field = vo.getClass().getDeclaredField(filedname);
                        fields[j].setAccessible(true);
                        field.setAccessible(true);
                        int findex = c.getColumnIndex(filedname);
                        if (findex > -1 && c.getString(findex) != null) {

                            String type = field.getType().getSimpleName()
                                    .toString();
                            if (type.indexOf("String") > -1) {
                                field.set(vo, c.getString(findex));
                            } else if (type.equals("int")) {
                                field.set(vo, c.getInt(findex));
                            } else if (type.equals("long")) {
                                field.set(vo, c.getLong(findex));
                            } else if (type.equals("double")) {
                                field.set(vo, c.getDouble(findex));
                            }
                        }

                    }
                    list.add(vo);
                } else {
                    int cint = c.getColumnCount();
                    String[] str = c.getColumnNames();
                    Object[] reobj = new Object[cint];
                    for (int i = 0; i < cint; i++) {
                        String value = c.getString(i);
                        reobj[i] = value;
                    }
                    list.add(reobj);
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (c != null) {
                c.close();
            }
        }
        return list;
    }



    public Object getObjcet(Class clas, String sql) {
        Cursor c = null;
        try {
            SQLiteDatabase sd = this.getReadableDatabase();
            c = sd.rawQuery(sql, null);
            c.moveToNext();
            int count = c.getCount();
            if (clas != null) {
                if (count > 0) {
                    Object vo = (Object) Class.forName(clas.getName())
                            .newInstance();
                    Field[] fields = clas.getDeclaredFields();
                    for (int j = 0; j < fields.length; j++) {
                        String filedname = fields[j].getName();
                        Field field = vo.getClass().getDeclaredField(filedname);
                        fields[j].setAccessible(true);
                        field.setAccessible(true);
                        int findex = c.getColumnIndex(filedname);

                        if (findex != -1 && c.moveToFirst()
                                && c.getString(findex) != null) {

                            String type = field.getType().getSimpleName()
                                    .toString();
                            if (type.indexOf("String") > -1) {
                                field.set(vo, c.getString(findex));
                            } else if (type.equals("int")) {
                                field.set(vo, c.getInt(findex));
                            } else if (type.equals("long")) {
                                field.set(vo, c.getLong(findex));
                            } else if (type.equals("double")) {
                                field.set(vo, c.getDouble(findex));
                            }
                        }

                    }
                    return vo;
                } else {
                    return null;
                }
            } else {
                int cint = c.getColumnCount();
                String[] str = c.getColumnNames();
                Object[] reobj = new Object[cint];
                for (int i = 0; i < cint; i++) {
                    String value = c.getString(i);
                    reobj[i] = value;
                }
                return reobj;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (c != null) {
                c.close();
            }
        }
        return null;
    }

    public String queryMax(String sql) {
        String max = "";
        Cursor c = null;
        try {
            SQLiteDatabase sd = this.getReadableDatabase();
            c = sd.rawQuery(sql, null);
            c.moveToNext();
            max = c.getString(0);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (c != null) {
                c.close();
            }
        }
        return max;
    }

    public void delete(String sql) {
        execSQL(sql);
    }

    public void execSQL(String sql) {
        try {
            SQLiteDatabase sd = this.getWritableDatabase();
            sd.execSQL(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
