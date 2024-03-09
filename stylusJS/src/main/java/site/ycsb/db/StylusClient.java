package site.ycsb.db;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

public class StylusClient extends DB {
    public void init() throws DBException {

    }

    @Override
    public Status read(String table, String key, Set<String> fields,
            Map<String, ByteIterator> result) {

        return Status.ERROR;
    }

    @Override
    public Status insert(String table, String key,
            Map<String, ByteIterator> values) {

        return Status.ERROR;
    }

    @Override
    public Status delete(String table, String key) {
        return Status.OK;
    }

    @Override
    public Status update(String table, String key,
            Map<String, ByteIterator> values) {
        return Status.ERROR;
    }

    @Override
    public Status scan(String table, String startkey, int recordcount,
            Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {

        return Status.OK;
    }

}