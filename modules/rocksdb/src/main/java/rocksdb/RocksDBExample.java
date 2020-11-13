package rocksdb;

import org.rocksdb.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class RocksDBExample {

    private static final String dbPath = "./rocksdb-data/";
    private static final String cfdbPath = "./rocksdb-data-cf/";

    static {
        RocksDB.loadLibrary();
    }

    public void testDefaultColumnFamily() {
        System.out.println("testDefaultColumnFamily begin...");
        try (final Options options = new Options().setCreateIfMissing(true)) {
            try (final RocksDB rocksDB = RocksDB.open(options, dbPath)) {
                byte[] key = "Hello".getBytes();
                rocksDB.put(key, "World".getBytes());
                System.out.println(new String(rocksDB.get(key)));
                rocksDB.put("SecondKey".getBytes(), "SecondValue".getBytes());
                List<byte[]> keys = Arrays.asList(key, "SecondKey".getBytes(), "missKey".getBytes());
                List<byte[]> values = rocksDB.multiGetAsList(keys);
                for (int i = 0; i < keys.size(); i++) {
                    System.out.println("multiGet " + new String(keys.get(i)) + ":" + (values.get(i) != null ? new String(values.get(i)) : null));
                }
                RocksIterator iter = rocksDB.newIterator();
                for (iter.seekToFirst(); iter.isValid(); iter.next()) {
                    System.out.println("iterator key:" + new String(iter.key()) + ", iter value:" + new String(iter.value()));
                }
                rocksDB.delete(key);
                System.out.println("after remove key:" + new String(key));
                iter = rocksDB.newIterator();
                for (iter.seekToFirst(); iter.isValid(); iter.next()) {
                    System.out.println("iterator key:" + new String(iter.key()) + ", iter value:" + new String(iter.value()));
                }
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    public void testCertainColumnFamily() {
        System.out.println("\ntestCertainColumnFamily begin...");
        try (final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions().optimizeUniversalStyleCompaction()) {
            String cfName = "my-first-columnfamily";
            final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
                    new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts),
                    new ColumnFamilyDescriptor(cfName.getBytes(), cfOpts)
            );

            List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
            try (final DBOptions dbOptions = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
                 final RocksDB rocksDB = RocksDB.open(dbOptions, cfdbPath, cfDescriptors, cfHandles)) {
                ColumnFamilyHandle cfHandle = cfHandles.stream().filter(x -> {
                    try {
                        return (new String(x.getName())).equals(cfName);
                    } catch (RocksDBException e) {
                        return false;
                    }
                }).collect(Collectors.toList()).get(0);
                String key = "FirstKey";
                rocksDB.put(cfHandle, key.getBytes(), "FirstValue".getBytes());
                byte[] getValue = rocksDB.get(cfHandle, key.getBytes());
                System.out.println("get Value : " + new String(getValue));
                rocksDB.put(cfHandle, "SecondKey".getBytes(), "SecondValue".getBytes());
                List<byte[]> keys = Arrays.asList(key.getBytes(), "SecondKey".getBytes());
                List<ColumnFamilyHandle> cfHandleList = Arrays.asList(cfHandle, cfHandle);
                List<byte[]> values = rocksDB.multiGetAsList(cfHandleList, keys);
                for (int i = 0; i < keys.size(); i++) {
                    System.out.println("multiGet:" + new String(keys.get(i)) + "--" + (values.get(i) == null ? null : new String(values.get(i))));
                }
                rocksDB.delete(cfHandle, key.getBytes());
                RocksIterator iter = rocksDB.newIterator(cfHandle);
                for (iter.seekToFirst(); iter.isValid(); iter.next()) {
                    System.out.println("iterator:" + new String(iter.key()) + ":" + new String(iter.value()));
                }
            } finally {
                for (final ColumnFamilyHandle cfHandle : cfHandles) {
                    cfHandle.close();
                }
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    public void testScan() {
        try (final Options options = new Options().setCreateIfMissing(true)) {
            try (final RocksDB rocksDB = RocksDB.open(options, dbPath)) {
                rocksDB.put("a1".getBytes(), "World".getBytes());
                rocksDB.put("a2".getBytes(), "World".getBytes());
                rocksDB.put("a3".getBytes(), "World".getBytes());
                rocksDB.put("b1".getBytes(), "World".getBytes());
                rocksDB.put("b2".getBytes(), "World".getBytes());
                rocksDB.put("c".getBytes(), "World".getBytes());
                rocksDB.put("d".getBytes(), "World".getBytes());
                try (ReadOptions ro = new ReadOptions()) {
                    ro.setTotalOrderSeek(true);
                    RocksIterator iterator = rocksDB.newIterator(ro);
                    iterator.seek("b".getBytes());
                    while (iterator.isValid()) {
                        System.out.println(new String(iterator.key()));
                        iterator.next();
                    }
                }
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {

        new RocksDBExample().testScan();

       /* RocksDBExample test = new RocksDBExample();
        test.testDefaultColumnFamily();
        test.testCertainColumnFamily();*/
    }

}