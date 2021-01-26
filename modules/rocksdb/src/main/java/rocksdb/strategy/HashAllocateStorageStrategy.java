package rocksdb.strategy;

import rocksdb.BackendStorageModuleImpl;

import java.util.List;

public class HashAllocateStorageStrategy implements AllocateStorageStrategy {

    @Override
    public String allocatePath(BackendStorageModuleImpl module, String name) {
        int hash = name.hashCode();
        List<String> dataDirs = module.getDataDirs();
        return dataDirs.get(hash % dataDirs.size());
    }
}
