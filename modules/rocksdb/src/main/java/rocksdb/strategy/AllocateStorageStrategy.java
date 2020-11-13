package rocksdb.strategy;

import rocksdb.BackendStorageModuleImpl;

public interface AllocateStorageStrategy {
    String allocatePath(BackendStorageModuleImpl module, String name);
}
