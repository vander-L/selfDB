package org.yujiabin.selfDB.index;

import org.junit.Test;
import org.yujiabin.selfDB.data.DataManager;
import org.yujiabin.selfDB.data.page.PageCache;
import org.yujiabin.selfDB.transaction.MockTransactionManager;
import org.yujiabin.selfDB.transaction.TransactionManager;;

import java.io.File;
import java.util.List;

public class BPlusTreeTest {
    @Test
    public void testTreeSingle() throws Exception {
        TransactionManager tm = new MockTransactionManager();
        DataManager dm = DataManager.createFile("/tmp/TestTreeSingle", PageCache.PAGE_SIZE*10, tm);

        long root = BPlusTree.create(dm);
        BPlusTree tree = BPlusTree.load(root, dm);

        int lim = 10000;
        for(int i = lim-1; i >= 0; i --) {
            tree.insert(i, i);
        }

        for(int i = 0; i < lim; i ++) {
            List<Long> uids = tree.search(i);
            assert uids.size() == 1;
            assert uids.get(0) == i;
        }
        tm.close();
        dm.close();
        assert new File("/tmp/TestTreeSingle.db").delete();
        assert new File("/tmp/TestTreeSingle.log").delete();
    }
}
