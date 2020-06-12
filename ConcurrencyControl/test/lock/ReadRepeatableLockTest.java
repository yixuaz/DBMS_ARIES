package lock;

import dbengine.transaction.model.IsolationLevelType;
import org.junit.Test;

import java.util.Arrays;

public class ReadRepeatableLockTest extends BaseTest {
    @Override
    public IsolationLevelType getIsolationLevel() {
        return IsolationLevelType.RR;
    }

    // test primary search, the locks added
    @Test
    public void testPrimaryKeyEqualSearch() throws Exception {
        testTemplate("select * from t where id = 2 lock in share mode",
                Arrays.asList("1:id=2, name='bbb', num=200", "SHARE_LOCK,id=2, name='bbb', num=200"));
    }

    @Test
    public void testPrimaryKeyLargerSearch() throws Exception{
        testTemplate("select * from t where id > 2 for update",
                Arrays.asList("1:id=3, name='bbb', num=300",
                        "1:id=7, name='ccc', num=200",
                        "GAP_LOCK(2,3),id=3, name='bbb', num=300",
                        "EXCLUSIVE_LOCK,id=3, name='bbb', num=300",
                        "GAP_LOCK(3,7),id=7, name='ccc', num=200",
                        "EXCLUSIVE_LOCK,id=7, name='ccc', num=200",
                        "GAP_LOCK(7,inf),id=0, name='null', num=0"));
    }

    @Test
    public void testPrimaryKeyLargerEqualSearch() throws Exception{
        testTemplate("select * from t where id >= 2 for update",
                Arrays.asList(
                        "1:id=2, name='bbb', num=200",
                        "1:id=3, name='bbb', num=300",
                        "1:id=7, name='ccc', num=200",
                        "EXCLUSIVE_LOCK,id=2, name='bbb', num=200",
                        "GAP_LOCK(2,3),id=3, name='bbb', num=300",
                        "EXCLUSIVE_LOCK,id=3, name='bbb', num=300",
                        "GAP_LOCK(3,7),id=7, name='ccc', num=200",
                        "EXCLUSIVE_LOCK,id=7, name='ccc', num=200",
                        "GAP_LOCK(7,inf),id=0, name='null', num=0"));

    }

    @Test
    public void testPrimaryKeyLessEqualSearch() throws Exception{
        testTemplate("select * from t where id <= 3 for update",
                Arrays.asList(
                        "1:id=1, name='aaa', num=100",
                        "1:id=2, name='bbb', num=200",
                        "1:id=3, name='bbb', num=300",
                        "GAP_LOCK(-inf,1),id=1, name='aaa', num=100",
                        "EXCLUSIVE_LOCK,id=1, name='aaa', num=100",
                        "GAP_LOCK(1,2),id=2, name='bbb', num=200",
                        "EXCLUSIVE_LOCK,id=2, name='bbb', num=200",
                        "GAP_LOCK(2,3),id=3, name='bbb', num=300",
                        "EXCLUSIVE_LOCK,id=3, name='bbb', num=300",
                        "GAP_LOCK(3,7),id=7, name='ccc', num=200"));
    }

    @Test
    public void testPrimaryKeyLessSearch() throws Exception{
        testTemplate("select * from t where id < 3 lock in share mode",
                Arrays.asList(
                        "1:id=1, name='aaa', num=100",
                        "1:id=2, name='bbb', num=200",
                        "GAP_LOCK(-inf,1),id=1, name='aaa', num=100",
                        "SHARE_LOCK,id=1, name='aaa', num=100",
                        "GAP_LOCK(1,2),id=2, name='bbb', num=200",
                        "SHARE_LOCK,id=2, name='bbb', num=200",
                        "GAP_LOCK(2,3),id=3, name='bbb', num=300"));
    }

    @Test
    public void testPrimaryKeyEqualNotMatchSearch() throws Exception{
        testTemplate("select * from t where id = 6 for update",
                Arrays.asList(
                        "1:no result found",
                        "GAP_LOCK(3,7),id=7, name='ccc', num=200"
                ));
    }

    @Test
    public void testPrimaryKeyLessNotMatchSearch() throws Exception{
        testTemplate("select * from t where id < 1 for update",
                Arrays.asList(
                        "1:no result found",
                        "GAP_LOCK(-inf,1),id=1, name='aaa', num=100"
                ));
    }

    @Test
    public void testPrimaryKeyLargeNotMatchSearch() throws Exception{
        testTemplate("select * from t where id > 6 for update",
                Arrays.asList(
                        "1:id=7, name='ccc', num=200",
                        "GAP_LOCK(3,7),id=7, name='ccc', num=200",
                        "EXCLUSIVE_LOCK,id=7, name='ccc', num=200",
                        "GAP_LOCK(7,inf),id=0, name='null', num=0"
                ));
    }

    @Test
    public void testPrimaryKeyLargeEqualNotMatchSearch() throws Exception{
        testTemplate("select * from t where id >= 18 for update",
                Arrays.asList(
                        "1:no result found",
                        "GAP_LOCK(7,inf),id=0, name='null', num=0"
                ));
    }

    @Test
    public void testPrimaryKeyLessEqualNotMatchSearch() throws Exception{
        testTemplate("select * from t where id <= -1 for update",
                Arrays.asList(
                        "1:no result found",
                        "GAP_LOCK(-inf,1),id=1, name='aaa', num=100"
                ));
    }

    @Test
    public void testPrimaryKeyMultiplePredicateSearch() throws Exception{
        testTemplate("select * from t where id <= 4 and id > 1 for update",
                Arrays.asList(
                        "1:id=2, name='bbb', num=200",
                        "1:id=3, name='bbb', num=300",
                        "GAP_LOCK(1,2),id=2, name='bbb', num=200",
                        "EXCLUSIVE_LOCK,id=2, name='bbb', num=200",
                        "GAP_LOCK(2,3),id=3, name='bbb', num=300",
                        "EXCLUSIVE_LOCK,id=3, name='bbb', num=300",
                        "GAP_LOCK(3,7),id=7, name='ccc', num=200"
                        ));
    }

    @Test
    public void testPrimaryKeyMultiplePredicateSearch2() throws Exception{
        testTemplate("select * from t where id > 1 and id < 2 for update",
                Arrays.asList(
                        "1:no result found",
                        "GAP_LOCK(1,2),id=2, name='bbb', num=200"
                ));
    }

    @Test
    public void testPrimaryKeyMultiplePredicateSearch3() throws Exception{
        testTemplate("select * from t where id = 2 and id < 1 for update",
                Arrays.asList(
                        "1:no result found"
                ));
    }

    // test secondary index search, the locks added
    @Test
    public void testSecKeyEqualSearch() throws Exception {
        testTemplate("select * from t where name = 'bbb' lock in share mode",
                Arrays.asList("1:id=2, name='bbb', num=200",
                        "1:id=3, name='bbb', num=300",
                        "GAP_LOCK(aaa:1,bbb:2),[SEC_IDX]name='bbb', primaryId=2",
                        "SHARE_LOCK,[SEC_IDX]name='bbb', primaryId=2",
                        "SHARE_LOCK,id=2, name='bbb', num=200",
                        "GAP_LOCK(bbb:2,bbb:3),[SEC_IDX]name='bbb', primaryId=3",
                        "SHARE_LOCK,[SEC_IDX]name='bbb', primaryId=3",
                        "SHARE_LOCK,id=3, name='bbb', num=300",
                        "GAP_LOCK(bbb:3,ccc:7),[SEC_IDX]name='ccc', primaryId=7"
                ));
    }

    @Test
    public void testSecKeyLargerSearch() throws Exception{
        testTemplate("select * from t where name > 'bbb' for update",
                Arrays.asList(
                        "1:id=7, name='ccc', num=200",
                        "GAP_LOCK(bbb:3,ccc:7),[SEC_IDX]name='ccc', primaryId=7",
                        "EXCLUSIVE_LOCK,[SEC_IDX]name='ccc', primaryId=7",
                        "EXCLUSIVE_LOCK,id=7, name='ccc', num=200",
                        "GAP_LOCK(ccc:7,inf),[SEC_IDX]name='null', primaryId=0"));
    }

    @Test
    public void testSecKeyLargerEqualSearch() throws Exception{
        testTemplate("select * from t where name >= 'bbb' for update",
                Arrays.asList(
                        "1:id=2, name='bbb', num=200",
                        "1:id=3, name='bbb', num=300",
                        "1:id=7, name='ccc', num=200",
                        "GAP_LOCK(aaa:1,bbb:2),[SEC_IDX]name='bbb', primaryId=2",
                        "EXCLUSIVE_LOCK,[SEC_IDX]name='bbb', primaryId=2",
                        "EXCLUSIVE_LOCK,id=2, name='bbb', num=200",
                        "GAP_LOCK(bbb:2,bbb:3),[SEC_IDX]name='bbb', primaryId=3",
                        "EXCLUSIVE_LOCK,[SEC_IDX]name='bbb', primaryId=3",
                        "EXCLUSIVE_LOCK,id=3, name='bbb', num=300",
                        "GAP_LOCK(bbb:3,ccc:7),[SEC_IDX]name='ccc', primaryId=7",
                        "EXCLUSIVE_LOCK,[SEC_IDX]name='ccc', primaryId=7",
                        "EXCLUSIVE_LOCK,id=7, name='ccc', num=200",
                        "GAP_LOCK(ccc:7,inf),[SEC_IDX]name='null', primaryId=0"
                ));

    }

    @Test
    public void testSecKeyLessEqualSearch() throws Exception{
        testTemplate("select * from t where name <= 'bbb' for update",
                Arrays.asList(
                        "1:id=1, name='aaa', num=100",
                        "1:id=2, name='bbb', num=200",
                        "1:id=3, name='bbb', num=300",
                        "GAP_LOCK(-inf,aaa:1),[SEC_IDX]name='aaa', primaryId=1",
                        "EXCLUSIVE_LOCK,[SEC_IDX]name='aaa', primaryId=1",
                        "EXCLUSIVE_LOCK,id=1, name='aaa', num=100",
                        "GAP_LOCK(aaa:1,bbb:2),[SEC_IDX]name='bbb', primaryId=2",
                        "EXCLUSIVE_LOCK,[SEC_IDX]name='bbb', primaryId=2",
                        "EXCLUSIVE_LOCK,id=2, name='bbb', num=200",
                        "GAP_LOCK(bbb:2,bbb:3),[SEC_IDX]name='bbb', primaryId=3",
                        "EXCLUSIVE_LOCK,[SEC_IDX]name='bbb', primaryId=3",
                        "EXCLUSIVE_LOCK,id=3, name='bbb', num=300",
                        "GAP_LOCK(bbb:3,ccc:7),[SEC_IDX]name='ccc', primaryId=7"
                        ));
    }

    @Test
    public void testSecKeyLessSearch() throws Exception{
        testTemplate("select * from t where name < 'bbb' for update",
                Arrays.asList(
                        "1:id=1, name='aaa', num=100",
                        "GAP_LOCK(-inf,aaa:1),[SEC_IDX]name='aaa', primaryId=1",
                        "EXCLUSIVE_LOCK,[SEC_IDX]name='aaa', primaryId=1",
                        "EXCLUSIVE_LOCK,id=1, name='aaa', num=100",
                        "GAP_LOCK(aaa:1,bbb:2),[SEC_IDX]name='bbb', primaryId=2"
                ));
    }

    @Test
    public void testSecKeyEqualNotMatchSearch() throws Exception{
        testTemplate("select * from t where name = 'bbc' for update",
                Arrays.asList(
                        "1:no result found",
                        "GAP_LOCK(bbb:3,ccc:7),[SEC_IDX]name='ccc', primaryId=7"
                ));
    }

    @Test
    public void testSecKeyLessNotMatchSearch() throws Exception{
        testTemplate("select * from t where name < 'aa' for update",
                Arrays.asList(
                        "1:no result found",
                        "GAP_LOCK(-inf,aaa:1),[SEC_IDX]name='aaa', primaryId=1"
                ));
    }

    @Test
    public void testSecKeyLargeNotMatchSearch() throws Exception{
        testTemplate("select * from t where name > 'ccc' for update",
                Arrays.asList(
                        "1:no result found",
                        "GAP_LOCK(ccc:7,inf),[SEC_IDX]name='null', primaryId=0"
                ));
    }

    @Test
    public void testSecKeyLargeEqualNotMatchSearch() throws Exception{
        testTemplate("select * from t where name >= 'bbc' for update",
                Arrays.asList(
                        "1:id=7, name='ccc', num=200",
                        "GAP_LOCK(bbb:3,ccc:7),[SEC_IDX]name='ccc', primaryId=7",
                        "EXCLUSIVE_LOCK,[SEC_IDX]name='ccc', primaryId=7",
                        "EXCLUSIVE_LOCK,id=7, name='ccc', num=200",
                        "GAP_LOCK(ccc:7,inf),[SEC_IDX]name='null', primaryId=0"
                ));
    }

    @Test
    public void testSecKeyLessEqualNotMatchSearch() throws Exception{
        testTemplate("select * from t where name <= 'aac' for update",
                Arrays.asList(
                        "1:id=1, name='aaa', num=100",
                        "GAP_LOCK(-inf,aaa:1),[SEC_IDX]name='aaa', primaryId=1",
                        "EXCLUSIVE_LOCK,[SEC_IDX]name='aaa', primaryId=1",
                        "EXCLUSIVE_LOCK,id=1, name='aaa', num=100",
                        "GAP_LOCK(aaa:1,bbb:2),[SEC_IDX]name='bbb', primaryId=2"
                ));
    }

    @Test
    public void testSecKeyMultiplePredicateSearch() throws Exception{
        testTemplate("select * from t where name > 'aaa' and name <= 'aac' for update",
                Arrays.asList(
                        "1:no result found",
                        "GAP_LOCK(aaa:1,bbb:2),[SEC_IDX]name='bbb', primaryId=2"
                ));
    }

    @Test
    public void testSecKeyMultiplePredicateSearch2() throws Exception{
        testTemplate("select * from t where name >= 'aaa' and name <= 'aac' for update",
                Arrays.asList(
                        "1:id=1, name='aaa', num=100",
                        "GAP_LOCK(-inf,aaa:1),[SEC_IDX]name='aaa', primaryId=1",
                        "EXCLUSIVE_LOCK,[SEC_IDX]name='aaa', primaryId=1",
                        "EXCLUSIVE_LOCK,id=1, name='aaa', num=100",
                        "GAP_LOCK(aaa:1,bbb:2),[SEC_IDX]name='bbb', primaryId=2"
                ));
    }

    @Test
    public void testNonIndexFindSearch() throws Exception{
        testTemplate("select * from t where num > 200 for update",
                Arrays.asList(
                        "1:id=3, name='bbb', num=300",
                        "GAP_LOCK(-inf,1),id=1, name='aaa', num=100",
                        "EXCLUSIVE_LOCK,id=1, name='aaa', num=100",
                        "GAP_LOCK(1,2),id=2, name='bbb', num=200",
                        "EXCLUSIVE_LOCK,id=2, name='bbb', num=200",
                        "GAP_LOCK(2,3),id=3, name='bbb', num=300",
                        "EXCLUSIVE_LOCK,id=3, name='bbb', num=300",
                        "GAP_LOCK(3,7),id=7, name='ccc', num=200",
                        "EXCLUSIVE_LOCK,id=7, name='ccc', num=200",
                        "GAP_LOCK(7,inf),id=0, name='null', num=0"
                ));
    }

    @Test
    public void testNonIndexNotFindSearch() throws Exception{
        testTemplate("select * from t where num = 250 lock in share mode",
                Arrays.asList(
                        "1:no result found",
                        "GAP_LOCK(-inf,1),id=1, name='aaa', num=100",
                        "SHARE_LOCK,id=1, name='aaa', num=100",
                        "GAP_LOCK(1,2),id=2, name='bbb', num=200",
                        "SHARE_LOCK,id=2, name='bbb', num=200",
                        "GAP_LOCK(2,3),id=3, name='bbb', num=300",
                        "SHARE_LOCK,id=3, name='bbb', num=300",
                        "GAP_LOCK(3,7),id=7, name='ccc', num=200",
                        "SHARE_LOCK,id=7, name='ccc', num=200",
                        "GAP_LOCK(7,inf),id=0, name='null', num=0"
                ));
    }

    @Test
    public void testNonIndexSearchWithoutLock() throws Exception{
        testTemplate("select * from t where num = 200",
                Arrays.asList(
                        "1:id=2, name='bbb', num=200",
                        "1:id=7, name='ccc', num=200"
                ));
    }

    @Test
    public void testInsertLock() throws Exception{
        testTemplate("insert t values(6,'6',6)",
                Arrays.asList(
                        "1:1 rows affected",
                        "INSERT_GAP_LOCK(3,7),id=7, name='ccc', num=200",
                        "INSERT_GAP_LOCK(-inf,aaa:1),[SEC_IDX]name='aaa', primaryId=1",
                        "EXCLUSIVE_LOCK,id=6, name='6', num=6"
                ));
    }
}
