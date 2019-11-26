import org.junit.Test;

/**
 * Created by MTL on 2019/11/26
 */
public class MyTest {
    @Test
    public void myTest(){
        String str = "1.1.2";
        System.out.println(str.replaceAll("\\d+(\\.\\d+)*", "sdfsdfds"));
        System.out.println(str);
    }
}
