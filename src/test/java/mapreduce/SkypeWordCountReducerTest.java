package mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

/**
 * Created by yusuke_harasono on 14/03/05.
 */
public class SkypeWordCountReducerTest {
    /** テスト対象の Mapper */
    Reducer<Text, Text, Text, Text> sut;

    /** テスト用ドライバ */
    ReduceDriver<Text, Text, Text, Text> driver;

    @Before
    public void setUp() throws Exception {
        // テスト対象インスタンス生成
        sut = new SkypeWordCountReducer();
        driver = new ReduceDriver<Text, Text, Text, Text>(sut);

        // Configuration 設定がある場合はここで行う
        // Configuration conf = driver.getConfiguration();
        // conf.set("name", "value");
    }

    @Test
    public void 単語が頻度順にソートされること() throws Exception {
        // setup
        // Reducer への入力のキー、値の指定
        driver.withInput(
                new Text("太郎"),
                Arrays.asList(
                        new Text("きのこ"),
                        new Text("たけのこ"),
                        new Text("きのこ"),
                        new Text("きのこ"),
                        new Text("たけのこ"),
                        new Text("パイ"),
                        new Text("たけのこ"),
                        new Text("きりかぶ"),
                        new Text("パイ"),
                        new Text("きのこ")));

        // Reducer の出力として期待されるキー、値
        driver.withOutput(
                new Text("太郎"),
                new Text("[きのこ, たけのこ, パイ, きりかぶ]"));

        // exercise & verify
        // 一連の Mapper 処理（setup - map - cleanup）を実行し、期待される結果になっているかをテストする
        driver.runTest();
    }
}
