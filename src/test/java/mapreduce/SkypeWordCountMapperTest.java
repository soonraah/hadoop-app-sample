package mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;


public class SkypeWordCountMapperTest {
    /** テスト対象の Mapper */
    Mapper<LongWritable, Text, Text, Text> sut;

    /** テスト用ドライバ */
    MapDriver<LongWritable, Text,Text, Text> driver;

    @Before
    public void setUp() throws Exception {
        // テスト対象インスタンス生成
        sut = new SkypeWordCountMapper();
        driver = new MapDriver<LongWritable, Text, Text, Text>(sut);

        // Configuration 設定がある場合はここで行う
        // Configuration conf = driver.getConfiguration();
        // conf.set("name", "value");
    }

    @Test
    public void 本文を形態素解析して名詞が取得できていること() throws Exception {
        // setup
        // Mapper への入力のキー、値の指定
        driver.withInput(
                new LongWritable(1),
                new Text("[2014/02/28 17:18:46] 花子: 車のエンジンがかからないの…"));

        // Mapper の出力として期待されるキー、値
        driver.withOutput(
                new Text("花子"),
                new Text("車"));
        driver.withOutput(
                new Text("花子"),
                new Text("エンジン"));

        // exercise & verify
        // 一連の Mapper 処理（setup - map - cleanup）を実行し、期待される結果になっているかをテストする
        driver.runTest();
    }

    @Test
    public void ユーザ別に解析結果が吐き出されること() throws Exception {
        // setup
        // Mapper への入力のキー、値の指定
        driver.withInput(
                new LongWritable(1),
                new Text("[2014/02/28 17:19:38] 太郎: バッテリーかな？ライトは点く？"));
        driver.withInput(
                new LongWritable(2),
                new Text("[2014/02/28 17:23:07] 花子: 昨日まではちゃんと動いてたのに。"));

        // Mapper の出力として期待されるキー、値
        driver.withOutput(
                new Text("太郎"),
                new Text("バッテリー"));
        driver.withOutput(
                new Text("太郎"),
                new Text("ライト"));
        driver.withOutput(
                new Text("花子"),
                new Text("昨日"));

        // exercise & verify
        // これで一連の Mapper 処理（setup - map - cleanup）を実行し、期待される結果になっているかをテストする
        driver.runTest();
    }
}
