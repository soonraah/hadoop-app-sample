package mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * Reducer の拡張
 * <p>
 *     指定している総称型はそれぞれ入力のキー・入力の値・出力のキー・出力の値
 * </p>
 */
public class SkypeWordCountReducer extends Reducer<Text, Text, Text, Text> {
    /** 出力する単語数（初期値：10） */
    private int numWords = 10;

    /**
     * Reducer の開始時に一度だけ実行されるセットアップ関数
     *
     * @param context コンテキスト情報
     * @throws IOException
     * @throws InterruptedException
     */
    protected void setup(Context context) throws IOException, InterruptedException {
        // Configuration の設定値から必要なパラメータを取得
        numWords = context.getConfiguration().getInt("output.numWords", numWords);
    }

    /**
     * Reducer のメインの処理
     * <p>
     *     Mapper の処理結果のキーごとに呼ばれます。
     * </p>
     *
     * @param key Reducer への入力のキー
     * @param values Reducer への入力の値
     * @param context コンテキスト情報
     * @throws IOException
     * @throws InterruptedException
     */
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 指定された個数の頻出単語を取得
        Map<String, Integer> wordCount = countWords(values);
        List<String> frequentWords = extractFrequentWord(wordCount);

        // ファイルへの出力！！
        context.write(key, new Text(frequentWords.toString()));
    }

    /**
     * Reducer 終了処理
     *
     * @param context コンテキスト情報
     * @throws java.io.IOException
     * @throws java.lang.InterruptedException
     */
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }

    private Map<String, Integer> countWords(Iterable<Text> values) {
        // 単語ごとに登場数を集計
        Map<String, Integer> wordCount = new HashMap<String, Integer>();
        for (Text value : values) {
            String word = value.toString();
            if (wordCount.containsKey(word)) {
                int count = wordCount.get(word);
                wordCount.put(word, count + 1);
            } else {
                wordCount.put(word, 1);
            }
        }
        return wordCount;
    }

    private List<String> extractFrequentWord(Map<String, Integer> wordCount) {
        // Entry のリストにして値の降順でソート
        List<Map.Entry<String, Integer>> entries = new ArrayList<Map.Entry<String, Integer>>(wordCount.entrySet());
        Collections.sort(entries, new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });

        int len = numWords <= entries.size() ? numWords : entries.size();
        List<String> frequentWords = new ArrayList<String>(len);
        for (int i = 0; i < len; ++i) {
            String word = entries.get(i).getKey();
            frequentWords.add(word);
        }
        return frequentWords;
    }
}
