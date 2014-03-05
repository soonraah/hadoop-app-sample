package mapreduce;

import net.java.sen.SenFactory;
import net.java.sen.StringTagger;
import net.java.sen.dictionary.Token;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Mapper の拡張
 * <p>
 *     指定している総称型はそれぞれ入力のキー・入力の値・出力のキー・出力の値
 * </p>
 */
public class SkypeWordCountMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final Pattern PATTERN_SKYPE_LINE = Pattern
            .compile("^\\[\\d{4}/\\d{2}/\\d{2} \\d\\d:\\d\\d:\\d\\d\\] (.+?): (.+)$");
    private static final Pattern PATTERN_URL = Pattern
            .compile("https?://\\S+");
    private static final Pattern PATTERN_REF = Pattern
            .compile("\\[.+\\] .+:");

    /** 形態素解析用 */
    private StringTagger stringTagger;

    /**
     * Mapper の開始時に一度だけ実行されるセットアップ関数
     *
     * @param context コンテキスト情報
     * @throws IOException
     * @throws InterruptedException
     */
    protected void setup(Context context) throws IOException, InterruptedException {
        // StringTagger インスタンスを最初に生成
        stringTagger = SenFactory.getStringTagger(null);
    }

    /**
     * Mapper のメインの処理
     * <p>
     *     このサンプルの場合入力はテキストファイルであり、そのテキストの1行1行に対して呼ばれます。
     * </p>
     *
     * @param key Mapper への入力のキー
     * @param value Mapper への入力の値
     * @param context コンテキスト情報
     * @throws java.io.IOException
     * @throws java.lang.InterruptedException
     */
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // ユーザ名と本文の抜き出し
        String line = value.toString();
        String userName = extractUserName(line);
        if (userName == null) {
            return;
        }
        String body = extractBody(line);
        if (body == null) {
            return;
        }

        // 不要部分の削除
        String editedBody = removeUnnecessaryParts(body);

        // 本文から名詞のみを取得
        List<String> words = getSubstantivalWords(editedBody);

        // Mapper からの出力！！
        Text textUserName = new Text(userName);
        for (String word : words) {
            context.write(textUserName, new Text(word));
        }
    }

    /**
     * Mapper 終了処理
     *
     * @param context コンテキスト情報
     * @throws java.io.IOException
     * @throws java.lang.InterruptedException
     */
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // in-mapper combining の場合はここで出力する
    }

    private String extractUserName(String line) {
        return extractFromLine(line, 1);
    }

    private String extractBody(String line) {
        return extractFromLine(line, 2);
    }

    private String extractFromLine(String line, int groupIndex) {
        Matcher matcher = PATTERN_SKYPE_LINE.matcher(line);
        if (matcher.find()) {
            return matcher.group(groupIndex);
        } else {
            return null;
        }
    }

    private String removeUnnecessaryParts(String body) {
        String work = body;

        // URL 部分を削除
        Matcher matcher = PATTERN_URL.matcher(work);
        work = matcher.replaceAll("");

        // 引用箇所を削除
        matcher = PATTERN_REF.matcher(work);
        work = matcher.replaceAll("");

        return work;
    }

    /**
     * 形態素解析により入力文字列（文を想定）から名詞のみを抜き出します。
     *
     * @param text 文字列（文）
     * @return 入力文字列に含まれる名詞のリスト
     */
    private List<String> getSubstantivalWords(String text) throws IOException {
        List<Token> tokens = new ArrayList<Token>();
        stringTagger.analyze(text, tokens);

        List<String> words = new ArrayList<String>();
        for (Token token : tokens) {
            String partOfSpeech = token.getMorpheme().getPartOfSpeech();
            if (partOfSpeech.startsWith("名詞")) {
                words.add(token.getSurface());
            }
        }
        return words;
    }
}
