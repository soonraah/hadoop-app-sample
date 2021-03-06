# hadoop-app-sample

当プロジェクトは Hadoop アプリケーションのサンプルプロジェクトになります。

## やりたいこと

次のような Skype のログを入力として想定します。

    [2014/02/28 17:18:46] 花子: 車のエンジンがかからないの…
    [2014/02/28 17:19:38] 太郎: あらら？バッテリーかな？ライトは点く？
    [2014/02/28 17:23:07 | 2014/02/28 17:23:13を編集しました] 花子: 昨日まではちゃんと動いてたのに。なんでいきなり動かなくなっちゃうんだろう。 
    [2014/02/28 17:19:38] 太郎: トラブルって怖いよね。
    で、バッテリーかどうか知りたいんだけどライトは点く？

ここから MapReduce を使って **ユーザ別の最も頻出の名詞N個** を抽出します。Nは指定できるようにします。

ただし、Hadoop アプリケーションを理解するという本筋から離れるため、編集されたログや2行にまたがるログに2行目以降は扱いません。別の言い方をすると、正規表現 `^\[\d{4}/\d{2}/\d{2} \d\d:\d\d:\d\d\] (.+?): (.+)$` にひっかかるものだけを処理します。

## ビルド

### テスト実行

    gradlew build

### ビルド実行

    gradlew build

## 実行方法

AWS Elastic MapReduce 上での動作を前提としています。
Custom JAR のステップとして実行してください。

## 引数の例
driver.SkypeWordCountDriver -D input.s3.objectKey=s3://BUCKET_NAME/input/skype.log -D output.s3.objectPrefix=s3://BUCKET_NAME/output -D output.numWords=20