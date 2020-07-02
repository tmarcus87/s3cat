s3cat
=====

S3Objectをローカルにダウンロードし、内容を出力します

Object名が.gzで終わる場合gzipテキストとして解凍した結果を出力します

## Usage

```bash
# bucket-name内の/object/key/prefixから始まるオブジェクトをダウンロードし、標準出力に書き出します
$ s3cat --region ap-northeast-1 /bucket-name/object/key/prefix
```

## Options

```bash
$ s3cat --help
  Usage:
    s3cat [OPTIONS] PATTERN...
  
  Application Options:
    -r, --region=      AWSリージョンを指定します
    -t, --temp=        S3Objectのダウンロード先を指定します (default: /tmp)
    -c, --concurrency= S3Objectの並列ダウンロード数を指定します (default: 1)
    -e, --timeout=     ファイル取得のタイムアウトを設定します
    -v, --verbose      詳細な出力を標準エラーに出力します
  
  Help Options:
    -h, --help         Show this help message
```
