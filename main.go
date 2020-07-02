package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/dustin/go-humanize"
	"github.com/jessevdk/go-flags"
	"golang.org/x/sync/errgroup"
	"io"
	"os"
	"path"
	"strings"
	"time"
)

type Options struct {
	Region      string        `short:"r" long:"region"      description:"AWSリージョンを指定します"`
	Temp        string        `short:"t" long:"temp"        description:"S3Objectのダウンロード先を指定します"`
	Concurrency int           `short:"c" long:"concurrency" description:"S3Objectの並列ダウンロード数を指定します"`
	Timeout     time.Duration `short:"e" long:"timeout"     description:"ファイル取得のタイムアウトを設定します"`
	Verbose     bool          `short:"v" long:"verbose"     description:"詳細な出力を標準エラーに出力します"`
}

var opts = Options{
	Concurrency: 1,
	Temp:        path.Join(os.TempDir(), "s3cat"),
}

func logFatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func logError(err error) {
	fmt.Fprintln(os.Stderr, err)
}

func stdout(format string, a ...interface{}) {
	fmt.Fprintf(os.Stdout, format, a...)
}

func stderr(format string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, format, a...)
}

func main() {
	// Parse option
	parser := flags.NewParser(&opts, flags.HelpFlag|flags.PassDoubleDash)
	parser.Name = os.Args[0]
	parser.Usage = "[OPTIONS] PATTERN..."

	args, err := parser.Parse()
	if err != nil {
		logFatal(err)
	}
	if len(args) == 0 {
		parser.WriteHelp(os.Stderr)
		os.Exit(1)
	}

	if opts.Verbose {
		stderr("%+v => %+v\n", opts, args)
	}

	// Parse pattern
	inputs, err := parseArgs(args)
	if err != nil {
		logFatal(err)
	}

	// Context
	var (
		ctx    = context.Background()
		cancel context.CancelFunc
	)

	if opts.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, opts.Timeout)
		defer cancel()
	}

	// Prepare aws session
	cfgs := make([]*aws.Config, 0)
	if opts.Region != "" {
		cfgs = append(cfgs, &aws.Config{
			Region: &opts.Region,
		})
	}
	sess, err := session.NewSession(cfgs...)
	if err != nil {
		logFatal(err)
	}

	// Patternに一致するObject一覧を取得
	objects, err := listS3Objects(ctx, sess, inputs)
	if err != nil {
		logFatal(err)
	}

	// ローカルとの差分をチェック
	for _, object := range objects {
		// ローカルに同じサイズのファイルが有る場合のみローカルフラグを立てる
		fi, err := os.Stat(object.LocalPath(opts.Temp))
		object.local = err == nil && fi.Size() == *object.raw.Size
	}

	// ダウンロード一覧を表示する
	var download uint64
	for _, object := range objects {
		if !object.local {
			download += uint64(*object.raw.Size)
		}
		if opts.Verbose {
			stderr("[%1s] /%s/%s\n",
				func(local bool) string {
					if local {
						return ""
					}
					return "D"
				}(object.local),
				object.bucket,
				*object.raw.Key)
		}
	}

	if opts.Verbose {
		stderr("Download to '%s'\n", opts.Temp)
	}

	// ダウンロードサイズを表示
	stderr("Size : %s\n", humanize.Bytes(download))

	// ダウンロード
	var (
		deg, childCtx = errgroup.WithContext(ctx)
		dsem          = make(chan struct{}, opts.Concurrency)
		s3m           = s3manager.NewDownloader(sess)
	)
	for _, object := range objects {
		if object.local {
			continue
		}

		dsem <- struct{}{}
		deg.Go(func(ctx context.Context, object *objectWrapper) func() error {
			return func() error {
				fp := object.LocalPath(opts.Temp)

				if opts.Verbose {
					stderr("%s ... Downloading\n", fp)
				}

				defer func() {
					if opts.Verbose {
						stderr("%s ... Done\n", fp)
					}
					<-dsem
				}()

				// ディレクトリがなければ作る
				dir := path.Dir(fp)
				if fi, err := os.Stat(dir); os.IsNotExist(err) {
					if err := os.MkdirAll(dir, 0755); err != nil {
						logFatal(fmt.Errorf("failed to create directory : %w", err))
					}
				} else if err != nil {
					logFatal(fmt.Errorf("failed to stat : %w", err))
				} else if !fi.IsDir() {
					logFatal(fmt.Errorf("'%s' is not a directory", dir))
				}

				// ファイルを作る
				f, err := os.Create(fp)
				if err != nil {
					return fmt.Errorf("failed to create local file '%s' : %w", fp, err)
				}
				defer func() {
					if err := f.Close(); err != nil {
						logFatal(fmt.Errorf("failed to close file : %w", err))
					}
				}()

				// ダウンロード
				if _, err =
					s3m.DownloadWithContext(
						ctx,
						f,
						&s3.GetObjectInput{
							Bucket: &object.bucket,
							Key:    object.raw.Key,
						}); err != nil {
					return fmt.Errorf("failed to download file '%s' : %w", fp, err)
				}
				return nil
			}
		}(childCtx, object))
	}
	if err := deg.Wait(); err != nil {
		logFatal(err)
	}

	// 出力
	for _, object := range objects {
		func () {
			fp := object.LocalPath(opts.Temp)

			var reader io.Reader

			if f, err := os.Open(fp); err != nil {
				logFatal(fmt.Errorf("failed to open '%s' : %w", fp, err))
			} else {
				defer f.Close()
				reader = f
			}

			if strings.HasSuffix(fp, ".gz") {
				if reader, err = gzip.NewReader(reader); err != nil {
					logFatal(fmt.Errorf("failed to open '%s' as gzip : %w", err))
				}
			}

			scanner := bufio.NewScanner(reader)
			for scanner.Scan() {
				stdout("%s\n", scanner.Text())
			}
		}()
	}
}

func parseArgs(args []string) ([]*s3.ListObjectsV2Input, error) {
	inputs := make([]*s3.ListObjectsV2Input, 0)
	for _, arg := range args {
		bucket, prefix, err := parseArg(arg)
		if err != nil {
			return nil, err
		}
		inputs = append(inputs, &s3.ListObjectsV2Input{Bucket: &bucket, Prefix: &prefix})
	}
	return inputs, nil
}

func parseArg(arg string) (bucket, prefix string, err error) {
	if !path.IsAbs(arg) {
		err = errors.New("PATTERN must be a start with '/'")
		return
	}

	var (
		bb       strings.Builder
		pb       strings.Builder
		isBucket = true
	)

	r := strings.NewReader(arg[1:])
	for {
		b, rberr := r.ReadByte()
		if rberr == io.EOF {
			break
		} else if rberr != nil {
			err = fmt.Errorf("failed to parse PATTERN : %w", rberr)
			return
		}

		if isBucket {
			if b == '/' {
				isBucket = false
			} else {
				bb.WriteByte(b)
			}
		} else {
			pb.WriteByte(b)
		}
	}

	return bb.String(), pb.String(), nil
}

type objectWrapper struct {
	bucket string
	raw    *s3.Object
	local  bool
}

func (w *objectWrapper) LocalPath(tmp string) string {
	return path.Join(tmp, w.bucket, *w.raw.Key)
}

func listS3Objects(ctx context.Context, sess *session.Session, inputs []*s3.ListObjectsV2Input) ([]*objectWrapper, error) {
	if opts.Verbose {
		stderr("Fetch S3 object list ")
	}
	defer func() {
		if opts.Verbose {
			stderr(" OK\n")
		}
	}()
	s3c := s3.New(sess)

	results := make([]*objectWrapper, 0)
	for _, input := range inputs {
		objects, err := listS3Object(ctx, s3c, input, nil)
		if err != nil {
			return nil, err
		}
		results = append(results, objects...)
	}

	return results, nil
}

func listS3Object(ctx context.Context, s3c *s3.S3, input *s3.ListObjectsV2Input, token *string) ([]*objectWrapper, error) {
	if opts.Verbose {
		stderr(".")
	}
	input.ContinuationToken = token
	out, err := s3c.ListObjectsV2WithContext(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to list S3 objects : %w", err)
	}

	results := make([]*objectWrapper, len(out.Contents))
	for i, object := range out.Contents {
		results[i] = &objectWrapper{bucket: *input.Bucket, raw: object}
	}

	if out.NextContinuationToken != nil {
		nextOut, err := listS3Object(ctx, s3c, input, out.NextContinuationToken)
		if err != nil {
			return nil, err
		}
		results = append(results, nextOut...)
	}

	return results, nil
}
