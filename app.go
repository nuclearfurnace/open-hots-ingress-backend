package main

import "bytes"
import "crypto/sha256"
import "fmt"
import "io"
import "net/http"
import "time"

import "cloud.google.com/go/storage"
import "google.golang.org/appengine"
import "google.golang.org/appengine/log"

func init() {
	http.HandleFunc("/", handler)
}

func handler(w http.ResponseWriter, r *http.Request) {
	// Create our AE context.
	ctx := appengine.NewContext(r)

	// Set up our storage client objects.
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Errorf(ctx, "failed to create client: %v", err)
		return
	}

	bucket := client.Bucket("open-hots-ingress")

	// Do some basic request validation.
	if r.Method != "POST" {
		http.NotFound(w, r)
		return
	}

	// Parse the form contents, up to 8MB.
	err = r.ParseMultipartForm(8 * 1024 * 1024)
	if err != nil {
		w.WriteHeader(400)
		io.WriteString(w, "failed to parse form upload")
		return
	}

	// Now extract the file.
	f, _, err := r.FormFile("replayFile")
	if err != nil {
		w.WriteHeader(500)
		io.WriteString(w, "failed to extract replay file from form upload")
		return
	}
	defer f.Close()

	buf := new(bytes.Buffer)
	if _, err := io.Copy(buf, f); err != nil {
		log.Errorf(ctx, "failed to read file buffer: %v", err)
		w.WriteHeader(500)
		io.WriteString(w, "failed to read replay file from form upload")
		return
	}

	replayHash := calculateFileHash(buf.Bytes())
	uploadTime := fmt.Sprintf("%d", time.Now().UnixNano())
	fileName := fmt.Sprintf("replay-upload-%s-%s.StormReplay", uploadTime, replayHash)

	// Now create a filename and ship it to GCS.
	wc := bucket.Object(fileName).NewWriter(ctx)
	wc.ContentType = "blizzard/storm-replay"
	wc.Metadata = map[string]string{
		"SourceAddress":   r.RemoteAddr,
		"UploadTimestamp": uploadTime,
		"FileHash":        replayHash,
	}

	if _, err := io.Copy(wc, buf); err != nil {
		log.Errorf(ctx, "failed to write replay file to bucket: %v", err)
		w.WriteHeader(500)
		io.WriteString(w, "failed to write replay file")
		return
	}

	if err := wc.Close(); err != nil {
		log.Errorf(ctx, "failed to close bucket: %v", err)
		w.WriteHeader(500)
		io.WriteString(w, "failed to finalize upload")
		return
	}
}

func calculateFileHash(buf []byte) string {
	sum := sha256.Sum256(buf)
	return fmt.Sprintf("%x", sum)
}
