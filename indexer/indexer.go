package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/gorilla/websocket"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
)

const wsUrl = "wss://jetstream2.us-west.bsky.network/subscribe?wantedCollections=app.bsky.feed.post&wantedCollections=app.bsky.feed.repost&wantedCollections=app.bsky.feed.like&wantedCollections=app.bsky.graph.follow"

type LikeMessage struct {
	Did    string `json:"did"`
	TimeUs int64  `json:"time_us"`
	Kind   string `json:"kind"`
	Commit Commit `json:"commit"`
}

type Commit struct {
	Rev        string     `json:"rev"`
	Operation  string     `json:"operation"`
	Collection string     `json:"collection"`
	RKey       string     `json:"rkey"`
	Record     LikeRecord `json:"record"`
	CID        string     `json:"cid"`
}

type LikeRecord struct {
	Type      string      `json:"$type"`
	CreatedAt string      `json:"createdAt"`
	Subject   LikeSubject `json:"subject"`
	Reply     *Reply      `json:"reply,omitempty"`
}

type FollowRecord struct {
	DID       string `json:"subject"`
	createdAt string `json:"createdAt"`
}

type Reply struct {
	Parent ReplySubject `json:"parent"`
	Root   ReplySubject `json:"root"`
}

type LikeSubject struct {
	CID string `json:"cid"`
	URI string `json:"uri"`
	Raw string // Stores raw string if not a JSON object
}

// This handles both string and object forms
func (ls *LikeSubject) UnmarshalJSON(data []byte) error {
	// Try to unmarshal as object first
	var obj struct {
		CID string `json:"cid"`
		URI string `json:"uri"`
	}
	if err := json.Unmarshal(data, &obj); err == nil && obj.URI != "" {
		ls.CID = obj.CID
		ls.URI = obj.URI
		return nil
	}

	// If it's not a valid object, try to unmarshal as string
	var str string
	if err := json.Unmarshal(data, &str); err == nil {
		ls.Raw = str
		return nil
	}

	return fmt.Errorf("LikeSubject: invalid JSON: %s", string(data))
}

type ReplySubject struct {
	CID string `json:"cid"`
	URI string `json:"uri"`
}

var lastLoggedSecond int64 // Keep track of the last logged second

var (
	postBatch       []Post
	likeBatch       []Like
	followBatch     []Follow
	batchInsertSize = 1000             // Adjust the batch size as needed
	batchInterval   = 30 * time.Second // Flush every 30 seconds
)

type Post struct {
	RelAuthor string
	PostUri   string
	RelDate   int64
	IsRepost  bool
	RepostUri string
	ReplyTo   string
}

type Like struct {
	RelAuthor string
	PostUri   string
	RelDate   int64
}

type Follow struct {
	RelAuthor        string
	followSubjectDID string
}

var trackedDIDsMap map[string]struct{}
var trackedDIDs []string

func initTrackedDIDsMap(dids []string) {
	trackedDIDsMap = make(map[string]struct{}, len(dids))
	for _, did := range dids {
		trackedDIDsMap[did] = struct{}{}
	}
}

func getLastCursor(db *sql.DB) int64 {
	var lastCursor int64
	err := db.QueryRow("SELECT lastCursor FROM cursor WHERE id = 1").Scan(&lastCursor)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Println("Cursor table is empty; starting fresh.")
			return 0
		}
		log.Fatalf("Error fetching last cursor: %v", err)
	}
	return lastCursor
}

func main() {
	// Connect to Postgres	// Open the database connection
	dbHost := os.Getenv("DB_HOST")
	dbUser := os.Getenv("DB_USER")
	dbName := os.Getenv("DB_NAME")
	dbPassword := os.Getenv("DB_PASSWORD")
	db, err := sql.Open("postgres", fmt.Sprintf("user=%s dbname=%s host=%s password=%s sslmode=disable", dbUser, dbName, dbHost, dbPassword))

	if err != nil {
		log.Fatalf("Failed to connect to Postgres: %v", err)
	}
	defer db.Close()

	// Ensure tables exist
	createTables(db)

	// Start the cleanup job
	go startCleanupJob(db)

	// Start the batch insert job
	go startBatchInsertJob(db)

	// Start the batch insert job for likes
	go startBatchInsertLikesJob(db)

	// Retrieve the last cursor
	lastCursor := getLastCursor(db)

	// initialize the tracked DIDs
	trackedDIDs, err = getTrackedDIDs(context.Background(), db)
	if err != nil {
		log.Fatalf("Failed to get tracked DIDs: %v", err)
	}
	log.Printf("Tracked DIDs: %v\n", trackedDIDs)
	initTrackedDIDsMap(trackedDIDs)

	go startBatchInsertFollowJob(db)
	// If the cursor is older than 24 hours, skip it
	if lastCursor > 0 {
		cursorTime := time.UnixMicro(lastCursor)
		if time.Since(cursorTime) > 24*time.Hour {
			log.Printf("Cursor is older than 24 hours (%s); skipping it.", cursorTime.Format("2006-01-02 15:04:05"))
			lastCursor = 0 // Ignore this cursor
		} else {
			log.Printf("Resuming from cursor: %d (%s)", lastCursor, cursorTime.Format("2006-01-02 15:04:05"))
		}
	}

	// WebSocket URL with cursor if available
	wsFullUrl := wsUrl
	if lastCursor > 0 {
		wsFullUrl += "&cursor=" + fmt.Sprintf("%d", lastCursor)
	}

	// Connect to WebSocket
	conn, _, err := websocket.DefaultDialer.Dial(wsFullUrl, nil)
	if err != nil {
		log.Fatalf("WebSocket connection error: %v", err)
	}
	defer conn.Close()

	//print wsFullUrl
	log.Printf("Connected to WebSocket: %s", wsFullUrl)

	log.Println("Listening for WebSocket messages...")

	// Process WebSocket messages
	for {
		var msg LikeMessage
		err := conn.ReadJSON(&msg)
		if err != nil {
			log.Printf("Error reading WebSocket message: %v", err)
			continue
		}

		processMessage(db, msg)
	}
}

func createTables(db *sql.DB) {
	_, err := db.Exec(`
        CREATE TABLE IF NOT EXISTS posts (
            id SERIAL PRIMARY KEY,
            rel_author TEXT NOT NULL,
            post_uri TEXT NOT NULL,
            rel_date BIGINT NOT NULL,
            is_repost BOOLEAN NOT NULL DEFAULT FALSE,
						repost_uri TEXT,
            reply_to TEXT,
            UNIQUE(rel_author, post_uri, rel_date)
        );
    `)
	if err != nil {
		log.Fatalf("Error creating 'posts' table: %v", err)
	}

	_, err = db.Exec(`
        CREATE TABLE IF NOT EXISTS likes (
            id SERIAL PRIMARY KEY,
            rel_author TEXT NOT NULL,
            post_uri TEXT NOT NULL,
						rel_date BIGINT NOT NULL,
        		UNIQUE(rel_author, post_uri)
        );
    `)
	if err != nil {
		log.Fatalf("Error creating 'posts' table: %v", err)
	}

	// Create a cursor table with a single-row constraint
	_, err = db.Exec(`
        CREATE TABLE IF NOT EXISTS cursor (
            id INT PRIMARY KEY CHECK (id = 1),
            lastCursor BIGINT NOT NULL
        );
    `)
	if err != nil {
		log.Fatalf("Error creating 'cursor' table: %v", err)
	}

	// Ensure the cursor table always has exactly one row
	_, err = db.Exec(`
        INSERT INTO cursor (id, lastCursor)
        VALUES (1, 0)
        ON CONFLICT (id) DO NOTHING;
    `)
	if err != nil {
		log.Fatalf("Error initializing cursor table: %v", err)
	}
}
func processMessage(db *sql.DB, msg LikeMessage) {
	// log.Print("Processing message...")
	// Convert cursor to time
	cursorTime := time.UnixMicro(msg.TimeUs)

	// Get the whole second as a Unix timestamp
	currentSecond := cursorTime.Unix()

	// Check if this second has already been logged
	if currentSecond != lastLoggedSecond && cursorTime.Nanosecond() >= 100_000_000 && cursorTime.Nanosecond() < 200_000_000 {
		// Update the last logged second
		lastLoggedSecond = currentSecond

		// Log only once per second
		humanReadableTime := cursorTime.Format("2006-01-02 15:04:05.000")
		log.Printf("Cursor (time_us): %d, Human-readable time: %s", msg.TimeUs, humanReadableTime)
	}

	// Save the record
	record := msg.Commit.Record
	postUri := fmt.Sprintf("at://%s/app.bsky.feed.post/%s", msg.Did, msg.Commit.RKey)
	repostUri := fmt.Sprintf("at://%s/app.bsky.feed.repost/%s", msg.Did, msg.Commit.RKey)
	reply := ""
	if msg.Commit.Record.Reply != nil {
		reply = fmt.Sprintf("Parent: %s, Root: %s", msg.Commit.Record.Reply.Parent.URI, msg.Commit.Record.Reply.Root.URI)
	}

	switch msg.Commit.Collection {
	case "app.bsky.feed.post":
		if msg.Commit.Operation == "create" {
			postBatch = append(postBatch, Post{msg.Did, postUri, msg.TimeUs, false, "", reply})
		} else if msg.Commit.Operation == "delete" {
			deletePost(db, msg.Did, postUri, msg.TimeUs)
		}
	case "app.bsky.feed.repost":

		if record.Subject.URI != "" {
			if msg.Commit.Operation == "create" {
				postBatch = append(postBatch, Post{msg.Did, record.Subject.URI, msg.TimeUs, true, repostUri, ""})
			} else if msg.Commit.Operation == "delete" {
				deletePost(db, msg.Did, record.Subject.URI, msg.TimeUs)
			}
		}
	case "app.bsky.feed.like":
		if record.Subject.URI != "" {
			if msg.Commit.Operation == "create" {
				likeBatch = append(likeBatch, Like{msg.Did, record.Subject.URI, msg.TimeUs})
			} else if msg.Commit.Operation == "delete" {
				deleteLike(db, msg.Did, record.Subject.URI)
			}
		}
	case "app.bsky.graph.follow":
		_, tracked := trackedDIDsMap[msg.Did]
		if tracked {
			// log.Printf("Following found tracked DID: %s\n", msg.Did)
			if msg.Commit.Operation == "create" {
				// log.Printf("Following Create; doer: %s, subject: %s\n", msg.Did, record.Subject.Raw)
				followBatch = append(followBatch, Follow{msg.Did, record.Subject.Raw})
			} else if msg.Commit.Operation == "delete" {
				// log.Printf("Following Delete; doer: %s, subject: %s\n", msg.Did, record.Subject.Raw)
				//log.Printf("Unfollowing: %s", msg.Commit.RKey)
				// Remove the DID from the tracked DIDs map
				deleteFollow(db, msg.Did, msg.Commit.RKey)
			}
		}
	default:
		//log.Printf("Unknown collection: %s", msg.Commit.Collection)
	}

	// Update the cursor in the single-row table
	_, err := db.Exec(`
			UPDATE cursor SET lastCursor = $1 WHERE id = 1;
	`, msg.TimeUs)
	if err != nil {
		log.Printf("Error updating cursor: %v", err)
	}
}

func deletePost(db *sql.DB, relAuthor, postUri string, relDate int64) {
	_, err := db.Exec(`
			DELETE FROM posts WHERE rel_author = $1 AND post_uri = $2 AND rel_date = $3;
	`, relAuthor, postUri, relDate)
	if err != nil {
		log.Printf("Error deleting post: %v", err)
	}
}

func deleteLike(db *sql.DB, relAuthor, postUri string) {
	_, err := db.Exec(`
			DELETE FROM likes WHERE rel_author = $1 AND post_uri = $2;
	`, relAuthor, postUri)
	if err != nil {
		log.Printf("Error deleting like: %v", err)
	}
}

func deleteFollow(db *sql.DB, relAuthor, did string) {
	unquotedTableName := "follows_" + relAuthor
	tableName := pq.QuoteIdentifier(unquotedTableName)
	_, err := db.Exec(fmt.Sprintf(`
			DELETE FROM %s WHERE follow = $1;
	`, tableName), did)
	if err != nil {
		log.Printf("Error deleting follow: %v", err)
	}
}

func startCleanupJob(db *sql.DB) {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for range ticker.C {
		cleanupOldPosts(db)
		if err := cleanOldFeedCaches(context.Background(), db); err != nil {
			log.Printf("Error cleaning old feed caches: %v\n", err)
		}
	}
}

func cleanupOldPosts(db *sql.DB) {
	threshold := time.Now().Add(-24 * time.Hour).UnixMicro()
	_, err := db.Exec(`
			DELETE FROM posts WHERE rel_date < $1;
	`, threshold)
	if err != nil {
		log.Printf("Error deleting old posts: %v", err)
	} else {
		log.Printf("Deleted posts older than 24 hours.")
	}
}

func startBatchInsertJob(db *sql.DB) {
	ticker := time.NewTicker(batchInterval)
	defer ticker.Stop()

	for range ticker.C {
		if len(postBatch) >= batchInsertSize {
			batchInsertPosts(db)
		}
	}
}

func batchInsertPosts(db *sql.DB) {
	tx, err := db.Begin()
	if err != nil {
		log.Printf("Error starting transaction: %v", err)
		return
	}

	stmt, err := tx.Prepare(`
		INSERT INTO posts (rel_author, post_uri, rel_date, is_repost, repost_uri, reply_to)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (rel_author, post_uri, rel_date) DO NOTHING;
	`)
	if err != nil {
		log.Printf("Error preparing statement: %v", err)
		return
	}
	defer stmt.Close()

	for _, post := range postBatch {
		_, err := stmt.Exec(post.RelAuthor, post.PostUri, post.RelDate, post.IsRepost, post.RepostUri, post.ReplyTo)
		if err != nil {
			log.Printf("Error executing statement: %v", err)
			log.Printf("Failed INSERT: %+v\nError: %v", post, err)
			os.Exit(1) // Exit on error
		}
	}

	err = tx.Commit()
	if err != nil {
		log.Printf("Error committing transaction: %v", err)
	}

	// Clear the batch
	postBatch = postBatch[:0]
}

func startBatchInsertLikesJob(db *sql.DB) {
	ticker := time.NewTicker(batchInterval)
	defer ticker.Stop()

	for range ticker.C {
		if len(likeBatch) >= batchInsertSize {
			batchInsertLikes(db)
		}
	}
}

func startBatchInsertFollowJob(db *sql.DB) {
	ticker := time.NewTicker(batchInterval)
	defer ticker.Stop()

	for range ticker.C {
		if len(followBatch) >= 1 {
			batchInsertFollow(db)
		}
	}
}
func batchInsertFollow(db *sql.DB) {
	tx, err := db.Begin()
	if err != nil {
		log.Printf("Error starting transaction: %v", err)
		return
	}

	unquotedTableName := "follows_" + followBatch[0].RelAuthor
	tableName := pq.QuoteIdentifier(unquotedTableName)

	stmt, err := tx.Prepare(fmt.Sprintf(`
		INSERT INTO %s (follow)
			VALUES ($1)
			ON CONFLICT (follow) DO NOTHING
	`, tableName))
	if err != nil {
		log.Printf("Error preparing statement: %v", err)
		return
	}
	defer stmt.Close()

	for _, follow := range followBatch {
		_, err := stmt.Exec(follow.followSubjectDID)
		if err != nil {
			log.Printf("Error executing statement: %v", err)
			log.Printf("Failed FOLLOW INSERT: %+v\nError: %v", follow, err)
			os.Exit(1) // Exit on error
		}
	}

	err = tx.Commit()
	if err != nil {
		log.Printf("Error committing transaction: %v", err)
	}

	// Clear the batch
	followBatch = followBatch[:0]
}

func batchInsertLikes(db *sql.DB) {
	tx, err := db.Begin()
	if err != nil {
		log.Printf("Error starting transaction: %v", err)
		return
	}

	stmt, err := tx.Prepare(`
		INSERT INTO likes (rel_author, post_uri, rel_date)
		VALUES ($1, $2, $3)
		ON CONFLICT (rel_author, post_uri) DO NOTHING;
	`)
	if err != nil {
		log.Printf("Error preparing statement: %v", err)
		return
	}
	defer stmt.Close()

	for _, like := range likeBatch {
		_, err := stmt.Exec(like.RelAuthor, like.PostUri, like.RelDate)
		if err != nil {
			log.Printf("Error executing statement: %v", err)
			log.Printf("Failed LIKE INSERT: %+v\nError: %v", like, err)
			os.Exit(1) // Exit on error
		}
	}

	err = tx.Commit()
	if err != nil {
		log.Printf("Error committing transaction: %v", err)
	}

	// Clear the batch
	likeBatch = likeBatch[:0]
}

func cleanOldFeedCaches(ctx context.Context, db *sql.DB) error {
	//log
	log.Println("Cleaning old feed caches")
	// Get the current time minus 24 hours
	expirationTime := time.Now().Add(-24 * time.Hour)

	// Get all tables from cachetimeout that are older than 24 hours
	rows, err := db.QueryContext(ctx, `
			SELECT table_name 
			FROM cachetimeout 
			WHERE creation_time < $1
	`, expirationTime)
	if err != nil {
		return fmt.Errorf("error querying cachetimeout table: %w", err)
	}
	defer rows.Close()

	var tablesToDelete []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return fmt.Errorf("error scanning table name: %w", err)
		}
		tablesToDelete = append(tablesToDelete, tableName)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating cachetimeout rows: %w", err)
	}

	// Get all feedcache_* tables that do not have an entry in cachetimeout
	rows, err = db.QueryContext(ctx, `
			SELECT table_name 
			FROM information_schema.tables 
			WHERE table_name LIKE 'feedcache_%' 
			AND table_name NOT IN (SELECT table_name FROM cachetimeout)
	`)
	if err != nil {
		return fmt.Errorf("error querying feedcache tables: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return fmt.Errorf("error scanning table name: %w", err)
		}
		tablesToDelete = append(tablesToDelete, tableName)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating feedcache rows: %w", err)
	}

	// Drop the old tables and remove their entries from cachetimeout
	for _, tableName := range tablesToDelete {
		_, err := db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", pq.QuoteIdentifier(tableName)))
		if err != nil {
			return fmt.Errorf("error dropping table %s: %w", tableName, err)
		}
		_, err = db.ExecContext(ctx, "DELETE FROM cachetimeout WHERE table_name = $1", tableName)
		if err != nil {
			return fmt.Errorf("error deleting from cachetimeout table: %w", err)
		}
	}

	return nil
}

// ehhhhh why are we doing this
func getTrackedDIDs(ctx context.Context, db *sql.DB) ([]string, error) {
	const prefix = "follows_"
	query := `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_name LIKE $1
	`
	rows, err := db.QueryContext(ctx, query, prefix+"%")
	if err != nil {
		return nil, fmt.Errorf("error querying tracked follows tables: %w", err)
	}
	defer rows.Close()

	var trackedDIDs []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("error scanning table name: %w", err)
		}
		// Strip prefix to get the DID
		if len(tableName) > len(prefix) {
			did := tableName[len(prefix):]
			trackedDIDs = append(trackedDIDs, did)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating tracked tables: %w", err)
	}

	return trackedDIDs, nil
}
