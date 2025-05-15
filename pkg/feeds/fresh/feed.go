package rinds

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"

	"net/http"

	"math/rand"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/lib/pq"
)

type CachedFollows struct {
	Follows      []string  `json:"follows"`
	LastModified time.Time `json:"last_modified"`
}

type FeedType string

const (
	Rinds    FeedType = "rinds"
	Random   FeedType = "random"
	Mnine    FeedType = "mnine"
	Reposts  FeedType = "reposts"
	OReplies FeedType = "oreplies"
)

type StaticFeed struct {
	FeedActorDID   string
	FeedName       string
	StaticPostURIs []string
	DB             *sql.DB
	FeedType       FeedType // random, mnine, reposts
	RepliesOn      bool
}

type Follower struct {
	DID string `json:"did"`
}

type Response struct {
	Follows []Follower `json:"follows"`
	Cursor  string     `json:"cursor"`
}

type PostWithDate struct {
	PostURI   string `json:"post_uri"`
	RelDate   int64  `json:"rel_date"`
	IsRepost  bool   `json:"is_repost"`
	RepostURI string `json:"repost_uri,omitempty"`
}

type CachedPosts struct {
	Posts        []PostWithDate `json:"posts"`
	LastModified time.Time      `json:"last_modified"`
}

// Describe implements feedrouter.Feed.
func (sf *StaticFeed) Describe(ctx context.Context) ([]appbsky.FeedDescribeFeedGenerator_Feed, error) {
	panic("unimplemented")
}

// NewStaticFeed returns a new StaticFeed, a list of aliases for the feed, and an error
// StaticFeed is a trivial implementation of the Feed interface, so its aliases are just the input feedName
func NewStaticFeed(ctx context.Context, feedActorDID string, feedName string, staticPostURIs []string, db *sql.DB, feedType FeedType, repliesOn bool) (*StaticFeed, []string, error) {
	return &StaticFeed{
		FeedActorDID:   feedActorDID,
		FeedName:       feedName,
		StaticPostURIs: staticPostURIs,
		DB:             db,
		FeedType:       feedType,
		RepliesOn:      repliesOn,
	}, []string{feedName}, nil
}

// GetPage returns a list of FeedDefs_SkeletonFeedPost, a new cursor, and an error
// It takes a feed name, a user DID, a limit, and a cursor
// The feed name can be used to produce different feeds from the same feed generator
func (sf *StaticFeed) GetPage(ctx context.Context, feed string, userDID string, limit int64, cursor string) ([]*appbsky.FeedDefs_SkeletonFeedPost, *string, error) {
	cursorAsInt := int64(0)
	var hash string
	var startOfLastPage int64
	var sizeOfLastPage int64
	var inflightstartOfLastPage int64
	var inflightsizeOfLastPage int64
	var err error

	var smartReadEnabled bool = true
	var smartReportingEnabled bool = true
	log.Printf("smartReadEnabled is %v; smartReportingEnabled is %v", smartReadEnabled, smartReportingEnabled)

	if cursor != "" {
		parts := strings.Split(cursor, "-")
		if len(parts) != 6 {
			return nil, nil, fmt.Errorf("invalid cursor format")
		}
		cursorAsInt, err = strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return nil, nil, fmt.Errorf("cursor is not an integer: %w", err)
		}
		hash = parts[1]
		inflightstartOfLastPage, err = strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			return nil, nil, fmt.Errorf("start of last page is not an integer: %w", err)
		}
		inflightsizeOfLastPage, err = strconv.ParseInt(parts[3], 10, 64)
		if err != nil {
			return nil, nil, fmt.Errorf("size of last page is not an integer: %w", err)
		}
		startOfLastPage, err = strconv.ParseInt(parts[4], 10, 64)
		if err != nil {
			return nil, nil, fmt.Errorf("start of last page is not an integer: %w", err)
		}
		sizeOfLastPage, err = strconv.ParseInt(parts[5], 10, 64)
		if err != nil {
			return nil, nil, fmt.Errorf("size of last page is not an integer: %w", err)
		}
	}

	if limit == 1 && cursor == "" {
		// this happens when the app tries to check if the timeline has new posts at the top or not
		// we should handle this better but ehhhhhhhh
		log.Print("limit is 1 and cursor is empty. Skipping the database query.")
		return nil, nil, nil
	} else if cursor == "" {
		log.Println("Generating new hash")
		hash = generateHash(userDID)
	} else {
		log.Println("Using existing hash")
	}

	tableName := fmt.Sprintf("feedcache_%s_%s", userDID, hash)

	// Check if cache table exists
	var exists bool
	err = sf.DB.QueryRowContext(ctx, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = $1)", tableName).Scan(&exists)
	if err != nil {
		return nil, nil, fmt.Errorf("error checking cache table existence: %w", err)
	}

	if exists {
		rows, err := sf.DB.QueryContext(ctx, fmt.Sprintf("SELECT post_uri, rel_date, is_repost, repost_uri FROM %s", pq.QuoteIdentifier(tableName)))
		if err != nil {
			return nil, nil, fmt.Errorf("error querying cache table: %w", err)
		}
		defer rows.Close()

		var cachedPosts []PostWithDate
		for rows.Next() {
			var post PostWithDate
			var repostURI sql.NullString
			if err := rows.Scan(&post.PostURI, &post.RelDate, &post.IsRepost, &repostURI); err != nil {
				return nil, nil, fmt.Errorf("error scanning cached post: %w", err)
			}
			if repostURI.Valid {
				post.RepostURI = repostURI.String
			}
			cachedPosts = append(cachedPosts, post)
		}

		if err := rows.Err(); err != nil {
			return nil, nil, fmt.Errorf("error iterating cached posts: %w", err)
		}

		log.Printf("Cached posts found: %d", len(cachedPosts))
		// fuck it print the entrie cachedPosts

		// Mark posts from the last page as viewed
		/*
			// Handle empty cache table
			if len(cachedPosts) == 0 || int64(len(cachedPosts)) < cursorAsInt {
				// just return https://bsky.app/profile/strandingy.bsky.social/post/3lgfdgn7i6s2q
				// special case when you reach the end
				markPostsAsViewed(ctx, sf.DB, userDID, cachedPosts, smartReportingEnabled)
				emptysinglepost := []PostWithDate{}
				emptysinglepost = append(emptysinglepost, PostWithDate{
					PostURI: "at://did:plc:css3l47v2r4xhcgykfd5mdmn/app.bsky.feed.post/3lgfdgn7i6s2q",
					RelDate: time.Now().UnixNano() / int64(time.Microsecond),
				})
				log.Print("empty page hanglerer paginateResult for cachedPosts")
				return paginateResults(emptysinglepost, inflightstartOfLastPage, inflightsizeOfLastPage, cursorAsInt, limit, hash)
			}
		*/

		if cursor != "" {
			lastPagePosts := cachedPosts[startOfLastPage : startOfLastPage+sizeOfLastPage]
			// the normal intended markPostsAsViewed call
			if err := markPostsAsViewed(ctx, sf.DB, userDID, lastPagePosts, smartReportingEnabled); err != nil {
				return nil, nil, fmt.Errorf("error marking posts as viewed: %w", err)
			}
			log.Printf("Marking these posts as viewed. Range: %d - %d", startOfLastPage, startOfLastPage+sizeOfLastPage)
		}

		log.Print("default paginateResult for cachedPosts")
		return paginateResults(cachedPosts, inflightstartOfLastPage, inflightsizeOfLastPage, cursorAsInt, limit, hash)
	}

	posts := []PostWithDate{}

	log.Println("Fetching followers")
	followers, err := getFollowers(ctx, sf.DB, userDID)
	if err != nil {
		log.Printf("Error fetching followers: %v\n", err)
		return nil, nil, err
	}
	//log.Printf("Raw response: %s\n", followers)

	log.Println("Converting followers to comma-separated string")
	followerIDs := make([]string, len(followers))
	copy(followerIDs, followers)

	if len(followerIDs) == 0 {
		log.Println("No followers found. Skipping the database query.")
		return nil, nil, nil
	}

	//log.Println("Follower IDs:", followerIDs)
	// Check if the table exists
	var tableExists bool
	err = sf.DB.QueryRowContext(ctx, fmt.Sprintf(`
      SELECT EXISTS (
          SELECT FROM information_schema.tables 
          WHERE table_name = %s
      )`, pq.QuoteLiteral(fmt.Sprintf("viewedby_%s", userDID)))).Scan(&tableExists)
	if err != nil {
		log.Printf("Error checking table existence: %v\n", err)
		return nil, nil, fmt.Errorf("error checking table existence: %w", err)
	}

	query := `
      SELECT post_uri, rel_date, is_repost, repost_uri 
      FROM posts 
      WHERE rel_author = ANY($1)`

	if smartReadEnabled {
		query += `
      AND post_uri NOT IN (SELECT post_uri FROM likes WHERE rel_author = $2) 
      AND post_uri NOT IN (SELECT post_uri FROM posts WHERE rel_author = $2 AND is_repost = TRUE)`
	}
	if !sf.RepliesOn {
		query += " AND (reply_to IS NULL OR reply_to = '')"
	}
	if sf.FeedType == "reposts" {
		query += " AND is_repost = TRUE"
	}
	if sf.FeedType == "oreplies" {
		query += " AND (reply_to IS NOT NULL AND reply_to != '') AND is_repost = FALSE"
	}
	if tableExists && smartReadEnabled {
		query += fmt.Sprintf(" AND post_uri NOT IN (SELECT post_uri FROM %s)", pq.QuoteIdentifier(fmt.Sprintf("viewedby_%s", userDID)))
	}
	var rows *sql.Rows

	if smartReadEnabled {
		if sf.FeedType == "mnine" {
			query += ` AND rel_date < $3`
			thresholdTime := time.Now().Add(-9*time.Hour).UnixNano() / int64(time.Microsecond)
			rows, err = sf.DB.QueryContext(ctx, query, pq.Array(followerIDs), userDID, thresholdTime)
		} else {
			rows, err = sf.DB.QueryContext(ctx, query, pq.Array(followerIDs), userDID)
		}
	} else {
		if sf.FeedType == "mnine" {
			query += ` AND rel_date < $2`
			thresholdTime := time.Now().Add(-9*time.Hour).UnixNano() / int64(time.Microsecond)
			rows, err = sf.DB.QueryContext(ctx, query, pq.Array(followerIDs), thresholdTime)
		} else {
			rows, err = sf.DB.QueryContext(ctx, query, pq.Array(followerIDs))
		}
	}
	log.Printf("Query: %s\n", query)
	if err != nil {
		log.Printf("Error querying posts: %v\n", err)
		return nil, nil, fmt.Errorf("error querying posts: %w", err)
	}
	defer rows.Close()

	log.Println("Iterating over rows")
	for rows.Next() {
		var postURI string
		var relDate int64
		var isRepost bool
		var repostURI sql.NullString
		if err := rows.Scan(&postURI, &relDate, &isRepost, &repostURI); err != nil {
			log.Printf("error scanning post URI: %v\n", err)
			return nil, nil, fmt.Errorf("error scanning post URI: %w", err)
		}
		post := PostWithDate{
			PostURI:  postURI,
			RelDate:  relDate,
			IsRepost: isRepost,
		}
		if repostURI.Valid {
			post.RepostURI = repostURI.String
		}
		posts = append(posts, post)
	}

	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("error iterating rows: %w", err)
	}

	log.Printf("Freshly queried posts found: %d", len(posts))

	if sf.FeedType == "random" {
		// Sort results randomly
		log.Println("Sorting results randomly")
		rand.Seed(time.Now().UnixNano())
		rand.Shuffle(len(posts), func(i, j int) {
			posts[i], posts[j] = posts[j], posts[i]
		})
	} else {
		// Sort results by date
		log.Println("Sorting results by date")
		sort.Slice(posts, func(i, j int) bool {
			return posts[i].RelDate > posts[j].RelDate
		})
	}

	// Cache the results in the database
	log.Println("Caching results in the database")

	// Ensure the cachetimeout table exists
	_, err = sf.DB.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS cachetimeout (
			table_name TEXT UNIQUE,
			creation_time TIMESTAMP
		)
	`)
	if err != nil {
		return nil, nil, fmt.Errorf("error creating cachetimeout table: %w", err)
	}

	_, err = sf.DB.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			post_uri TEXT,
			rel_date BIGINT,
			is_repost BOOLEAN,
			repost_uri TEXT,
			viewed BOOLEAN DEFAULT FALSE
		)
	`, pq.QuoteIdentifier(tableName)))
	if err != nil {
		return nil, nil, fmt.Errorf("error creating cache table: %w", err)
	}

	// Store the table name and creation time in cachetimeout
	_, err = sf.DB.ExecContext(ctx, `
		INSERT INTO cachetimeout (table_name, creation_time)
		VALUES ($1, $2)
		ON CONFLICT (table_name) DO NOTHING
	`, tableName, time.Now())
	if err != nil {
		return nil, nil, fmt.Errorf("error inserting into cachetimeout table: %w", err)
	}

	for _, post := range posts {
		_, err := sf.DB.ExecContext(ctx, fmt.Sprintf(`
			INSERT INTO %s (post_uri, rel_date, is_repost, repost_uri)
			VALUES ($1, $2, $3, $4)
		`, pq.QuoteIdentifier(tableName)), post.PostURI, post.RelDate, post.IsRepost, post.RepostURI)
		if err != nil {
			return nil, nil, fmt.Errorf("error inserting into cache table: %w", err)
		}
	}
	log.Print("default paginateResult for freshly queried posts")
	return paginateResults(posts, inflightstartOfLastPage, inflightsizeOfLastPage, cursorAsInt, limit, hash)
}

func paginateResults(posts []PostWithDate, inflightcursorAsInt int64, inflightlimit int64, cursorAsInt int64, limit int64, hash string) ([]*appbsky.FeedDefs_SkeletonFeedPost, *string, error) {
	log.Println("Paginating results")
	var paginatedPosts []*appbsky.FeedDefs_SkeletonFeedPost

	if int64(len(posts)) > cursorAsInt+limit {
		for _, post := range posts[cursorAsInt : cursorAsInt+limit] {
			paginatedPosts = append(paginatedPosts, formatPost(post))
		}
		newCursor := fmt.Sprintf("%d-%s-%d-%d-%d-%d", cursorAsInt+limit, hash, cursorAsInt, limit, inflightcursorAsInt, inflightlimit)
		return paginatedPosts, &newCursor, nil
	}

	for _, post := range posts[cursorAsInt:] {
		paginatedPosts = append(paginatedPosts, formatPost(post))
	}

	return paginatedPosts, nil, nil
}

func formatPost(post PostWithDate) *appbsky.FeedDefs_SkeletonFeedPost {
	if post.IsRepost {
		return &appbsky.FeedDefs_SkeletonFeedPost{
			Post: post.PostURI,
			Reason: &appbsky.FeedDefs_SkeletonFeedPost_Reason{
				FeedDefs_SkeletonReasonRepost: &appbsky.FeedDefs_SkeletonReasonRepost{
					Repost: post.RepostURI,
				},
			},
		}
	}
	return &appbsky.FeedDefs_SkeletonFeedPost{
		Post: post.PostURI,
	}
}

func generateHash(input string) string {
	hash := sha256.Sum256([]byte(input + time.Now().String()))
	return hex.EncodeToString(hash[:])[:5]
}

func getFollowers(ctx context.Context, db *sql.DB, userdid string) ([]string, error) {
	unquotedTableName := "follows_" + userdid
	tableName := pq.QuoteIdentifier(unquotedTableName)
	fmt.Printf("Checking for table: %s\n", tableName) // Debug log

	// Check if cache table exists
	var exists bool
	query := "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = $1)"
	fmt.Printf("Executing query: %s with parameter: %s\n", query, unquotedTableName) // Debug log
	err := db.QueryRowContext(ctx, query, unquotedTableName).Scan(&exists)
	if err != nil {
		// Check for specific errors
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("table existence check returned no rows: %w", err)
		}
		if pqErr, ok := err.(*pq.Error); ok {
			return nil, fmt.Errorf("PostgreSQL error: %s, Code: %s", pqErr.Message, pqErr.Code)
		}
		return nil, fmt.Errorf("error checking cache table existence: %w", err)
	}

	fmt.Printf("Table exists: %v\n", exists) // Debug log

	if exists {
		rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT follow FROM %s", tableName))
		if err != nil {
			return nil, fmt.Errorf("error querying cache table: %w", err)
		}
		defer rows.Close()

		var cachedFollows []string
		for rows.Next() {
			var follow string
			if err := rows.Scan(&follow); err != nil {
				return nil, fmt.Errorf("error scanning cached follow: %w", err)
			}
			cachedFollows = append(cachedFollows, follow)
		}

		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("error iterating cached follows: %w", err)
		}

		log.Println("Returning cached followers")
		return cachedFollows, nil
	}

	log.Println("Fetching followers from API")
	var allDIDs []string
	cursor := ""

	for {
		apiURL := fmt.Sprintf("https://public.api.bsky.app/xrpc/app.bsky.graph.getFollows?actor=%s&cursor=%s", userdid, cursor)
		resp, err := http.Get(apiURL)
		if err != nil {
			log.Printf("Error making request: %v\n", err)
			return nil, fmt.Errorf("failed to make request: %v", err)
		}
		defer resp.Body.Close()

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Printf("Error reading response: %v\n", err)
			return nil, fmt.Errorf("failed to read response: %v", err)
		}

		var response Response
		if err := json.Unmarshal(body, &response); err != nil {
			log.Printf("Error unmarshalling JSON: %v\n", err)
			return nil, fmt.Errorf("failed to unmarshal JSON: %v", err)
		}

		for _, follow := range response.Follows {
			allDIDs = append(allDIDs, follow.DID)
		}

		if response.Cursor == "" {
			break
		}
		cursor = response.Cursor
	}

	// Drop the existing table if it exists
	log.Println("Dropping existing followers table if it exists")
	_, err = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	if err != nil {
		return nil, fmt.Errorf("error dropping existing cache table: %w", err)
	}

	// Cache the results in the database
	log.Println("Caching followers in the database")
	_, err = db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			follow TEXT UNIQUE
		)
	`, tableName))
	if err != nil {
		return nil, fmt.Errorf("error creating cache table: %w", err)
	}

	// Use a map to track unique follows
	followMap := make(map[string]struct{})
	for _, follow := range allDIDs {
		if _, exists := followMap[follow]; !exists {
			followMap[follow] = struct{}{}
			_, err := db.ExecContext(ctx, fmt.Sprintf(`
				INSERT INTO %s (follow)
				VALUES ($1)
				ON CONFLICT (follow) DO NOTHING
			`, tableName), follow)
			if err != nil {
				return nil, fmt.Errorf("error inserting into cache table: %w", err)
			}
		}
	}

	log.Println("Returning fetched followers")
	return allDIDs, nil
}

func markPostsAsViewed(ctx context.Context, db *sql.DB, userDID string, posts []PostWithDate, smartReportingEnabled bool) error {
	if len(posts) == 0 || !smartReportingEnabled {
		return nil
	}
	tableName := fmt.Sprintf("viewedby_%s", userDID)

	// Create the table if it doesn't exist
	_, err := db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			post_uri TEXT UNIQUE
		)
	`, pq.QuoteIdentifier(tableName)))
	if err != nil {
		return fmt.Errorf("error creating viewed table: %w", err)
	}

	// Insert posts into the table
	for _, post := range posts {
		_, err := db.ExecContext(ctx, fmt.Sprintf(`
			INSERT INTO %s (post_uri)
			VALUES ($1)
			ON CONFLICT (post_uri) DO NOTHING
		`, pq.QuoteIdentifier(tableName)), post.PostURI)
		if err != nil {
			return fmt.Errorf("error inserting into viewed table: %w", err)
		}
	}

	return nil
}
