package twitproto

type TweetSrc struct {
	Text                    *string
	Geo                     *string
	In_reply_to_screen_name *string
	Favorited               bool
	Source                  *string
	Contributors            *string
	In_reply_to_status_id   *string
	In_reply_to_user_id     *string
	Id                      int64
	Created_at              *string
}

type Tweet struct {
	Text                    string
	Geo                     string
	In_reply_to_screen_name string
	Favorited               bool
	Source                  string
	Contributors            string
	In_reply_to_status_id   string
	In_reply_to_user_id     string
	Id                      int64
	Created_at              string
}
